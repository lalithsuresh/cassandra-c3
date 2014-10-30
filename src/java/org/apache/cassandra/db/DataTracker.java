/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    public final Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<>();
    public final ColumnFamilyStore cfstore;
    private final AtomicReference<View> view;

    public DataTracker(ColumnFamilyStore cfstore)
    {
        this.cfstore = cfstore;
        this.view = new AtomicReference<>();
        this.init();
    }

    public Memtable getMemtable()
    {
        return view.get().memtable;
    }

    public Set<Memtable> getMemtablesPendingFlush()
    {
        return view.get().memtablesPendingFlush;
    }

    /**
     * @return the active memtable and all the memtables that are pending flush.
     */
    public Iterable<Memtable> getAllMemtables()
    {
        View snapshot = view.get();
        return Iterables.concat(snapshot.memtablesPendingFlush, Collections.singleton(snapshot.memtable));
    }

    public Set<SSTableReader> getSSTables()
    {
        return view.get().sstables;
    }

    public Set<SSTableReader> getUncompactingSSTables()
    {
        return view.get().nonCompactingSStables();
    }

    public Iterable<SSTableReader> getUncompactingSSTables(Iterable<SSTableReader> candidates)
    {
        final View v = view.get();
        return Iterables.filter(candidates, new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return !v.compacting.contains(sstable);
            }
        });
    }

    public View getView()
    {
        return view.get();
    }

    /**
     * Switch the current memtable.
     * This atomically adds the current memtable to the memtables pending
     * flush and replace it with a fresh memtable.
     *
     * @return the previous current memtable (the one added to the pending
     * flush)
     */
    public Memtable switchMemtable()
    {
        // atomically change the current memtable
        Memtable newMemtable = new Memtable(cfstore);
        Memtable toFlushMemtable;
        View currentView, newView;
        do
        {
            currentView = view.get();
            toFlushMemtable = currentView.memtable;
            newView = currentView.switchMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));

        return toFlushMemtable;
    }

    /**
     * Renew the current memtable without putting the old one for a flush.
     * Used when we flush but a memtable is clean (in which case we must
     * change it because it was frozen).
     */
    public void renewMemtable()
    {
        assert !cfstore.keyspace.metadata.durableWrites;

        Memtable newMemtable = new Memtable(cfstore);
        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.renewMemtable(newMemtable);
        }
        while (!view.compareAndSet(currentView, newView));
        notifyRenewed(currentView.memtable);
    }

    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        // sstable may be null if we flushed batchlog and nothing needed to be retained

        if (!cfstore.isValid())
        {
            View currentView, newView;
            do
            {
                currentView = view.get();
                newView = currentView.replaceFlushed(memtable, sstable);
                if (sstable != null)
                    newView = newView.replace(Arrays.asList(sstable), Collections.<SSTableReader>emptyList());
            }
            while (!view.compareAndSet(currentView, newView));
            return;
        }

        // back up before creating a new View (which makes the new one eligible for compaction)
        if (sstable != null)
            maybeIncrementallyBackup(sstable);

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replaceFlushed(memtable, sstable);
        }
        while (!view.compareAndSet(currentView, newView));

        if (sstable != null)
        {
            addNewSSTablesSize(Arrays.asList(sstable));
            notifyAdded(sstable);
        }
    }

    public void maybeIncrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.isIncrementalBackupsEnabled())
            return;

        File backupsDir = Directories.getBackupsDirectory(sstable.descriptor);
        sstable.createLinks(FileUtils.getCanonicalPath(backupsDir));
    }

    /**
     * @return true if we are able to mark the given @param sstables as compacted, before anyone else
     *
     * Note that we could acquire references on the marked sstables and release them in
     * unmarkCompacting, but since we will never call markObsolete on a sstable marked
     * as compacting (unless there is a serious bug), we can skip this.
     */
    public boolean markCompacting(Iterable<SSTableReader> sstables)
    {
        assert sstables != null && !Iterables.isEmpty(sstables);

        View currentView = view.get();
        Set<SSTableReader> inactive = Sets.difference(ImmutableSet.copyOf(sstables), currentView.compacting);
        if (inactive.size() < Iterables.size(sstables))
            return false;

        if (Iterables.any(sstables, new Predicate<SSTableReader>()
        {
            @Override
            public boolean apply(SSTableReader sstable)
            {
                return sstable.isMarkedCompacted();
            }
        }))
        {
            return false;
        }

        View newView = currentView.markCompacting(inactive);
        return view.compareAndSet(currentView, newView);
    }

    /**
     * Removes files from compacting status: this is different from 'markObsolete'
     * because it should be run regardless of whether a compaction succeeded.
     */
    public void unmarkCompacting(Iterable<SSTableReader> unmark)
    {
        boolean isValid = cfstore.isValid();
        if (!isValid)
        {
            // The CF has been dropped.  We don't know if the original compaction suceeded or failed,
            // which makes it difficult to know if the sstable reference has already been released.
            // A "good enough" approach is to mark the sstables involved obsolete, which if compaction succeeded
            // is harmlessly redundant, and if it failed ensures that at least the sstable will get deleted on restart.
            for (SSTableReader sstable : unmark)
                sstable.markObsolete();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.unmarkCompacting(unmark);
        }
        while (!view.compareAndSet(currentView, newView));

        if (!isValid)
        {
            // when the CFS is invalidated, it will call unreferenceSSTables().  However, unreferenceSSTables only deals
            // with sstables that aren't currently being compacted.  If there are ongoing compactions that finish or are
            // interrupted after the CFS is invalidated, those sstables need to be unreferenced as well, so we do that here.
            unreferenceSSTables();
        }
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        replace(sstables, Collections.<SSTableReader>emptyList());
        notifySSTablesChanged(sstables, Collections.<SSTableReader>emptyList(), compactionType);
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Collection<SSTableReader> replacements, OperationType compactionType)
    {
        replace(sstables, replacements);
        notifySSTablesChanged(sstables, replacements, compactionType);
    }

    public void addInitialSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        // no notifications or backup necessary
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        replace(Collections.<SSTableReader>emptyList(), sstables);
        for (SSTableReader sstable : sstables)
        {
            maybeIncrementallyBackup(sstable);
            notifyAdded(sstable);
        }
    }

    /**
     * removes all sstables that are not busy compacting.
     */
    public void unreferenceSSTables()
    {
        Set<SSTableReader> notCompacting;

        View currentView, newView;
        do
        {
            currentView = view.get();
            notCompacting = currentView.nonCompactingSStables();
            newView = currentView.replace(notCompacting, Collections.<SSTableReader>emptySet());
        }
        while (!view.compareAndSet(currentView, newView));

        if (notCompacting.isEmpty())
        {
            // notifySSTablesChanged -> LeveledManifest.promote doesn't like a no-op "promotion"
            return;
        }
        notifySSTablesChanged(notCompacting, Collections.<SSTableReader>emptySet(), OperationType.UNKNOWN);
        postReplace(notCompacting, Collections.<SSTableReader>emptySet(), true);
    }

    /**
     * Removes every SSTable in the directory from the DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void removeUnreadableSSTables(File directory)
    {
        View currentView, newView;
        List<SSTableReader> remaining = new ArrayList<>();
        do
        {
            currentView = view.get();
            for (SSTableReader r : currentView.nonCompactingSStables())
                if (!r.descriptor.directory.equals(directory))
                    remaining.add(r);

            if (remaining.size() == currentView.nonCompactingSStables().size())
                return;

            newView = currentView.replace(currentView.sstables, remaining);
        }
        while (!view.compareAndSet(currentView, newView));
        notifySSTablesChanged(remaining, Collections.<SSTableReader>emptySet(), OperationType.UNKNOWN);
    }

    /** (Re)initializes the tracker, purging all references. */
    void init()
    {
        view.set(new View(new Memtable(cfstore),
                          Collections.<Memtable>emptySet(),
                          Collections.<SSTableReader>emptySet(),
                          Collections.<SSTableReader>emptySet(),
                          SSTableIntervalTree.empty()));
    }

    private void replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
    {
        if (!cfstore.isValid())
        {
            removeOldSSTablesSize(replacements, false);
            replacements = Collections.emptyList();
        }

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = currentView.replace(oldSSTables, replacements);
        }
        while (!view.compareAndSet(currentView, newView));

        postReplace(oldSSTables, replacements, false);
    }

    private void postReplace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements, boolean tolerateCompacted)
    {
        addNewSSTablesSize(replacements);
        removeOldSSTablesSize(oldSSTables, tolerateCompacted);
    }

    private void addNewSSTablesSize(Iterable<SSTableReader> newSSTables)
    {
        for (SSTableReader sstable : newSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("adding %s to list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.keyspace.getName(), cfstore.name));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.inc(size);
            cfstore.metric.liveDiskSpaceUsed.inc(size);
            cfstore.metric.totalDiskSpaceUsed.inc(size);
            sstable.setTrackedBy(this);
        }
    }

    private void removeOldSSTablesSize(Iterable<SSTableReader> oldSSTables, boolean tolerateCompacted)
    {
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isDebugEnabled())
                logger.debug(String.format("removing %s from list of files tracked for %s.%s",
                            sstable.descriptor, cfstore.keyspace.getName(), cfstore.name));
            long size = sstable.bytesOnDisk();
            StorageMetrics.load.dec(size);
            cfstore.metric.liveDiskSpaceUsed.dec(size);

            // tolerateCompacted will be true when the CFS is no longer valid (dropped). If there were ongoing
            // compactions when it was invalidated, sstables may already be marked compacted, so we should
            // tolerate that (see CASSANDRA-5957)
            boolean firstToCompact = sstable.markObsolete();
            assert (tolerateCompacted || firstToCompact) : sstable + " was already marked compacted";

            sstable.releaseReference();
        }
    }

    public void spaceReclaimed(long size)
    {
        cfstore.metric.totalDiskSpaceUsed.dec(size);
    }

    public long estimatedKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables())
            n += sstable.estimatedKeys();
        return n;
    }

    public int getMeanColumns()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables())
        {
            long n = sstable.getEstimatedColumnCount().count();
            sum += sstable.getEstimatedColumnCount().mean() * n;
            count += n;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables())
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - sstable.metadata.getGcGraceSeconds());
            allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count();
        }
        if (allColumns > 0)
        {
            return allDroppable / allColumns;
        }
        return 0;
    }

    public void notifySSTablesChanged(Collection<SSTableReader> removed, Collection<SSTableReader> added, OperationType compactionType)
    {
        INotification notification = new SSTableListChangedNotification(added, removed, compactionType);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyAdded(SSTableReader added)
    {
        INotification notification = new SSTableAddedNotification(added);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyDeleting(SSTableReader deleting)
    {
        INotification notification = new SSTableDeletingNotification(deleting);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyRenewed(Memtable renewed)
    {
        INotification notification = new MemtableRenewedNotification(renewed);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyTruncated(long truncatedAt)
    {
        INotification notification = new TruncationNotification(truncatedAt);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void subscribe(INotificationConsumer consumer)
    {
        subscribers.add(consumer);
    }

    public void unsubscribe(INotificationConsumer consumer)
    {
        subscribers.remove(consumer);
    }

    public static SSTableIntervalTree buildIntervalTree(Iterable<SSTableReader> sstables)
    {
        List<Interval<RowPosition, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return new SSTableIntervalTree(intervals);
    }

    public Set<SSTableReader> getCompacting()
    {
        return getView().compacting;
    }

    public static class SSTableIntervalTree extends IntervalTree<RowPosition, SSTableReader, Interval<RowPosition, SSTableReader>>
    {
        private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

        private SSTableIntervalTree(Collection<Interval<RowPosition, SSTableReader>> intervals)
        {
            super(intervals, null);
        }

        public static SSTableIntervalTree empty()
        {
            return EMPTY;
        }
    }

    /**
     * An immutable structure holding the current memtable, the memtables pending
     * flush, the sstables for a column family, and the sstables that are active
     * in compaction (a subset of the sstables).
     */
    static class View
    {
        public final Memtable memtable;
        public final Set<Memtable> memtablesPendingFlush;
        public final Set<SSTableReader> compacting;
        public final Set<SSTableReader> sstables;
        public final SSTableIntervalTree intervalTree;

        View(Memtable memtable, Set<Memtable> pendingFlush, Set<SSTableReader> sstables, Set<SSTableReader> compacting, SSTableIntervalTree intervalTree)
        {
            assert memtable != null;
            assert pendingFlush != null;
            assert sstables != null;
            assert compacting != null;
            assert intervalTree != null;

            this.memtable = memtable;
            this.memtablesPendingFlush = pendingFlush;
            this.sstables = sstables;
            this.compacting = compacting;
            this.intervalTree = intervalTree;
        }

        public Sets.SetView<SSTableReader> nonCompactingSStables()
        {
            return Sets.difference(ImmutableSet.copyOf(sstables), compacting);
        }

        public View switchMemtable(Memtable newMemtable)
        {
            Set<Memtable> newPending = ImmutableSet.<Memtable>builder().addAll(memtablesPendingFlush).add(memtable).build();
            return new View(newMemtable, newPending, sstables, compacting, intervalTree);
        }

        public View renewMemtable(Memtable newMemtable)
        {
            return new View(newMemtable, memtablesPendingFlush, sstables, compacting, intervalTree);
        }

        public View replaceFlushed(Memtable flushedMemtable, SSTableReader newSSTable)
        {
            Set<Memtable> newPending = ImmutableSet.copyOf(Sets.difference(memtablesPendingFlush, Collections.singleton(flushedMemtable)));
            Set<SSTableReader> newSSTables = newSSTable == null
                                           ? sstables
                                           : newSSTables(newSSTable);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(memtable, newPending, newSSTables, compacting, intervalTree);
        }

        public View replace(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            Set<SSTableReader> newSSTables = newSSTables(oldSSTables, replacements);
            SSTableIntervalTree intervalTree = buildIntervalTree(newSSTables);
            return new View(memtable, memtablesPendingFlush, newSSTables, compacting, intervalTree);
        }

        public View markCompacting(Collection<SSTableReader> tomark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.<SSTableReader>builder().addAll(compacting).addAll(tomark).build();
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree);
        }

        public View unmarkCompacting(Iterable<SSTableReader> tounmark)
        {
            Set<SSTableReader> compactingNew = ImmutableSet.copyOf(Sets.difference(compacting, ImmutableSet.copyOf(tounmark)));
            return new View(memtable, memtablesPendingFlush, sstables, compactingNew, intervalTree);
        }

        private Set<SSTableReader> newSSTables(SSTableReader newSSTable)
        {
            assert newSSTable != null;
            // not performance-sensitive, don't obsess over doing a selection merge here
            return newSSTables(Collections.<SSTableReader>emptyList(), Collections.singletonList(newSSTable));
        }

        private Set<SSTableReader> newSSTables(Collection<SSTableReader> oldSSTables, Iterable<SSTableReader> replacements)
        {
            ImmutableSet<SSTableReader> oldSet = ImmutableSet.copyOf(oldSSTables);
            int newSSTablesSize = sstables.size() - oldSSTables.size() + Iterables.size(replacements);
            assert newSSTablesSize >= Iterables.size(replacements) : String.format("Incoherent new size %d replacing %s by %s in %s", newSSTablesSize, oldSSTables, replacements, this);
            Set<SSTableReader> newSSTables = new HashSet<>(newSSTablesSize);

            for (SSTableReader sstable : sstables)
                if (!oldSet.contains(sstable))
                    newSSTables.add(sstable);

            Iterables.addAll(newSSTables, replacements);
            assert newSSTables.size() == newSSTablesSize : String.format("Expecting new size of %d, got %d while replacing %s by %s in %s", newSSTablesSize, newSSTables.size(), oldSSTables, replacements, this);
            return ImmutableSet.copyOf(newSSTables);
        }

        @Override
        public String toString()
        {
            return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", memtablesPendingFlush.size(), sstables, compacting);
        }

        public List<SSTableReader> sstablesInBounds(AbstractBounds<RowPosition> rowBounds)
        {
            RowPosition stopInTree = rowBounds.right.isMinimum(memtable.cfs.partitioner) ? intervalTree.max() : rowBounds.right;
            return intervalTree.search(Interval.<RowPosition, SSTableReader>create(rowBounds.left, stopInTree));
        }
    }
}
