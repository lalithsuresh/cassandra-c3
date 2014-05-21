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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.primitives.Doubles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;

public class LeveledCompactionStrategy extends AbstractCompactionStrategy implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);
    private static final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";

    @VisibleForTesting
    final LeveledManifest manifest;
    private final int maxSSTableSizeInMB;

    public LeveledCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        int configuredMaxSSTableSize = 160;
        SizeTieredCompactionStrategyOptions localOptions = new SizeTieredCompactionStrategyOptions(options);
        if (options != null)
        {             
            if (options.containsKey(SSTABLE_SIZE_OPTION))             
            {                 
                configuredMaxSSTableSize = Integer.parseInt(options.get(SSTABLE_SIZE_OPTION));                 
                if (!Boolean.getBoolean("cassandra.tolerate_sstable_size"))                 
                {                     
                    if (configuredMaxSSTableSize >= 1000)
                        logger.warn("Max sstable size of {}MB is configured for {}.{}; having a unit of compaction this large is probably a bad idea",
                                configuredMaxSSTableSize, cfs.name, cfs.getColumnFamilyName());
                    if (configuredMaxSSTableSize < 50)  
                        logger.warn("Max sstable size of {}MB is configured for {}.{}.  Testing done for CASSANDRA-5727 indicates that performance improves up to 160MB",
                                configuredMaxSSTableSize, cfs.name, cfs.getColumnFamilyName());
                }
            }
        }
        maxSSTableSizeInMB = configuredMaxSSTableSize;

        manifest = LeveledManifest.create(cfs, this.maxSSTableSizeInMB, cfs.getSSTables(), localOptions);
        logger.debug("Created {}", manifest);
    }

    @Override
    public void startup()
    {
        super.startup();
        cfs.getDataTracker().subscribe(this);
        logger.debug("{} subscribed to the data tracker.", this);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        cfs.getDataTracker().unsubscribe(this);
        logger.debug("{} unsubscribed from the data tracker.", this);
    }

    public int getLevelSize(int i)
    {
        return manifest.getLevelSize(i);
    }

    public int[] getAllLevelSize()
    {
        return manifest.getAllLevelSize();
    }

    /**
     * the only difference between background and maximal in LCS is that maximal is still allowed
     * (by explicit user request) even when compaction is disabled.
     */
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        return getMaximalTask(gcBefore);
    }

    public AbstractCompactionTask getMaximalTask(int gcBefore)
    {
        while (true)
        {
            OperationType op;
            LeveledManifest.CompactionCandidate candidate = manifest.getCompactionCandidates();
            if (candidate == null)
            {
                // if there is no sstable to compact in standard way, try compacting based on droppable tombstone ratio
                SSTableReader sstable = findDroppableSSTable(gcBefore);
                if (sstable == null)
                {
                    logger.debug("No compaction necessary for {}", this);
                    return null;
                }
                candidate = new LeveledManifest.CompactionCandidate(Collections.singleton(sstable),
                                                                    sstable.getSSTableLevel(),
                                                                    getMaxSSTableBytes());
                op = OperationType.TOMBSTONE_COMPACTION;
            }
            else
            {
                op = OperationType.COMPACTION;
            }

            if (cfs.getDataTracker().markCompacting(candidate.sstables))
            {
                LeveledCompactionTask newTask = new LeveledCompactionTask(cfs, candidate.sstables, candidate.level, gcBefore, candidate.maxSSTableBytes);
                newTask.setCompactionType(op);
                return newTask;
            }
        }
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        throw new UnsupportedOperationException("LevelDB compaction strategy does not allow user-specified compactions");
    }

    @Override
    public AbstractCompactionTask getCompactionTask(Collection<SSTableReader> sstables, int gcBefore, long maxSSTableBytes)
    {
        assert sstables.size() > 0;
        int level = -1;
        // if all sstables are in the same level, we can set that level:
        for (SSTableReader sstable : sstables)
        {
            if (level == -1)
                level = sstable.getSSTableLevel();
            if (level != sstable.getSSTableLevel())
                level = 0;
        }
        return new LeveledCompactionTask(cfs, sstables, level, gcBefore, maxSSTableBytes);
    }

    public int getEstimatedRemainingTasks()
    {
        return manifest.getEstimatedTasks();
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            manifest.add(flushedNotification.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            manifest.replace(listChangedNotification.removed, listChangedNotification.added);
        }
    }

    public long getMaxSSTableBytes()
    {
        return maxSSTableSizeInMB * 1024L * 1024L;
    }

    public List<ICompactionScanner> getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        Multimap<Integer, SSTableReader> byLevel = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
            byLevel.get(sstable.getSSTableLevel()).add(sstable);

        List<ICompactionScanner> scanners = new ArrayList<ICompactionScanner>(sstables.size());
        for (Integer level : byLevel.keySet())
        {
            // level can be -1 when sstables are added to DataTracker but not to LeveledManifest
            // since we don't know which level those sstable belong yet, we simply do the same as L0 sstables.
            if (level <= 0)
            {
                // L0 makes no guarantees about overlapping-ness.  Just create a direct scanner for each
                for (SSTableReader sstable : byLevel.get(level))
                    scanners.add(sstable.getScanner(range, CompactionManager.instance.getRateLimiter()));
            }
            else
            {
                // Create a LeveledScanner that only opens one sstable at a time, in sorted order
                List<SSTableReader> intersecting = LeveledScanner.intersecting(byLevel.get(level), range);
                if (!intersecting.isEmpty())
                    scanners.add(new LeveledScanner(intersecting, range));
            }
        }

        return scanners;
    }

    // Lazily creates SSTableBoundedScanner for sstable that are assumed to be from the
    // same level (e.g. non overlapping) - see #4142
    private static class LeveledScanner extends AbstractIterator<OnDiskAtomIterator> implements ICompactionScanner
    {
        private final Range<Token> range;
        private final List<SSTableReader> sstables;
        private final Iterator<SSTableReader> sstableIterator;
        private final long totalLength;

        private ICompactionScanner currentScanner;
        private long positionOffset;

        public LeveledScanner(Collection<SSTableReader> sstables, Range<Token> range)
        {
            this.range = range;

            // add only sstables that intersect our range, and estimate how much data that involves
            this.sstables = new ArrayList<SSTableReader>(sstables.size());
            long length = 0;
            for (SSTableReader sstable : sstables)
            {
                this.sstables.add(sstable);
                long estimatedKeys = sstable.estimatedKeys();
                double estKeysInRangeRatio = 1.0;

                if (estimatedKeys > 0 && range != null)
                    estKeysInRangeRatio = ((double) sstable.estimatedKeysForRanges(Collections.singleton(range))) / estimatedKeys;

                length += sstable.uncompressedLength() * estKeysInRangeRatio;
            }

            totalLength = length;
            Collections.sort(this.sstables, SSTable.sstableComparator);
            sstableIterator = this.sstables.iterator();
            assert sstableIterator.hasNext(); // caller should check intersecting first
            currentScanner = sstableIterator.next().getScanner(range, CompactionManager.instance.getRateLimiter());
        }

        public static List<SSTableReader> intersecting(Collection<SSTableReader> sstables, Range<Token> range)
        {
            ArrayList<SSTableReader> filtered = new ArrayList<SSTableReader>();
            for (SSTableReader sstable : sstables)
            {
                Range<Token> sstableRange = new Range<Token>(sstable.first.getToken(), sstable.last.getToken(), sstable.partitioner);
                if (range == null || sstableRange.intersects(range))
                    filtered.add(sstable);
            }
            return filtered;
        }

        protected OnDiskAtomIterator computeNext()
        {
            if (currentScanner == null)
                return endOfData();

            try
            {
                while (true)
                {
                    if (currentScanner.hasNext())
                        return currentScanner.next();

                    positionOffset += currentScanner.getLengthInBytes();
                    currentScanner.close();
                    if (!sstableIterator.hasNext())
                    {
                        // reset to null so getCurrentPosition does not return wrong value
                        currentScanner = null;
                        return endOfData();
                    }
                    currentScanner = sstableIterator.next().getScanner(range, CompactionManager.instance.getRateLimiter());
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void close() throws IOException
        {
            if (currentScanner != null)
                currentScanner.close();
        }

        public long getLengthInBytes()
        {
            return totalLength;
        }

        public long getCurrentPosition()
        {
            return positionOffset + (currentScanner == null ? 0L : currentScanner.getCurrentPosition());
        }

        public String getBackingFiles()
        {
            return Joiner.on(", ").join(sstables);
        }
    }

    @Override
    public String toString()
    {
        return String.format("LCS@%d(%s)", hashCode(), cfs.name);
    }

    private SSTableReader findDroppableSSTable(final int gcBefore)
    {
        level:
        for (int i = manifest.getLevelCount(); i >= 0; i--)
        {
            // sort sstables by droppable ratio in descending order
            SortedSet<SSTableReader> sstables = manifest.getLevelSorted(i, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    double r1 = o1.getEstimatedDroppableTombstoneRatio(gcBefore);
                    double r2 = o2.getEstimatedDroppableTombstoneRatio(gcBefore);
                    return -1 * Doubles.compare(r1, r2);
                }
            });
            if (sstables.isEmpty())
                continue;

            Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
            for (SSTableReader sstable : sstables)
            {
                if (sstable.getEstimatedDroppableTombstoneRatio(gcBefore) <= tombstoneThreshold)
                    continue level;
                else if (!compacting.contains(sstable) && !sstable.isMarkedSuspect() && worthDroppingTombstones(sstable, gcBefore))
                    return sstable;
            }
        }
        return null;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String size = options.containsKey(SSTABLE_SIZE_OPTION) ? options.get(SSTABLE_SIZE_OPTION) : "1";
        try
        {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 1)
            {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", SSTABLE_SIZE_OPTION, ssSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, SSTABLE_SIZE_OPTION), ex);
        }

        uncheckedOptions.remove(SSTABLE_SIZE_OPTION);

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }
}
