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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableDeletingTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDeletingTask.class);

    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Set<SSTableDeletingTask> failedTasks = new CopyOnWriteArraySet<SSTableDeletingTask>();

    private final SSTableReader referent;
    private final Descriptor desc;
    private final Set<Component> components;
    private DataTracker tracker;

    public SSTableDeletingTask(SSTableReader referent)
    {
        this.referent = referent;
        this.desc = referent.descriptor;
        this.components = referent.components;
    }

    public void setTracker(DataTracker tracker)
    {
        this.tracker = tracker;
    }

    public void schedule()
    {
        StorageService.tasks.submit(this);
    }

    public void run()
    {
        long size = referent.bytesOnDisk();

        if (tracker != null)
            tracker.notifyDeleting(referent);

        if (referent.readMeter != null)
            SystemKeyspace.clearSSTableReadMeter(referent.getKeyspaceName(), referent.getColumnFamilyName(), referent.descriptor.generation);

        // If we can't successfully delete the DATA component, set the task to be retried later: see above
        File datafile = new File(desc.filenameFor(Component.DATA));
        if (!datafile.delete())
        {
            logger.error("Unable to delete " + datafile + " (it will be removed on server restart; we'll also retry after GC)");
            failedTasks.add(this);
            return;
        }
        // let the remainder be cleaned up by delete
        SSTable.delete(desc, Sets.difference(components, Collections.singleton(Component.DATA)));
        if (tracker != null)
            tracker.spaceReclaimed(size);
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedTasks()
    {
        for (SSTableDeletingTask task : failedTasks)
        {
            failedTasks.remove(task);
            task.schedule();
        }
    }

    /** for tests */
    public static void waitForDeletions()
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
            }
        };

        FBUtilities.waitOnFuture(StorageService.tasks.schedule(runnable, 0, TimeUnit.MILLISECONDS));
    }
}

