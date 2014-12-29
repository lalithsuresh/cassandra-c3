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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ColumnFamilyMetricTest extends SchemaLoader
{
    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        store.disableAutoCompaction();

        store.truncateBlocking();

        assertEquals(0, store.metric.liveDiskSpaceUsed.count());
        assertEquals(0, store.metric.totalDiskSpaceUsed.count());

        for (int j = 0; j < 10; j++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add("Standard1", ByteBufferUtil.bytes("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        Collection<SSTableReader> sstables = store.getSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, store.metric.liveDiskSpaceUsed.count());
        assertEquals(size, store.metric.totalDiskSpaceUsed.count());

        store.truncateBlocking();

        // after truncate, size metrics should be down to 0
        assertEquals(0, store.metric.liveDiskSpaceUsed.count());
        assertEquals(0, store.metric.totalDiskSpaceUsed.count());

        store.enableAutoCompaction();
    }

}
