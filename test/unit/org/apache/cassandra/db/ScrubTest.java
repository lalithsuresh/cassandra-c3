package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ScrubTest extends SchemaLoader
{
    public String KEYSPACE = "Keyspace1";
    public String CF = "Standard1";
    public String CF3 = "Standard2";
    public String COUNTER_CF = "Counter1";

    @Test
    public void testScrubOneRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());
    }

    @Test
    public void testScrubCorruptedCounterRow() throws IOException, InterruptedException, ExecutionException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.clearUnsafe();

        fillCounterCF(cfs, 2);

        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(2, rows.size());

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        // overwrite one row with garbage
        long row0Start = sstable.getPosition(RowPosition.forKey(ByteBufferUtil.bytes("0"), sstable.partitioner), SSTableReader.Operator.EQ).position;
        long row1Start = sstable.getPosition(RowPosition.forKey(ByteBufferUtil.bytes("1"), sstable.partitioner), SSTableReader.Operator.EQ).position;
        long startPosition = row0Start < row1Start ? row0Start : row1Start;
        long endPosition = row0Start < row1Start ? row1Start : row0Start;

        RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw");
        file.seek(startPosition);
        file.writeBytes(StringUtils.repeat('z', (int) (endPosition - startPosition)));
        file.close();

        // with skipCorrupted == false, the scrub is expected to fail
        Scrubber scrubber = new Scrubber(cfs, sstable, false);
        try
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt row will be skipped
        scrubber = new Scrubber(cfs, sstable, true);
        scrubber.scrub();
        scrubber.close();
        cfs.replaceCompactedSSTables(Collections.singletonList(sstable), Collections.singletonList(scrubber.getNewSSTable()), OperationType.SCRUB);
        assertEquals(1, cfs.getSSTables().size());

        // verify that we can read all of the rows, and there is now one less row
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());
    }

    @Test
    public void testScrubDeletedRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF3);
        cfs.clearUnsafe();

        ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(KEYSPACE, CF3);
        cf.delete(new DeletionInfo(0, 1)); // expired tombstone
        RowMutation rm = new RowMutation(KEYSPACE, ByteBufferUtil.bytes(1), cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs, false);
        assert cfs.getSSTables().isEmpty();
    }

    @Test
    public void testScrubMultiRow() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(10, rows.size());

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(10, rows.size());
    }

    @Test
    public void testScrubOutOfOrder() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String columnFamily = "Standard3";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        /*
         * Code used to generate an outOfOrder sstable. The test for out-of-order key in SSTableWriter must also be commented out.
         * The test also assumes an ordered partitioner.
         *
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cf.addColumn(new Column(ByteBufferUtil.bytes("someName"), ByteBufferUtil.bytes("someValue"), 0L));

        SSTableWriter writer = new SSTableWriter(cfs.getTempSSTablePath(new File(System.getProperty("corrupt-sstable-root"))),
                                                 cfs.metadata.getIndexInterval(),
                                                 cfs.metadata,
                                                 cfs.partitioner,
                                                 SSTableMetadata.createCollector(BytesType.instance));
        writer.append(Util.dk("a"), cf);
        writer.append(Util.dk("b"), cf);
        writer.append(Util.dk("z"), cf);
        writer.append(Util.dk("c"), cf);
        writer.append(Util.dk("y"), cf);
        writer.append(Util.dk("d"), cf);
        writer.closeAndOpenReader();
        */

        String root = System.getProperty("corrupt-sstable-root");
        assert root != null;
        File rootDir = new File(root);
        assert rootDir.isDirectory();
        Descriptor desc = new Descriptor(new Descriptor.Version("jb"), rootDir, KEYSPACE, columnFamily, 1, false);
        CFMetaData metadata = Schema.instance.getCFMetaData(desc.ksname, desc.cfname);

        try
        {
            SSTableReader.open(desc, metadata);
            fail("SSTR validation should have caught the out-of-order rows");
        }
        catch (IllegalStateException ise) { /* this is expected */ }

        // open without validation for scrubbing
        Set<Component> components = new HashSet<>();
        components.add(Component.COMPRESSION_INFO);
        components.add(Component.DATA);
        components.add(Component.PRIMARY_INDEX);
        components.add(Component.FILTER);
        components.add(Component.STATS);
        components.add(Component.SUMMARY);
        components.add(Component.TOC);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, components, metadata);

        Scrubber scrubber = new Scrubber(cfs, sstable, false);
        scrubber.scrub();

        cfs.loadNewSSTables();
        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assert isRowOrdered(rows) : "Scrub failed: " + rows;
        assert rows.size() == 6 : "Got " + rows.size();
    }

    private static boolean isRowOrdered(List<Row> rows)
    {
        DecoratedKey prev = null;
        for (Row row : rows)
        {
            if (prev != null && prev.compareTo(row.key) > 0)
                return false;
            prev = row.key;
        }
        return true;
    }

    protected void fillCF(ColumnFamilyStore cfs, int rowsPerSSTable) throws ExecutionException, InterruptedException, IOException
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(KEYSPACE, CF);
            cf.addColumn(column("c1", "1", 1L));
            cf.addColumn(column("c2", "2", 1L));
            RowMutation rm = new RowMutation(KEYSPACE, ByteBufferUtil.bytes(key), cf);
            rm.applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, int rowsPerSSTable) throws ExecutionException, InterruptedException, IOException
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(KEYSPACE, COUNTER_CF);
            RowMutation rm = new RowMutation(KEYSPACE, ByteBufferUtil.bytes(key), cf);
            rm.addCounter(COUNTER_CF, ByteBufferUtil.bytes("Column1"), 100);
            CounterMutation cm = new CounterMutation(rm, ConsistencyLevel.ONE);
            cm.apply();
        }

        cfs.forceBlockingFlush();
    }

    @Test
    public void testScrubColumnValidation() throws InterruptedException, RequestExecutionException, ExecutionException
    {
        QueryProcessor.process("CREATE TABLE \"Keyspace1\".test_compact_static_columns (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))", ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_static_columns");

        QueryProcessor.processInternal("INSERT INTO \"Keyspace1\".test_compact_static_columns (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false);
    }

    /**
     * Tests CASSANDRA-6892 (key aliases being used improperly for validation)
     */
    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("UUIDKeys");

        ColumnFamily cf = TreeMapBackedSortedColumns.factory.create("Keyspace1", "UUIDKeys");
        cf.addColumn(column(CFMetaData.DEFAULT_KEY_ALIAS, "not a uuid", 1L));
        RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes(UUIDGen.getTimeUUID()), cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false);

        assertEquals(1, cfs.getSSTables().size());
    }

    /**
     * For CASSANDRA-6892 too, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage() throws Exception
    {
        QueryProcessor.process("CREATE TABLE \"Keyspace1\".test_compact_dynamic_columns (a int, b text, c text, PRIMARY KEY (a, b)) WITH COMPACT STORAGE", ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_dynamic_columns");

        QueryProcessor.processInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'a', 'foo')");
        QueryProcessor.processInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'b', 'bar')");
        QueryProcessor.processInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'c', 'boo')");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, true);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        UntypedResultSet rs = QueryProcessor.processInternal("SELECT * FROM \"Keyspace1\".test_compact_dynamic_columns");
        assertEquals(3, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        assertEquals("foo", iter.next().getString("c"));
        assertEquals("bar", iter.next().getString("c"));
        assertEquals("boo", iter.next().getString("c"));
    }
}
