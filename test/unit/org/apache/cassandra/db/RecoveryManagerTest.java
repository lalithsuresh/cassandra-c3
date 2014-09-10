/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.Util;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;

@RunWith(OrderedJUnit4ClassRunner.class)
public class RecoveryManagerTest extends SchemaLoader
{
    @Test
    public void testNothingToRecover() throws IOException {
        CommitLog.instance.recover();
    }

    @Test
    public void testOne() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace1 = Keyspace.open("Keyspace1");
        Keyspace keyspace2 = Keyspace.open("Keyspace2");

        RowMutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        cf = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm = new RowMutation("Keyspace1", dk.key, cf);
        rm.apply();

        cf = TreeMapBackedSortedColumns.factory.create("Keyspace2", "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm = new RowMutation("Keyspace2", dk.key, cf);
        rm.apply();

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.instance.recover();

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard3"), "col2");
    }

    @Test
    public void testRecoverCounter() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace1 = Keyspace.open("Keyspace1");

        RowMutation rm;
        DecoratedKey dk = Util.dk("key");
        ColumnFamily cf;

        for (int i = 0; i < 10; ++i)
        {
            cf = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Counter1");
            cf.addColumn(new CounterColumn(ByteBufferUtil.bytes("col"), 1L, 1L));
            rm = new RowMutation("Keyspace1", dk.key, cf);
            rm.apply();
        }

        keyspace1.getColumnFamilyStore("Counter1").clearUnsafe();

        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.instance.recover();

        cf = Util.getColumnFamily(keyspace1, dk, "Counter1");

        assert cf.getColumnCount() == 1;
        Column c = cf.getColumn(ByteBufferUtil.bytes("col"));

        assert c != null;
        assert ((CounterColumn)c).total() == 10L;
    }

    @Test
    public void testRecoverPIT() throws Exception
    {
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime() - 5000;

        Keyspace keyspace1 = Keyspace.open("Keyspace1");
        DecoratedKey dk = Util.dk("dkey");
        for (int i = 0; i < 10; ++i)
        {
            long ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));
            ColumnFamily cf = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
            cf.addColumn(column("name-" + i, "value", ts));
            RowMutation rm = new RowMutation("Keyspace1", dk.key, cf);
            rm.apply();
        }
        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.instance.recover();

        ColumnFamily cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(6, cf.getColumnCount());
    }


    @Test
    public void testRecoverPITUnordered() throws Exception
    {
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime();

        Keyspace keyspace1 = Keyspace.open("Keyspace1");
        DecoratedKey dk = Util.dk("dkey");

        // Col 0 and 9 are the only ones to be recovered
        for (int i = 0; i < 10; ++i)
        {
            long ts;
            if(i==9)
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS - 1000);
            else
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));

            ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
            cf.addColumn(column("name-" + i, "value", ts));
            RowMutation rm = new RowMutation("Keyspace1", dk.key, cf);
            rm.apply();
        }

        ColumnFamily cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(10, cf.getColumnCount());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.instance.recover();

        cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(2, cf.getColumnCount());
    }
}
