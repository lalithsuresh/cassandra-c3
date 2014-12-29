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

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexColumnSizeTest
{
    @Test
    public void test64kColumn()
    {
        // a byte buffer more than 64k
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 65);
        buffer.clear();

        //read more than 64k
        for (int i=0; i<1024*64/4 + 1; i++)
            buffer.putInt(0);

        // for read
        buffer.flip();
        Column column = new Column(ByteBufferUtil.bytes("test"), buffer, 0);

        SecondaryIndexColumnSizeTest.MockRowIndex mockRowIndex = new SecondaryIndexColumnSizeTest.MockRowIndex();
        SecondaryIndexColumnSizeTest.MockColumnIndex mockColumnIndex = new SecondaryIndexColumnSizeTest.MockColumnIndex();

        assertTrue(mockRowIndex.validate(column));
        assertFalse(mockColumnIndex.validate(column));

        // test less than 64k value
        buffer.flip();
        buffer.clear();
        buffer.putInt(20);
        buffer.flip();

        assertTrue(mockRowIndex.validate(column));
        assertTrue(mockColumnIndex.validate(column));
    }

    private class MockRowIndex extends PerRowSecondaryIndex
    {
        @Override
        public void init()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return null;
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public long getLiveSize()
        {
            return 0;
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return null;
        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        public void index(ByteBuffer rowKey, ColumnFamily cf)
        {
        }

        public void index(ByteBuffer rowKey)
        {
        }

        public void delete(DecoratedKey key)
        {
        }

        @Override
        public void reload()
        {
        }
    }


    private class MockColumnIndex extends PerColumnSecondaryIndex
    {
        @Override
        public void init()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return null;
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public long getLiveSize()
        {
            return 0;
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return null;
        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public void delete(ByteBuffer rowKey, Column col)
        {
        }

        @Override
        public void deleteForCleanup(ByteBuffer rowKey, Column col)
        {
        }

        @Override
        public void insert(ByteBuffer rowKey, Column col)
        {
        }

        @Override
        public void update(ByteBuffer rowKey, Column col)
        {
        }

        @Override
        public void reload()
        {
        }
    }
}
