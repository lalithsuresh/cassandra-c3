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

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

/**
 * A counter update while it hasn't been applied yet by the leader replica.
 *
 * Contains a single counter update. When applied by the leader replica, this
 * is transformed to a relevant CounterColumn. This Column is a temporary data
 * structure that should never be stored inside a memtable or an sstable.
 */
public class CounterUpdateColumn extends Column
{
    public CounterUpdateColumn(ByteBuffer name, long value, long timestamp)
    {
        this(name, ByteBufferUtil.bytes(value), timestamp);
    }

    public CounterUpdateColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    public long delta()
    {
        return value().getLong(value().position());
    }

    @Override
    public Column diff(Column column)
    {
        // Diff is used during reads, but we should never read those columns
        throw new UnsupportedOperationException("This operation is unsupported on CounterUpdateColumn.");
    }

    @Override
    public CounterUpdateColumn withUpdatedName(ByteBuffer newName)
    {
        return new CounterUpdateColumn(newName, value, timestamp);
    }

    @Override
    public Column reconcile(Column column, Allocator allocator)
    {
        // The only time this could happen is if a batchAdd ships two
        // increment for the same column. Hence we simply sums the delta.

        assert (column instanceof CounterUpdateColumn) || (column instanceof DeletedColumn) : "Wrong class type.";

        // tombstones take precedence
        if (column.isMarkedForDelete(Long.MIN_VALUE)) // can't be an expired column, so the current time is irrelevant
            return timestamp() > column.timestamp() ? this : column;

        // neither is tombstoned
        CounterUpdateColumn c = (CounterUpdateColumn)column;
        return new CounterUpdateColumn(name(), delta() + c.delta(), Math.max(timestamp(), c.timestamp()));
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_UPDATE_MASK;
    }

    @Override
    public CounterColumn localCopy(ColumnFamilyStore cfs)
    {
        return new CounterColumn(cfs.internOrCopy(name, HeapAllocator.instance),
                                 CounterContext.instance().createLocal(delta(), HeapAllocator.instance),
                                 timestamp(),
                                 Long.MIN_VALUE);
    }

    @Override
    public Column localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new CounterColumn(cfs.internOrCopy(name, allocator),
                                 CounterContext.instance().createLocal(delta(), allocator),
                                 timestamp(),
                                 Long.MIN_VALUE);
    }
}
