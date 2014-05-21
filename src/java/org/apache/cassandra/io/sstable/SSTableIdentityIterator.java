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

import java.io.*;
import java.util.Iterator;

import org.apache.cassandra.serializers.MarshalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.util.RandomAccessReader;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, OnDiskAtomIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final DataInput in;
    public final long dataSize; // we [still] require this so compaction can tell if it's safe to read the row into memory
    public final ColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final int columnCount;

    private final Iterator<OnDiskAtom> atomIterator;
    private final Descriptor.Version dataVersion;

    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    private final int expireBefore;

    private final boolean validateColumns;
    private final String filename;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataSize length of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataSize)
    {
        this(sstable, file, key, dataSize, false);
    }

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataSize length of row data
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataSize, boolean checkData)
    {
        this(sstable.metadata, file, file.getPath(), key, dataSize, checkData, sstable, ColumnSerializer.Flag.LOCAL);
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag)
    {
        assert !checkData || (sstable != null);
        this.in = in;
        this.filename = filename;
        this.key = key;
        this.dataSize = dataSize;
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
        this.flag = flag;
        this.validateColumns = checkData;
        this.dataVersion = sstable == null ? Descriptor.Version.CURRENT : sstable.descriptor.version;

        try
        {
            columnFamily = EmptyColumns.factory.create(metadata);
            columnFamily.delete(DeletionTime.serializer.deserialize(in));
            columnCount = dataVersion.hasRowSizeAndColumnCount ? in.readInt() : Integer.MAX_VALUE;
            atomIterator = columnFamily.metadata().getOnDiskIterator(in, columnCount, flag, expireBefore, dataVersion);
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        try
        {
            return atomIterator.hasNext();
        }
        catch (IOError e)
        {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
                throw new CorruptSSTableException((IOException)e.getCause(), filename);
            else
                throw e;
        }
    }

    public OnDiskAtom next()
    {
        try
        {
            OnDiskAtom atom = atomIterator.next();
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        }
        catch (MarshalException me)
        {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        // if input is from file, then return that path, otherwise it's from streaming
        if (in instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) in;
            return file.getPath();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public ColumnFamily getColumnFamilyWithColumns(ColumnFamily.Factory containerFactory)
    {
        ColumnFamily cf = columnFamily.cloneMeShallow(containerFactory, false);
        // since we already read column count, just pass that value and continue deserialization
        Iterator<OnDiskAtom> iter = cf.metadata().getOnDiskIterator(in, columnCount, flag, expireBefore, dataVersion);
        while (iter.hasNext())
            cf.addAtom(iter.next());

        if (validateColumns)
        {
            try
            {
                cf.metadata().validateColumns(cf);
            }
            catch (MarshalException e)
            {
                throw new RuntimeException("Error validating row " + key, e);
            }
        }
        return cf;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

}
