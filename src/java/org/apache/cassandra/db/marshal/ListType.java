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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ListType<T> extends CollectionType<List<T>>
{
    // interning instances
    private static final Map<AbstractType<?>, ListType> instances = new HashMap<AbstractType<?>, ListType>();

    public final AbstractType<T> elements;
    public final ListSerializer<T> serializer;

    public static ListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized <T> ListType<T> getInstance(AbstractType<T> elements)
    {
        ListType<T> t = instances.get(elements);
        if (t == null)
        {
            t = new ListType<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListType(AbstractType<T> elements)
    {
        super(Kind.LIST);
        this.elements = elements;
        this.serializer = ListSerializer.getInstance(elements.getSerializer());
    }

    public AbstractType<UUID> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<T> valueComparator()
    {
        return elements;
    }

    public TypeSerializer<List<T>> getSerializer()
    {
        return serializer;
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return compareListOrSet(elements, o1, o2);
    }

    static int compareListOrSet(AbstractType<?> elementsComparator, ByteBuffer o1, ByteBuffer o2)
    {
        // Note that this is only used if the collection is inside an UDT
        if (o1 == null || !o1.hasRemaining())
            return o2 == null || !o2.hasRemaining() ? 0 : -1;
        if (o2 == null || !o2.hasRemaining())
            return 1;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int size1 = ByteBufferUtil.readShortLength(bb1);
        int size2 = ByteBufferUtil.readShortLength(bb2);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            ByteBuffer v1 = ByteBufferUtil.readBytesWithShortLength(bb1);
            ByteBuffer v2 = ByteBufferUtil.readBytesWithShortLength(bb2);
            int cmp = elementsComparator.compare(v1, v2);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public ByteBuffer serialize(List<Pair<ByteBuffer, Column>> columns)
    {
        columns = enforceLimit(columns);

        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(columns.size());
        int size = 0;
        for (Pair<ByteBuffer, Column> p : columns)
        {
            bbs.add(p.right.value());
            size += 2 + p.right.value().remaining();
        }
        return pack(bbs, columns.size(), size);
    }
}
