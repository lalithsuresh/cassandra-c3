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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A CQL3 condition.
 */
public class ColumnCondition
{
    public final CFDefinition.Name column;

    // For collection, when testing the equality of a specific element, null otherwise.
    private final Term collectionElement;

    private final Term value;

    private ColumnCondition(CFDefinition.Name column, Term collectionElement, Term value)
    {
        this.column = column;
        this.collectionElement = collectionElement;
        this.value = value;
    }

    public static ColumnCondition equal(CFDefinition.Name column, Term value)
    {
        return new ColumnCondition(column, null, value);
    }

    public static ColumnCondition equal(CFDefinition.Name column, Term collectionElement, Term value)
    {
        return new ColumnCondition(column, collectionElement, value);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (collectionElement != null)
            collectionElement.collectMarkerSpecification(boundNames);
        value.collectMarkerSpecification(boundNames);
    }

    public ColumnCondition.Bound bind(List<ByteBuffer> variables) throws InvalidRequestException
    {
        return column.type instanceof CollectionType
             ? (collectionElement == null ? new CollectionBound(this, variables) : new ElementAccessBound(this, variables))
             : new SimpleBound(this, variables);
    }

    public static abstract class Bound
    {
        public final CFDefinition.Name column;

        protected Bound(CFDefinition.Name column)
        {
            this.column = column;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException;

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        protected ColumnNameBuilder copyOrUpdatePrefix(CFMetaData cfm, ColumnNameBuilder rowPrefix)
        {
            return column.kind == CFDefinition.Name.Kind.STATIC ? cfm.getStaticColumnNameBuilder() : rowPrefix.copy();
        }

        protected boolean equalsValue(ByteBuffer value, Column c, AbstractType<?> type, long now)
        {
            return value == null
                 ? c == null || !c.isLive(now)
                 : c != null && c.isLive(now) && type.compare(c.value(), value) == 0;
        }

        protected Iterator<Column> collectionColumns(ColumnNameBuilder collectionPrefix, ColumnFamily cf, final long now)
        {
            // We are testing for collection equality, so we need to have the expected values *and* only those.
            ColumnSlice[] collectionSlice = new ColumnSlice[]{ new ColumnSlice(collectionPrefix.build(), collectionPrefix.buildAsEndOfRange()) };
            // Filter live columns, this makes things simpler afterwards
            return Iterators.filter(cf.iterator(collectionSlice), new Predicate<Column>()
            {
                public boolean apply(Column c)
                {
                    // we only care about live columns
                    return c.isLive(now);
                }
            });
        }
    }

    private static class SimpleBound extends Bound
    {
        public final ByteBuffer value;

        private SimpleBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            this.value = condition.value.bindAndGet(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            ColumnNameBuilder prefix = copyOrUpdatePrefix(current.metadata(), rowPrefix);
            ByteBuffer columnName = column.kind == CFDefinition.Name.Kind.VALUE_ALIAS
                                  ? prefix.build()
                                  : prefix.add(column.name.key).build();

            return equalsValue(value, current.getColumn(columnName), column.type, now);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof SimpleBound))
                return false;

            SimpleBound that = (SimpleBound)o;
            if (!column.equals(that.column))
                return false;

            return value == null || that.value == null
                 ? value == null && that.value == null
                 : column.type.compare(value, that.value) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, value);
        }
    }

    private static class ElementAccessBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final ByteBuffer value;

        private ElementAccessBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            this.collectionElement = condition.collectionElement.bindAndGet(variables);
            this.value = condition.value.bindAndGet(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(current.metadata(), rowPrefix).add(column.name.key);
            if (column.type instanceof MapType)
                return equalsValue(value, current.getColumn(collectionPrefix.add(collectionElement).build()), ((MapType)column.type).values, now);

            assert column.type instanceof ListType;
            int idx = ByteBufferUtil.toInt(collectionElement);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Invalid negative list index %d", idx));

            Iterator<Column> iter = collectionColumns(collectionPrefix, current, now);
            int adv = Iterators.advance(iter, idx);
            if (adv != idx || !iter.hasNext())
                throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", idx, adv));

            // We don't support null values inside collections, so a condition like 'IF l[3] = null' can only
            // be false. We do special case though, as the compare below might mind getting a null.
            if (value == null)
                return false;

            return ((ListType)column.type).elements.compare(iter.next().value(), value) == 0;
        }

        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof ElementAccessBound))
                return false;

            ElementAccessBound that = (ElementAccessBound)o;
            if (!column.equals(that.column))
                return false;

            if ((collectionElement == null) != (that.collectionElement == null))
                return false;

            if (collectionElement != null)
            {
                assert column.type instanceof ListType || column.type instanceof MapType;
                AbstractType<?> comparator = column.type instanceof ListType
                                           ? Int32Type.instance
                                           : ((MapType)column.type).keys;

                if (comparator.compare(collectionElement, that.collectionElement) != 0)
                    return false;
            }

            return column.type.compare(value, that.value) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, collectionElement, value);
        }
    }

    private static class CollectionBound extends Bound
    {
        public final Term.Terminal value;

        private CollectionBound(ColumnCondition condition, List<ByteBuffer> variables) throws InvalidRequestException
        {
            super(condition.column);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            this.value = condition.value.bind(variables);
        }

        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            CFMetaData cfm = current.metadata();

            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(cfm, rowPrefix).add(column.name.key);

            Iterator<Column> iter = collectionColumns(collectionPrefix, current, now);
            if (value == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST: return listAppliesTo((ListType)type, cfm, iter, ((Lists.Value)value).elements);
                case SET: return setAppliesTo((SetType)type, cfm, iter, ((Sets.Value)value).elements);
                case MAP: return mapAppliesTo((MapType)type, cfm, iter, ((Maps.Value)value).map);
            }
            throw new AssertionError();
        }

        private ByteBuffer collectionKey(CFMetaData cfm, Column c)
        {
            ByteBuffer[] bbs = ((CompositeType)cfm.comparator).split(c.name());
            return bbs[bbs.length - 1];
        }

        private boolean listAppliesTo(ListType type, CFMetaData cfm, Iterator<Column> iter, List<ByteBuffer> elements)
        {
            for (ByteBuffer e : elements)
                if (!iter.hasNext() || type.elements.compare(iter.next().value(), e) != 0)
                    return false;
            // We must not have more elements than expected
            return !iter.hasNext();
        }

        private boolean setAppliesTo(SetType type, CFMetaData cfm, Iterator<Column> iter, Set<ByteBuffer> elements)
        {
            Set<ByteBuffer> remaining = new TreeSet<>(type.elements);
            remaining.addAll(elements);
            while (iter.hasNext())
            {
                if (remaining.isEmpty())
                    return false;

                if (!remaining.remove(collectionKey(cfm, iter.next())))
                    return false;
            }
            return remaining.isEmpty();
        }

        private boolean mapAppliesTo(MapType type, CFMetaData cfm, Iterator<Column> iter, Map<ByteBuffer, ByteBuffer> elements)
        {
            Map<ByteBuffer, ByteBuffer> remaining = new TreeMap<>(type.keys);
            remaining.putAll(elements);
            while (iter.hasNext())
            {
                if (remaining.isEmpty())
                    return false;

                Column c = iter.next();
                ByteBuffer previous = remaining.remove(collectionKey(cfm, c));
                if (previous == null || type.values.compare(previous, c.value()) != 0)
                    return false;
            }
            return remaining.isEmpty();
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CollectionBound))
                return false;

            CollectionBound that = (CollectionBound)o;
            if (!column.equals(that.column))
                return false;

            // Slightly inefficient because it serialize the collection just for the sake of comparison.
            // We could improve by adding an equals() method to Lists.Value, Sets.Value and Maps.Value but
            // this method is only called when there is 2 conditions on the same collection to make sure
            // both are not incompatible, so overall it's probably not worth the effort.
            ByteBuffer thisVal = value.get();
            ByteBuffer thatVal = that.value.get();
            return thisVal == null || thatVal == null
                 ? thisVal == null && thatVal == null
                 : column.type.compare(thisVal, thatVal) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, value.get());
        }
    }

    public static class Raw
    {
        private final Term.Raw value;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        private Raw(Term.Raw value, Term.Raw collectionElement)
        {
            this.value = value;
            this.collectionElement = collectionElement;
        }

        public static Raw simpleEqual(Term.Raw value)
        {
            return new Raw(value, null);
        }

        public static Raw collectionEqual(Term.Raw value, Term.Raw collectionElement)
        {
            return new Raw(value, collectionElement);
        }

        public ColumnCondition prepare(CFDefinition.Name receiver) throws InvalidRequestException
        {
            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException("Condtions on counters are not supported");

            if (collectionElement == null)
                return ColumnCondition.equal(receiver, value.prepare(receiver));

            if (!(receiver.type.isCollection()))
                throw new InvalidRequestException(String.format("Invalid element access syntax for non-collection column %s", receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    return ColumnCondition.equal(receiver, collectionElement.prepare(Lists.indexSpecOf(receiver)), value.prepare(Lists.valueSpecOf(receiver)));
                case SET:
                    throw new InvalidRequestException(String.format("Invalid element access syntax for set column %s", receiver.name));
                case MAP:
                    return ColumnCondition.equal(receiver, collectionElement.prepare(Maps.keySpecOf(receiver)), value.prepare(Maps.valueSpecOf(receiver)));
            }
            throw new AssertionError();
        }
    }
}
