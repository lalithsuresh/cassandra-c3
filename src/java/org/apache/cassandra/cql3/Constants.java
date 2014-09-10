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
import java.util.List;

import org.apache.cassandra.serializers.MarshalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static helper methods and classes for constants.
 */
public abstract class Constants
{
    private static final Logger logger = LoggerFactory.getLogger(Constants.class);

    public enum Type
    {
        STRING, INTEGER, UUID, FLOAT, BOOLEAN, HEX;
    }

    public static final Term.Raw NULL_LITERAL = new Term.Raw()
    {
        private final Term.Terminal NULL_VALUE = new Value(null)
        {
            @Override
            public Terminal bind(List<ByteBuffer> values)
            {
                // We return null because that makes life easier for collections
                return null;
            }
        };

        public Term prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!isAssignableTo(receiver))
                throw new InvalidRequestException("Invalid null value for counter increment/decrement");

            return NULL_VALUE;
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return !(receiver.type instanceof CounterColumnType);
        }

        @Override
        public String toString()
        {
            return null;
        }
    };

    public static class Literal implements Term.Raw
    {
        private final Type type;
        private final String text;

        private Literal(Type type, String text)
        {
            assert type != null && text != null;
            this.type = type;
            this.text = text;
        }

        public static Literal string(String text)
        {
            return new Literal(Type.STRING, text);
        }

        public static Literal integer(String text)
        {
            return new Literal(Type.INTEGER, text);
        }

        public static Literal floatingPoint(String text)
        {
            return new Literal(Type.FLOAT, text);
        }

        public static Literal uuid(String text)
        {
            return new Literal(Type.UUID, text);
        }

        public static Literal bool(String text)
        {
            return new Literal(Type.BOOLEAN, text);
        }

        public static Literal hex(String text)
        {
            return new Literal(Type.HEX, text);
        }

        public Value prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!isAssignableTo(receiver))
                throw new InvalidRequestException(String.format("Invalid %s constant (%s) for %s of type %s", type, text, receiver, receiver.type.asCQL3Type()));

            return new Value(parsedValue(receiver.type));
        }

        private ByteBuffer parsedValue(AbstractType<?> validator) throws InvalidRequestException
        {
            if (validator instanceof ReversedType<?>)
                validator = ((ReversedType<?>) validator).baseType;
            try
            {
                // BytesType doesn't want it's input prefixed by '0x'.
                if (type == Type.HEX && validator instanceof BytesType)
                    return validator.fromString(text.substring(2));
                if (validator instanceof CounterColumnType)
                    return LongType.instance.fromString(text);
                return validator.fromString(text);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public String getRawText()
        {
            return text;
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            CQL3Type receiverType = receiver.type.asCQL3Type();
            if (receiverType.isCollection())
                return false;

            if (!(receiverType instanceof CQL3Type.Native))
                // Skip type validation for custom types. May or may not be a good idea
                return true;

            CQL3Type.Native nt = (CQL3Type.Native)receiverType;
            switch (type)
            {
                case STRING:
                    switch (nt)
                    {
                        case ASCII:
                        case TEXT:
                        case INET:
                        case VARCHAR:
                        case TIMESTAMP:
                            return true;
                    }
                    return false;
                case INTEGER:
                    switch (nt)
                    {
                        case BIGINT:
                        case COUNTER:
                        case DECIMAL:
                        case DOUBLE:
                        case FLOAT:
                        case INT:
                        case TIMESTAMP:
                        case VARINT:
                            return true;
                    }
                    return false;
                case UUID:
                    switch (nt)
                    {
                        case UUID:
                        case TIMEUUID:
                            return true;
                    }
                    return false;
                case FLOAT:
                    switch (nt)
                    {
                        case DECIMAL:
                        case DOUBLE:
                        case FLOAT:
                            return true;
                    }
                    return false;
                case BOOLEAN:
                    switch (nt)
                    {
                        case BOOLEAN:
                            return true;
                    }
                    return false;
                case HEX:
                    switch (nt)
                    {
                        case BLOB:
                            return true;
                    }
                    return false;
            }
            return false;
        }

        @Override
        public String toString()
        {
            return type == Type.STRING ? String.format("'%s'", text) : text;
        }
    }

    /**
     * A constant value, i.e. a ByteBuffer.
     */
    public static class Value extends Term.Terminal
    {
        public final ByteBuffer bytes;

        public Value(ByteBuffer bytes)
        {
            this.bytes = bytes;
        }

        public ByteBuffer get()
        {
            return bytes;
        }

        @Override
        public ByteBuffer bindAndGet(List<ByteBuffer> values)
        {
            return bytes;
        }

        @Override
        public String toString()
        {
            return ByteBufferUtil.bytesToHex(bytes);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert !(receiver.type instanceof CollectionType);
        }

        @Override
        public ByteBuffer bindAndGet(List<ByteBuffer> values) throws InvalidRequestException
        {
            try
            {
                ByteBuffer value = values.get(bindIndex);
                if (value != null)
                    receiver.type.validate(value);
                return value;
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer bytes = bindAndGet(values);
            return bytes == null ? null : new Constants.Value(bytes);
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            prefix = maybeUpdatePrefix(cf.metadata(), prefix);
            ByteBuffer cname = columnName == null ? prefix.build() : prefix.add(columnName.key).build();
            ByteBuffer value = t.bindAndGet(params.variables);
            cf.addColumn(value == null ? params.makeTombstone(cname) : params.makeColumn(cname, value));
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer bytes = t.bindAndGet(params.variables);
            if (bytes == null)
                throw new InvalidRequestException("Invalid null value for counter increment");
            long increment = ByteBufferUtil.toLong(bytes);
            prefix = maybeUpdatePrefix(cf.metadata(), prefix);
            ByteBuffer cname = columnName == null ? prefix.build() : prefix.add(columnName.key).build();
            cf.addColumn(params.makeCounter(cname, increment));
        }
    }

    public static class Substracter extends Operation
    {
        public Substracter(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer bytes = t.bindAndGet(params.variables);
            if (bytes == null)
                throw new InvalidRequestException("Invalid null value for counter increment");

            long increment = ByteBufferUtil.toLong(bytes);
            if (increment == Long.MIN_VALUE)
                throw new InvalidRequestException("The negation of " + increment + " overflows supported counter precision (signed 8 bytes integer)");

            prefix = maybeUpdatePrefix(cf.metadata(), prefix);
            ByteBuffer cname = columnName == null ? prefix.build() : prefix.add(columnName.key).build();
            cf.addColumn(params.makeCounter(cname, -increment));
        }
    }

    // This happens to also handle collection because it doesn't felt worth
    // duplicating this further
    public static class Deleter extends Operation
    {
        private final boolean isCollection;

        public Deleter(ColumnIdentifier column, boolean isCollection)
        {
            super(column, null);
            this.isCollection = isCollection;
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            ColumnNameBuilder column = maybeUpdatePrefix(cf.metadata(), prefix).add(columnName.key);

            if (isCollection)
                cf.addAtom(params.makeRangeTombstone(column.build(), column.buildAsEndOfRange()));
            else
                cf.addColumn(params.makeTombstone(column.build()));
        }
    };
}
