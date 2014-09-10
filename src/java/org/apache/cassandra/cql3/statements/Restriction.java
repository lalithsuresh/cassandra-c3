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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.cql3.*;

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 */
public interface Restriction
{
    public boolean isOnToken();

    public boolean isSlice();
    public boolean isEQ();
    public boolean isIN();
    public boolean isMultiColumn();

    // Only supported for EQ and IN, but it's convenient to have here
    public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException;

    public static interface EQ extends Restriction {}

    public static interface IN extends Restriction
    {
        public boolean canHaveOnlyOneValue();
    }

    public static interface Slice extends Restriction
    {
        public List<ByteBuffer> values(List<ByteBuffer> variables) throws InvalidRequestException;

        /** Returns true if the start or end bound (depending on the argument) is set, false otherwise */
        public boolean hasBound(Bound b);

        public ByteBuffer bound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException;

        /** Returns true if the start or end bound (depending on the argument) is inclusive, false otherwise */
        public boolean isInclusive(Bound b);

        public Relation.Type getRelation(Bound eocBound, Bound inclusiveBound);

        public IndexOperator getIndexOperator(Bound b);

        public void setBound(Relation.Type type, Term t) throws InvalidRequestException;
    }
}
