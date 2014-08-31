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

import org.apache.cassandra.net.*;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.concurrent.atomic.AtomicInteger;

public class ReadVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger( ReadVerbHandler.class );

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        AtomicInteger counter = MessagingService.instance().getPendingRequestsCounter(FBUtilities.getBroadcastAddress());
        counter.incrementAndGet();
        long start = System.nanoTime();
        ReadCommand command = message.payload;
        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row = command.getRow(keyspace);

        int queueSize = counter.decrementAndGet();
        MessageOut<ReadResponse> reply = new MessageOut<ReadResponse>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                      getResponse(command, row),
                                                                      ReadResponse.serializer)
                .withParameter(MacheteMetrics.MU, ByteBufferUtil.bytes(System.nanoTime() - start).array())
                .withParameter(MacheteMetrics.QSZ, ByteBufferUtil.bytes(queueSize).array());
        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            return new ReadResponse(ColumnFamily.digest(row.cf));
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
