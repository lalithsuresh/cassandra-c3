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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.SelectionStrategy;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);
    protected final ReadCallback<ReadResponse, Row> handler;
    protected final ReadCommand command;
    protected final RowDigestResolver resolver;
    protected List<InetAddress> unfiltered;
    protected List<InetAddress> endpoints;
    protected final ColumnFamilyStore cfs;

    AbstractReadExecutor(ColumnFamilyStore cfs,
                         ReadCommand command,
                         ConsistencyLevel consistency_level,
                         List<InetAddress> allReplicas,
                         List<InetAddress> queryTargets)
    throws UnavailableException
    {
        unfiltered = allReplicas;
        this.endpoints = queryTargets;
        this.resolver = new RowDigestResolver(command.ksName, command.key);
        this.handler = new ReadCallback<ReadResponse, Row>(resolver, consistency_level, command, this.endpoints);
        this.command = command;
        this.cfs = cfs;

        handler.assureSufficientLiveNodes();
        assert !handler.endpoints.isEmpty();
    }

    void executeAsync()
    {
        ActorRef actor = MessagingService.instance().getReplicaGroupActor(handler.endpoints);
        actor.tell(this, null);
    }

    public double pushRead() {

        // We block until the upstream queues look all right if we're using one of our
        // strategies
        if (!DatabaseDescriptor.getScoreStrategy().equals(SelectionStrategy.DEFAULT)) {
            List<InetAddress> newEndpointList = StorageProxy.getLiveSortedEndpoints(Keyspace.open(command.ksName), command.key);
            int originalSize = handler.endpoints.size();
            boolean shouldWait = true;
            int dataEndpointIndex = 0;

            //
            // This is our backpressure knob. If we exceed the rate, bail.
            // Every token bucket's tryAcquire() gives us the duration we need
            // to wait until the rate is available again. If all token buckets
            // are empty, then we tell the corresponding actor to wait for the
            // minimum duration required until one of the rate limiters is available.
            //
            double minimumDurationToWait = Double.MAX_VALUE;
            for(int i = 0; i < newEndpointList.size(); i++) {
                final InetAddress ep = newEndpointList.get(i);
                double timeToNextRefill = 0L;
                if (!ep.equals(FBUtilities.getBroadcastAddress())) {
                    timeToNextRefill = MessagingService.instance().sendingRateTryAcquire(ep);
                }

                if (timeToNextRefill == 0L) {
                    dataEndpointIndex = i;
                    shouldWait = false;

                    // Other parts of the code require this endpoint-mapping
                    // to be ordered such that the data-endpoint is the first
                    // one. Let's do that now itself.
                    Collections.<InetAddress>swap(newEndpointList, dataEndpointIndex, 0);
                    break;
                }

                minimumDurationToWait = Math.min(minimumDurationToWait, timeToNextRefill);
            }

            if (shouldWait == true) {
                return minimumDurationToWait;
            }

            // We're within our expected rate. Update le endpoints.
            this.endpoints = newEndpointList.subList(0, originalSize);
            handler.endpoints = this.endpoints;
            unfiltered = newEndpointList;
        }

        // The data-request message is sent to dataPoint, the node that will actually get the data for us
        InetAddress dataPoint = handler.endpoints.get(0);
        if (dataPoint.equals(FBUtilities.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS)
        {
            logger.trace("reading data locally");
            StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
        }
        else
        {
            logger.trace("reading data from {}", dataPoint);
            MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
        }

        if (handler.endpoints.size() == 1)
            return 0L;

        // send the other endpoints a digest request
        ReadCommand digestCommand = command.copy();
        digestCommand.setDigestQuery(true);
        MessageOut<?> message = null;
        for (int i = 1; i < handler.endpoints.size(); i++)
        {
            InetAddress digestPoint = handler.endpoints.get(i);
            if (digestPoint.equals(FBUtilities.getBroadcastAddress()))
            {
                logger.trace("reading digest locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
            }
            else
            {
                logger.trace("reading digest from {}", digestPoint);
                // (We lazy-construct the digest Message object since it may not be necessary if we
                // are doing a local digest read, or no digest reads at all.)
                if (message == null)
                    message = digestCommand.createMessage();
                MessagingService.instance().sendRR(message, digestPoint, handler);
            }
        }

        return 0L;
    }

    void speculate()
    {
        // noop by default.
    }

    Row get() throws ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistency_level) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(command.ksName);
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.key);
        CFMetaData metaData = Schema.instance.getCFMetaData(command.ksName, command.cfName);

        ReadRepairDecision rrDecision = metaData.newReadRepairDecision();
         
        if (rrDecision != ReadRepairDecision.NONE) {
            ReadRepairMetrics.attempted.mark();
        }

        List<InetAddress> queryTargets = consistency_level.filterForQuery(keyspace, allReplicas, rrDecision);

        if (StorageService.instance.isClientMode())
        {
            return new DefaultReadExecutor(null, command, consistency_level, allReplicas, queryTargets);
        }

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);

        switch (metaData.getSpeculativeRetry().type)
        {
            case ALWAYS:
                return new SpeculateAlwaysExecutor(cfs, command, consistency_level, allReplicas, queryTargets);
            case PERCENTILE:
            case CUSTOM:
                return queryTargets.size() < allReplicas.size()
                       ? new SpeculativeReadExecutor(cfs, command, consistency_level, allReplicas, queryTargets)
                       : new DefaultReadExecutor(cfs, command, consistency_level, allReplicas, queryTargets);
            default:
                return new DefaultReadExecutor(cfs, command, consistency_level, allReplicas, queryTargets);
        }
    }

    private static class DefaultReadExecutor extends AbstractReadExecutor
    {
        public DefaultReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ConsistencyLevel consistency_level, List<InetAddress> allReplicas, List<InetAddress> queryTargets) throws UnavailableException
        {
            super(cfs, command, consistency_level, allReplicas, queryTargets);
        }
    }

    private static class SpeculativeReadExecutor extends AbstractReadExecutor
    {
        public SpeculativeReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ConsistencyLevel consistency_level, List<InetAddress> allReplicas, List<InetAddress> queryTargets) throws UnavailableException
        {
            super(cfs, command, consistency_level, allReplicas, queryTargets);
            assert handler.endpoints.size() < unfiltered.size();
        }

        @Override
        void speculate()
        {
            // no latency information, or we're overloaded
            if (cfs.sampleLatency > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                return;

            if (!handler.await(cfs.sampleLatency, TimeUnit.NANOSECONDS))
            {
                InetAddress endpoint = unfiltered.get(handler.endpoints.size());

                // could be waiting on the data, or on enough digests
                ReadCommand scommand = command;
                if (resolver.getData() != null)
                {
                    scommand = command.copy();
                    scommand.setDigestQuery(true);
                }

                logger.trace("Speculating read retry on {}", endpoint);
                MessagingService.instance().sendRR(scommand.createMessage(), endpoint, handler);
                cfs.metric.speculativeRetry.inc();
            }
        }
    }

    private static class SpeculateAlwaysExecutor extends AbstractReadExecutor
    {
        public SpeculateAlwaysExecutor(ColumnFamilyStore cfs, ReadCommand command, ConsistencyLevel consistency_level, List<InetAddress> allReplicas, List<InetAddress> queryTargets) throws UnavailableException
        {
            super(cfs, command, consistency_level, allReplicas, queryTargets);
        }

        @Override
        void executeAsync()
        {
            int limit = unfiltered.size() >= 2 ? 2 : 1;
            for (int i = 0; i < limit; i++)
            {
                InetAddress endpoint = unfiltered.get(i);
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.trace("reading full data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
                }
                else
                {
                    logger.trace("reading full data from {}", endpoint);
                    MessagingService.instance().sendRR(command.createMessage(), endpoint, handler);
                }
            }
            if (handler.endpoints.size() <= limit)
                return;

            ReadCommand digestCommand = command.copy();
            digestCommand.setDigestQuery(true);
            MessageOut<?> message = digestCommand.createMessage();
            for (int i = limit; i < handler.endpoints.size(); i++)
            {
                // Send the message
                InetAddress endpoint = handler.endpoints.get(i);
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.trace("reading data locally, isDigest: {}", command.isDigestQuery());
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                }
                else
                {
                    logger.trace("reading full data from {}, isDigest: {}", endpoint, command.isDigestQuery());
                    MessagingService.instance().sendRR(message, endpoint, handler);
                }
            }
        }
    }
}
