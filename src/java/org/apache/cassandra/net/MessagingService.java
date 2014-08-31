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
package org.apache.cassandra.net;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.io.Inet;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.cassandra.metrics.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import com.typesafe.config.ConfigFactory;

public final class MessagingService implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // 8 bits version, so don't waste versions
    public static final int VERSION_12  = 6;
    public static final int VERSION_20  = 7;
    public static final int current_version = VERSION_20;

    /**
     * we preface every message with this number so the recipient can validate the sender is sane
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    /* All verb handler identifiers */
    public enum Verb
    {
        MUTATION,
        @Deprecated BINARY,
        READ_REPAIR,
        READ,
        REQUEST_RESPONSE, // client-initiated reads and writes
        @Deprecated STREAM_INITIATE,
        @Deprecated STREAM_INITIATE_DONE,
        @Deprecated STREAM_REPLY,
        @Deprecated STREAM_REQUEST,
        RANGE_SLICE,
        @Deprecated BOOTSTRAP_TOKEN,
        @Deprecated TREE_REQUEST,
        @Deprecated TREE_RESPONSE,
        @Deprecated JOIN,
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        @Deprecated DEFINITIONS_ANNOUNCE,
        DEFINITIONS_UPDATE,
        TRUNCATE,
        SCHEMA_CHECK,
        @Deprecated INDEX_SCAN,
        REPLICATION_FINISHED,
        INTERNAL_RESPONSE, // responses to internal calls
        COUNTER_MUTATION,
        @Deprecated STREAMING_REPAIR_REQUEST,
        @Deprecated STREAMING_REPAIR_RESPONSE,
        SNAPSHOT, // Similar to nt snapshot
        MIGRATION_REQUEST,
        GOSSIP_SHUTDOWN,
        _TRACE, // dummy verb so we can use MS.droppedMessages
        ECHO,
        REPAIR_MESSAGE,
        // use as padding for backwards compatability where a previous version needs to validate a verb from the future.
        PAXOS_PREPARE,
        PAXOS_PROPOSE,
        PAXOS_COMMIT,
        PAGED_RANGE,
        // remember to add new verbs at the end, since we serialize by ordinal
        UNUSED_1,
        UNUSED_2,
        UNUSED_3,
        ;
    }

    public static final EnumMap<MessagingService.Verb, Stage> verbStages = new EnumMap<MessagingService.Verb, Stage>(MessagingService.Verb.class)
    {{
        put(Verb.MUTATION, Stage.MUTATION);
        put(Verb.READ_REPAIR, Stage.MUTATION);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.COUNTER_MUTATION, Stage.MUTATION);
        put(Verb.PAXOS_PREPARE, Stage.MUTATION);
        put(Verb.PAXOS_PROPOSE, Stage.MUTATION);
        put(Verb.PAXOS_COMMIT, Stage.MUTATION);

        put(Verb.READ, Stage.READ);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.PAGED_RANGE, Stage.READ);

        put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);

        put(Verb.STREAM_REPLY, Stage.MISC); // actually handled by FileStreamTask and streamExecutors
        put(Verb.STREAM_REQUEST, Stage.MISC);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.SNAPSHOT, Stage.MISC);

        put(Verb.TREE_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.REPAIR_MESSAGE, Stage.ANTI_ENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.GOSSIP_SHUTDOWN, Stage.GOSSIP);

        put(Verb.DEFINITIONS_UPDATE, Stage.MIGRATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.MIGRATION_REQUEST, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);
        put(Verb.COUNTER_MUTATION, Stage.MUTATION);
        put(Verb.SNAPSHOT, Stage.MISC);
        put(Verb.ECHO, Stage.GOSSIP);

        put(Verb.UNUSED_1, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_2, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_3, Stage.INTERNAL_RESPONSE);
    }};

    /**
     * Messages we receive in IncomingTcpConnection have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RESPONSE verb, which just means "a reply to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See CASSANDRA-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> verbSerializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.REQUEST_RESPONSE, CallbackDeterminedSerializer.instance);
        put(Verb.INTERNAL_RESPONSE, CallbackDeterminedSerializer.instance);

        put(Verb.MUTATION, RowMutation.serializer);
        put(Verb.READ_REPAIR, RowMutation.serializer);
        put(Verb.READ, ReadCommand.serializer);
        put(Verb.RANGE_SLICE, RangeSliceCommand.serializer);
        put(Verb.PAGED_RANGE, RangeSliceCommand.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.REPAIR_MESSAGE, RepairMessage.serializer);
        put(Verb.GOSSIP_DIGEST_ACK, GossipDigestAck.serializer);
        put(Verb.GOSSIP_DIGEST_ACK2, GossipDigestAck2.serializer);
        put(Verb.GOSSIP_DIGEST_SYN, GossipDigestSyn.serializer);
        put(Verb.DEFINITIONS_UPDATE, MigrationManager.MigrationsSerializer.instance);
        put(Verb.TRUNCATE, Truncation.serializer);
        put(Verb.REPLICATION_FINISHED, null);
        put(Verb.COUNTER_MUTATION, CounterMutation.serializer);
        put(Verb.SNAPSHOT, SnapshotCommand.serializer);
        put(Verb.ECHO, EchoMessage.serializer);
        put(Verb.PAXOS_PREPARE, Commit.serializer);
        put(Verb.PAXOS_PROPOSE, Commit.serializer);
        put(Verb.PAXOS_COMMIT, Commit.serializer);
    }};

    /**
     * A Map of what kind of serializer to wire up to a REQUEST_RESPONSE callback, based on outbound Verb.
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> callbackDeserializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.MUTATION, WriteResponse.serializer);
        put(Verb.READ_REPAIR, WriteResponse.serializer);
        put(Verb.COUNTER_MUTATION, WriteResponse.serializer);
        put(Verb.RANGE_SLICE, RangeSliceReply.serializer);
        put(Verb.PAGED_RANGE, RangeSliceReply.serializer);
        put(Verb.READ, ReadResponse.serializer);
        put(Verb.TRUNCATE, TruncateResponse.serializer);
        put(Verb.SNAPSHOT, null);

        put(Verb.MIGRATION_REQUEST, MigrationManager.MigrationsSerializer.instance);
        put(Verb.SCHEMA_CHECK, UUIDSerializer.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.REPLICATION_FINISHED, null);

        put(Verb.PAXOS_PREPARE, PrepareResponse.serializer);
        put(Verb.PAXOS_PROPOSE, BooleanSerializer.serializer);
    }};

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo> callbacks;

    /**
     * a placeholder class that means "deserialize using the callback." We can't implement this without
     * special-case code in InboundTcpConnection because there is no way to pass the message id to IVersionedSerializer.
     */
    static class CallbackDeterminedSerializer implements IVersionedSerializer<Object>
    {
        public static final CallbackDeterminedSerializer instance = new CallbackDeterminedSerializer();

        public Object deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public void serialize(Object o, DataOutput out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(Object o, int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<Verb, IVerbHandler> verbHandlers;

    /**
     * One executor per destination InetAddress for streaming.
     * <p/>
     * See CASSANDRA-3494 for the background. We have streaming in place so we do not want to limit ourselves to
     * one stream at a time for throttling reasons. But, we also do not want to just arbitrarily stream an unlimited
     * amount of files at once because a single destination might have hundreds of files pending and it would cause a
     * seek storm. So, transfer exactly one file per destination host. That puts a very natural rate limit on it, in
     * addition to mapping well to the expected behavior in many cases.
     * <p/>
     * We will create our stream executors with a core size of 0 so that they time out and do not consume threads. This
     * means the overhead in the degenerate case of having streamed to everyone in the ring over time as a ring changes,
     * is not going to be a thread per node - but rather an instance per node. That's totally fine.
     */
    private final ConcurrentMap<InetAddress, DebuggableThreadPoolExecutor> streamExecutors = new NonBlockingHashMap<InetAddress, DebuggableThreadPoolExecutor>();

    private final NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool> connectionManagers = new NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool>();

    /* Machete actor stuff goes here */
    private final Config config = ConfigFactory.parseString("my-dispatcher {\n" +
            "  type = Dispatcher\n" +
            "  executor = \"fork-join-executor\"\n" +
            "  fork-join-executor {\n" +
            "    parallelism-min = 2\n" +
            "    parallelism-factor = 2.0\n" +
            "    parallelism-max = 20\n" +
            "  }\n" +
            "  throughput = 10\n" +
            "}\n");

    private final ActorSystem actorSystem = ActorSystem.create("Machete", config);
    private final ConcurrentHashMap<InetAddress, ActorRef> actorMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, CopyOnWriteArrayList<String>> rgInvertedInex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, AtomicInteger> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, SendReceiveRateContainer> sendReceiveRateContainers
            = new ConcurrentHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private final List<SocketThread> socketThreads = Lists.newArrayList();
    private final SimpleCondition listenGate;

    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    public static final EnumSet<Verb> DROPPABLE_VERBS = EnumSet.of(Verb.BINARY,
                                                                   Verb._TRACE,
                                                                   Verb.MUTATION,
                                                                   Verb.READ_REPAIR,
                                                                   Verb.READ,
                                                                   Verb.RANGE_SLICE,
                                                                   Verb.PAGED_RANGE,
                                                                   Verb.REQUEST_RESPONSE);

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedMessageMetrics> droppedMessages = new EnumMap<Verb, DroppedMessageMetrics>(Verb.class);
    // dropped count when last requested for the Recent api.  high concurrency isn't necessary here.
    private final Map<Verb, Integer> lastDroppedInternal = new EnumMap<Verb, Integer>(Verb.class);

    private final List<ILatencySubscriber> subscribers = new ArrayList<ILatencySubscriber>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, Integer> versions = new NonBlockingHashMap<InetAddress, Integer>();

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService();
    }
    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    private MessagingService()
    {
        for (Verb verb : DROPPABLE_VERBS)
        {
            droppedMessages.put(verb, new DroppedMessageMetrics(verb));
            lastDroppedInternal.put(verb, 0);
        }
        listenGate = new SimpleCondition();
        verbHandlers = new EnumMap<Verb, IVerbHandler>(Verb.class);
        Runnable logDropped = new Runnable()
        {
            public void run()
            {
                logDroppedMessages();
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(logDropped, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, ?> timeoutReporter = new Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, Object>()
        {
            public Object apply(Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>> pair)
            {
                CallbackInfo expiredCallbackInfo = pair.right.value;
                maybeAddLatency(expiredCallbackInfo.callback, expiredCallbackInfo.target, pair.right.timeout);
                ConnectionMetrics.totalTimeouts.mark();
                getConnectionPool(expiredCallbackInfo.target).incrementTimeout();

                if (expiredCallbackInfo != null
                        && (callbackDeserializers.get(Verb.READ).equals(expiredCallbackInfo.serializer))) {
                    int count = pendingRequests.get(expiredCallbackInfo.target).decrementAndGet();
                    logger.trace("Decrementing pendingJob count Endpoint: {}, Count: {} ", expiredCallbackInfo.target, count);
                }


                if (expiredCallbackInfo.shouldHint())
                {
                    assert expiredCallbackInfo.sentMessage != null;
                    RowMutation rm = (RowMutation) expiredCallbackInfo.sentMessage.payload;
                    return StorageProxy.submitHint(rm, expiredCallbackInfo.target, null, null);
                }

                return null;
            }
        };

        callbacks = new ExpiringMap<Integer, CallbackInfo>(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Track latency information for the dynamic snitch
     *
     * @param cb      the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAddLatency(IAsyncCallback cb, InetAddress address, long latency)
    {
        if (cb.isLatencyForSnitch())
            addLatency(address, latency);
    }

    public void addLatency(InetAddress address, long latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public void convict(InetAddress ep)
    {
        logger.debug("Resetting pool for " + ep);
        getConnectionPool(ep).reset();
    }

    /**
     * Listen on the specified port.
     *
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws ConfigurationException
    {
        callbacks.reset(); // hack to allow tests to stop/restart MS
        for (ServerSocket ss : getServerSocket(localEp))
        {
            SocketThread th = new SocketThread(ss, "ACCEPT-" + localEp);
            th.start();
            socketThreads.add(th);
        }
        listenGate.signalAll();
    }

    private List<ServerSocket> getServerSocket(InetAddress localEp) throws ConfigurationException
    {
        final List<ServerSocket> ss = new ArrayList<ServerSocket>(2);
        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.none)
        {
            try
            {
                ss.add(SSLFactory.getServerSocket(DatabaseDescriptor.getServerEncryptionOptions(), localEp, DatabaseDescriptor.getSSLStoragePort()));
            }
            catch (IOException e)
            {
                throw new ConfigurationException("Unable to create ssl socket", e);
            }
            // setReuseAddress happens in the factory.
            logger.info("Starting Encrypted Messaging Service on SSL port {}", DatabaseDescriptor.getSSLStoragePort());
        }

        ServerSocketChannel serverChannel;
        try
        {
            serverChannel = ServerSocketChannel.open();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        ServerSocket socket = serverChannel.socket();
        try
        {
            socket.setReuseAddress(true);
        }
        catch (SocketException e)
        {
            throw new ConfigurationException("Insufficient permissions to setReuseAddress", e);
        }
        InetSocketAddress address = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());
        try
        {
            socket.bind(address);
        }
        catch (BindException e)
        {
            if (e.getMessage().contains("in use"))
                throw new ConfigurationException(address + " is in use by another process.  Change listen_address:storage_port in cassandra.yaml to values that do not conflict with other services");
            else if (e.getMessage().contains("Cannot assign requested address"))
                throw new ConfigurationException("Unable to bind to address " + address
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            else
                throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        logger.info("Starting Messaging Service on port {}", DatabaseDescriptor.getStoragePort());
        ss.add(socket);
        return ss;
    }

    public void waitUntilListening()
    {
        try
        {
            listenGate.await();
        }
        catch (InterruptedException ie)
        {
            logger.debug("await interrupted");
        }
    }

    public void destroyConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers.get(to);
        if (cp == null)
            return;
        cp.close();
        connectionManagers.remove(to);
    }

    public OutboundTcpConnectionPool getConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers.get(to);
        if (cp == null)
        {
            connectionManagers.putIfAbsent(to, new OutboundTcpConnectionPool(to));
            cp = connectionManagers.get(to);
        }
        return cp;
    }

    public OutboundTcpConnection getConnection(InetAddress to, MessageOut msg)
    {
        return getConnectionPool(to).getConnection(msg);
    }

    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     *
     * @param verb
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(Verb verb, IVerbHandler verbHandler)
    {
        assert !verbHandlers.containsKey(verb);
        verbHandlers.put(verb, verbHandler);
    }

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     *
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(Verb type)
    {
        return verbHandlers.get(type);
    }

    public int addCallback(IAsyncCallback cb, MessageOut message, InetAddress to, long timeout)
    {
        int messageId = nextId();
        CallbackInfo previous;

        // If HH is enabled and this is a mutation message => store the message to track for potential hints.
        if (DatabaseDescriptor.hintedHandoffEnabled() && message.verb == Verb.MUTATION)
            previous = callbacks.put(messageId, new CallbackInfo(to, cb, message, callbackDeserializers.get(message.verb)), timeout);
        else
            previous = callbacks.put(messageId, new CallbackInfo(to, cb, callbackDeserializers.get(message.verb)), timeout);

        assert previous == null;
        return messageId;
    }

    private static final AtomicInteger idGen = new AtomicInteger(0);

    private static int nextId()
    {
        return idGen.incrementAndGet();
    }

    /*
     * @see #sendRR(Message message, InetAddress to, IAsyncCallback cb, long timeout)
     */
    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return sendRR(message, to, cb, message.getTimeout());
    }

    /**
     * Send a message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * Also holds the message (only mutation messages) to determine if it
     * needs to trigger a hint (uses StorageProxy for that).
     *
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param cb      callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     *                suggest that a timeout occurred to the invoker of the send().
     * @param timeout the timeout used for expiration
     * @return an reference to message id used to match with the result
     */
    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb, long timeout)
    {
        int id = addCallback(cb, message, to, timeout);

        if(message.verb.equals(Verb.READ)) {
            if(!pendingRequests.containsKey(to)) {
                pendingRequests.put(to, new AtomicInteger(0));
            }
            int val = pendingRequests.get(to).incrementAndGet();
        }

        sendOneWay(message, id, to);
        return id;
    }

    public void sendOneWay(MessageOut message, InetAddress to)
    {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(MessageOut message, int id, InetAddress to)
    {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void sendOneWay(MessageOut message, int id, InetAddress to)
    {
        if (logger.isTraceEnabled())
            logger.trace(FBUtilities.getBroadcastAddress() + " sending " + message.verb + " to " + id + "@" + to);

        if (to.equals(FBUtilities.getBroadcastAddress()))
            logger.trace("Message-to-self {} going over MessagingService", message);

        // message sinks are a testing hook
        MessageOut processedMessage = SinkManager.processOutboundMessage(message, id, to);
        if (processedMessage == null)
        {
            return;
        }

        // get pooled connection (really, connection queue)
        OutboundTcpConnection connection = getConnection(to, processedMessage);

        // write it
        connection.enqueue(processedMessage, id);
    }

    public <T> AsyncOneResponse<T> sendRR(MessageOut message, InetAddress to)
    {
        AsyncOneResponse<T> iar = new AsyncOneResponse<T>();
        sendRR(message, to, iar);
        return iar;
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.reset();
    }

    public void waitForStreaming() throws InterruptedException
    {
        // this does not prevent new streams from beginning after a drain begins, but since streams are only
        // started in response to explicit operator action (bootstrap/move/repair/etc) that feels like a feature.
        for (DebuggableThreadPoolExecutor e : streamExecutors.values())
            e.shutdown();

        for (DebuggableThreadPoolExecutor e : streamExecutors.values())
        {
            if (!e.awaitTermination(24, TimeUnit.HOURS))
                logger.error("Stream took more than 24H to complete; skipping");
        }
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(Stage.MUTATION).isShutdown();

        // the important part
        callbacks.shutdownBlocking();

        // attempt to humor tests that try to stop and restart MS
        try
        {
            for (SocketThread th : socketThreads)
                th.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void receive(MessageIn message, int id, long timestamp)
    {
        TraceState state = Tracing.instance.initializeFromMessage(message);
        if (state != null)
            state.trace("Message received from {}", message.from);

        message = SinkManager.processInboundMessage(message, id);
        if (message == null)
            return;

        Runnable runnable = new MessageDeliveryTask(message, id, timestamp);
        TracingAwareExecutorService stage = StageManager.getStage(message.getMessageType());
        assert stage != null : "No stage for message type " + message.verb;

        stage.execute(runnable, state);
    }

    public void setCallbackForTests(int messageId, CallbackInfo callback)
    {
        callbacks.put(messageId, callback);
    }

    public CallbackInfo getRegisteredCallback(int messageId)
    {
        return callbacks.get(messageId);
    }

    public CallbackInfo removeRegisteredCallback(int messageId, InetAddress endpoint)
    {
        CallbackInfo ret = callbacks.remove(messageId);

        if (ret != null && (callbackDeserializers.get(Verb.READ).equals(ret.serializer))) {
            receiveRateTick(endpoint);
            updateRates(endpoint);
            int count = pendingRequests.get(endpoint).decrementAndGet();
            logger.trace("Decrementing pendingJob count Endpoint: {}, Count: {} ", endpoint, count);
        }

        return ret;
    }

    /**
     * @return System.nanoTime() when callback was created.
     */
    public long getRegisteredCallbackAge(int messageId)
    {
        return callbacks.getAge(messageId);
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    public static int getBits(int packed, int start, int count)
    {
        return packed >>> (start + 1) - count & ~(-1 << count);
    }

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public int setVersion(InetAddress address, int version)
    {
        logger.debug("Setting version {} for {}", version, address);
        Integer v = versions.put(address, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.debug("Reseting version for {}", endpoint);
        versions.remove(endpoint);
    }

    public Integer getVersion(InetAddress address)
    {
        Integer v = versions.get(address);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            logger.trace("Assuming current protocol version for {}", address);
            return MessagingService.current_version;
        }
        else
            return v;
    }

    public int getVersion(String address) throws UnknownHostException
    {
        return getVersion(InetAddress.getByName(address));
    }

    public boolean knowsVersion(InetAddress endpoint)
    {
        return versions.get(endpoint) != null;
    }

    public void incrementDroppedMessages(Verb verb)
    {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        droppedMessages.get(verb).dropped.mark();
    }

    private void logDroppedMessages()
    {
        boolean logTpstats = false;
        for (Map.Entry<Verb, DroppedMessageMetrics> entry : droppedMessages.entrySet())
        {
            int dropped = (int) entry.getValue().dropped.count();
            Verb verb = entry.getKey();
            int recent = dropped - lastDroppedInternal.get(verb);
            if (recent > 0)
            {
                logTpstats = true;
                logger.info("{} {} messages dropped in last {}ms",
                             new Object[] {recent, verb, LOG_DROPPED_INTERVAL_IN_MS});
                lastDroppedInternal.put(verb, dropped);
            }
        }

        if (logTpstats)
            StatusLogger.log();
    }

    private static class SocketThread extends Thread
    {
        private final ServerSocket server;

        SocketThread(ServerSocket server, String name)
        {
            super(name);
            this.server = server;
        }

        public void run()
        {
            while (true)
            {
                try
                {
                    Socket socket = server.accept();
                    if (authenticate(socket))
                    {
                        // determine the connection type to decide whether to buffer
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        MessagingService.validateMagic(in.readInt());
                        int header = in.readInt();
                        boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
                        int version = MessagingService.getBits(header, 15, 8);
                        logger.debug("Connection version {} from {}", version, socket.getInetAddress());

                        if (isStream)
                        {
                            new IncomingStreamingConnection(version, socket).start();
                        }
                        else
                        {
                            boolean compressed = MessagingService.getBits(header, 2, 1) == 1;
                            new IncomingTcpConnection(version, compressed, socket).start();
                        }
                    }
                    else
                    {
                        socket.close();
                    }
                }
                catch (AsynchronousCloseException e)
                {
                    // this happens when another thread calls close().
                    logger.info("MessagingService shutting down server thread.");
                    break;
                }
                catch (ClosedChannelException e)
                {
                    logger.debug("MessagingService server thread already closed.");
                    break;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        void close() throws IOException
        {
            server.close();
        }

        private boolean authenticate(Socket socket)
        {
            return DatabaseDescriptor.getInternodeAuthenticator().authenticate(socket.getInetAddress(), socket.getPort());
        }
    }

    public Map<String, Integer> getCommandPendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getPendingMessages());
        return pendingTasks;
    }

    public int getCommandPendingTasks(InetAddress address)
    {
        OutboundTcpConnectionPool connection = connectionManagers.get(address);
        return connection == null ? 0 : connection.cmdCon.getPendingMessages();
    }

    public Map<String, Long> getCommandCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Long> getCommandDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getResponsePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getResponseCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<Verb, DroppedMessageMetrics> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().dropped.count());
        return map;
    }

    public Map<String, Integer> getRecentlyDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<Verb, DroppedMessageMetrics> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), entry.getValue().getRecentlyDropped());
        return map;
    }

    public long getTotalTimeouts()
    {
        return ConnectionMetrics.totalTimeouts.count();
    }

    public long getRecentTotalTimouts()
    {
        return ConnectionMetrics.getRecentTotalTimeout();
    }

    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry: connectionManagers.entrySet())
        {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }

    public Map<String, Long> getRecentTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry: connectionManagers.entrySet())
        {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getRecentTimeouts();
            result.put(ip, recent);
        }
        return result;
    }

    /* Machete methods */
    public ActorRef getReplicaGroupActor(List<InetAddress> endpoints) {
        final InetAddress rgOwner = endpoints.get(0);
        ActorRef rgActor = actorMap.get(rgOwner);
        if (rgActor == null) {
            synchronized (this) {
                if (!actorMap.containsKey(rgOwner)) {
                    rgActor = actorSystem.actorOf(Props.create(ReplicaGroupActor.class).withDispatcher("my-dispatcher"), rgOwner.getHostName());
                    final ActorRef result = actorMap.putIfAbsent(rgOwner, rgActor);
                    if (result == null) {
                        /* For every endpoint, we add the replica group actor's name to the list
                         */
                        for (InetAddress e : endpoints) {
                            rgInvertedInex.putIfAbsent(e, new CopyOnWriteArrayList<String>());
                            rgInvertedInex.get(e).add(rgOwner.getHostName());
                        }
                    }
                }
            }
            return actorMap.get(rgOwner);
        }
        return rgActor;
    }

    public double sendingRateTryAcquire(InetAddress endpoint) {
        SendReceiveRateContainer rateContainer = sendReceiveRateContainers.get(endpoint);

        if (rateContainer == null) {
            sendReceiveRateContainers.putIfAbsent(endpoint, new SendReceiveRateContainer(endpoint));
            rateContainer = sendReceiveRateContainers.get(endpoint);
        }

        assert(rateContainer != null);
        return rateContainer.tryAcquire();
    }

    public void receiveRateTick(InetAddress endpoint) {
        SendReceiveRateContainer rateContainer = sendReceiveRateContainers.get(endpoint);

        if (rateContainer == null) {
            sendReceiveRateContainers.putIfAbsent(endpoint, new SendReceiveRateContainer(endpoint));
            rateContainer = sendReceiveRateContainers.get(endpoint);
        }

        assert(rateContainer != null);
        rateContainer.receiveRateTrackerTick();
    }

    public void updateRates(final InetAddress endpoint) {
        final SendReceiveRateContainer rateContainer = sendReceiveRateContainers.get(endpoint);

        assert(rateContainer != null);

        rateContainer.updateCubicSendingRate();
    }

    public AtomicInteger getPendingRequestsCounter(final InetAddress endpoint) {
        AtomicInteger counter = MessagingService.instance().pendingRequests.get(FBUtilities.getBroadcastAddress());
        if(counter == null) {
            MessagingService.instance().pendingRequests.put(FBUtilities.getBroadcastAddress(), new AtomicInteger(0));
            counter = MessagingService.instance().pendingRequests.get(FBUtilities.getBroadcastAddress());
        }

        return counter;
    }

    private class SendReceiveRateContainer {
        private static final long RECEIVE_RATE_INTERVAL_MS = 20;
        private static final long RECEIVE_RATE_INITIAL = 100;

        // Constants for cubic function
        private static final double CUBIC_BETA = 0.2;
        private static final double CUBIC_C = 0.000004;
        private static final double CUBIC_SMAX = 10;
        private static final double CUBIC_HYSTERISIS_FACTOR = 4;
        private static final double CUBIC_BETA_BY_C = CUBIC_BETA / CUBIC_C;
        private static final double CUBIC_HYSTERISIS_DURATION = RECEIVE_RATE_INTERVAL_MS * CUBIC_HYSTERISIS_FACTOR;

        private final InetAddress endpoint;
        private final SimpleRateLimiter sendingRateLimiter;
        private final SlottedRateTracker receiveRateTracker;

        private long timeOfLastRateDecrease = 0L;
        private long timeOfLastRateIncrease = 0L;
        private double Rmax = 0;



        SendReceiveRateContainer(InetAddress endpoint) {
            this.endpoint = endpoint;
            this.sendingRateLimiter = new SimpleRateLimiter(1, RECEIVE_RATE_INTERVAL_MS, 50);
            this.receiveRateTracker = new SlottedRateTracker(RECEIVE_RATE_INITIAL, RECEIVE_RATE_INTERVAL_MS);
        }

        public double tryAcquire() {
            return sendingRateLimiter.tryAcquire();
        }

        public void receiveRateTrackerTick() {
            receiveRateTracker.add(1);
        }

        public synchronized void updateCubicSendingRate() {
            final double currentReceiveRate = receiveRateTracker.getCurrentRate();
            final double currentSendingRate = sendingRateLimiter.getRate();
            final long now = System.currentTimeMillis();

            if (currentSendingRate > currentReceiveRate
                    && (now - timeOfLastRateIncrease > CUBIC_HYSTERISIS_DURATION)) {
                Rmax = currentSendingRate;
                sendingRateLimiter.setRate(Math.max(currentSendingRate * CUBIC_BETA, 0.001));
                timeOfLastRateDecrease = now;
            }
            else if (currentSendingRate < currentReceiveRate) {
                final double T = System.currentTimeMillis() - timeOfLastRateDecrease;
                timeOfLastRateIncrease = now;
                final double scalingFactor = Math.cbrt(Rmax * CUBIC_BETA_BY_C);
                final double newSendingRate = CUBIC_C * Math.pow(T - scalingFactor, 3) + Rmax;

                if (newSendingRate - currentSendingRate > CUBIC_SMAX) {
                    sendingRateLimiter.setRate(currentSendingRate + CUBIC_SMAX);
                } else {
                    sendingRateLimiter.setRate(newSendingRate);
                }

                assert(newSendingRate > 0);
            }

//            System.out.println(System.currentTimeMillis() + " " + endpoint +
//                        " OldRate: " + currentSendingRate +
//                        " NewRate: " + sendingRateLimiter.getRate() +
//                        " ReceiveRate: " + currentReceiveRate);
        }
    }

}
