package org.apache.cassandra.c3;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;

public class HostTracker
{

    private static final Logger logger = LoggerFactory.getLogger(HostTracker.class);
    private final Config config = ConfigFactory.parseString("dispatcher {\n" +
                                                            "  type = Dispatcher\n" +
                                                            "  executor = \"fork-join-executor\"\n" +
                                                            "  fork-join-executor {\n" +
                                                            "    parallelism-min = 2\n" +
                                                            "    parallelism-factor = 2.0\n" +
                                                            "    parallelism-max = 20\n" +
                                                            "  }\n" +
                                                            "  throughput = 10\n" +
                                                            "}\n");

    private final ConcurrentHashMap<InetAddress, ActorRef> actors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, AtomicInteger> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, RateController> rateControllers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InetAddress, ScoreTracker> scoreTrackers = new ConcurrentHashMap<>();
    private final ActorSystem actorSystem = ActorSystem.create("C3", config);

    public ActorRef getActor(List<InetAddress> endpoints)
    {
        final InetAddress endpoint = endpoints.get(0);
        ActorRef actor = actors.get(endpoint);
        if (actor == null)
        {
            synchronized (this)
            {
                if (!actors.containsKey(endpoint))
                {
                    actor = actorSystem.actorOf(Props.create(ReplicaGroupActor.class).withDispatcher("dispatcher"), endpoint.getHostName());
                    actors.putIfAbsent(endpoint, actor);
                    logger.info("Creating actor for: " + endpoints);
                }
            }
            return actors.get(endpoint);
        }
        return actor;
    }

    public RateController getRateController(InetAddress endpoint) {
        return rateControllers.get(endpoint);
    }

    public ScoreTracker getScoreTracker(InetAddress endpoint) {
        if (!scoreTrackers.containsKey(endpoint)) {
            scoreTrackers.putIfAbsent(endpoint, new ScoreTracker());
        }
        return scoreTrackers.get(endpoint);
    }

    public boolean containsKey(InetAddress key) {
        return pendingRequests.containsKey(key);
    }

    public AtomicInteger put(InetAddress key, AtomicInteger value) {
        return pendingRequests.put(key, value);
    }

    public AtomicInteger get(InetAddress key) {
        return pendingRequests.get(key);
    }

    public double sendingRateTryAcquire(InetAddress endpoint)
    {
        RateController rateController = rateControllers.get(endpoint);

        if (rateController == null)
        {
            rateControllers.putIfAbsent(endpoint, new RateController());
            rateController = rateControllers.get(endpoint);
        }

        assert (rateController != null);
        return rateController.tryAcquire();
    }

    public void receiveRateTick(InetAddress endpoint)
    {
        RateController rateController = rateControllers.get(endpoint);

        if (rateController == null)
        {
            rateControllers.putIfAbsent(endpoint, new RateController());
            rateController = rateControllers.get(endpoint);
        }

        assert (rateController != null);
        rateController.receiveRateTrackerTick();
    }

    public double getScore(InetAddress endpoint) {
        RateController rateController = rateControllers.get(endpoint);
        ScoreTracker scoreTracker = scoreTrackers.get(endpoint);

        if (rateController == null) {
            rateControllers.putIfAbsent(endpoint, new RateController());
            rateController = rateControllers.get(endpoint);
        }

        if (scoreTracker == null) {
            scoreTrackers.putIfAbsent(endpoint, new ScoreTracker());
            scoreTracker = scoreTrackers.get(endpoint);
        }

        assert(rateController != null);
        assert(scoreTracker != null);

        return scoreTracker.getScore(pendingRequests, endpoint);
    }

    public AtomicInteger getPendingRequestsCounter(final InetAddress endpoint)
    {
        AtomicInteger counter = pendingRequests.get(endpoint);
        if (counter == null)
        {
            pendingRequests.put(endpoint, new AtomicInteger(0));
            counter = pendingRequests.get(endpoint);
        }

        return counter;
    }

    public void updateMetrics(MessageIn message, long latency) {
        receiveRateTick(message.from);
        final RateController rateController = getRateController(message.from);
        assert (rateController != null);
        rateController.updateCubicSendingRate();
        int count = pendingRequests.get(message.from).decrementAndGet();
        logger.trace("Decrementing pendingJob count Endpoint: {}, Count: {} ", message.from, count);

        int queueSize = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.QSZ)).getInt();
        double serviceTimeInMillis = ByteBuffer.wrap((byte[]) message.parameters.get(C3Metrics.MU)).getLong() / 1000000.0;
        double latencyInMillis = latency / 1000000.0;

        assert serviceTimeInMillis < latencyInMillis;
        ScoreTracker scoreTracker = getScoreTracker(message.from);
        scoreTracker.updateNodeScore(queueSize, serviceTimeInMillis, latencyInMillis);
    }

    // Required for handling coordinator local reads correctly
    public void updateMetricsLocal(int queueSize, long serviceTime) {
        final InetAddress from = FBUtilities.getBroadcastAddress();
        logger.trace("Local pendingJob count Endpoint: {}, Count: {} ", from, queueSize);

        double serviceTimeInMillis = serviceTime / 1000000.0;
        double latencyInMillis = serviceTimeInMillis;

        ScoreTracker scoreTracker = getScoreTracker(from);
        scoreTracker.updateNodeScore(queueSize, serviceTimeInMillis, latencyInMillis);
    }

}
