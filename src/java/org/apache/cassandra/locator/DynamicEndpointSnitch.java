/**
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

package org.apache.cassandra.locator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import org.hsqldb.Database;

/**
 * A dynamic snitch that sorts endpoints by latency with an adapted phi failure detector
 */
public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean
{
    private static final double ALPHA = 0.75; // set to 0.75 to make EDS more biased to towards the newer values
    private static final int WINDOW_SIZE = 100;

    private int UPDATE_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicUpdateInterval();
    private int RESET_INTERVAL_IN_MS = DatabaseDescriptor.getDynamicResetInterval();
    private double BADNESS_THRESHOLD = DatabaseDescriptor.getDynamicBadnessThreshold();
    private String mbeanName;
    private boolean registered = false;

    private final ConcurrentHashMap<InetAddress, Double> scores = new ConcurrentHashMap<InetAddress, Double>();
    private final ConcurrentHashMap<InetAddress, Long> lastReceived = new ConcurrentHashMap<InetAddress, Long>();
    private final ConcurrentHashMap<InetAddress, ExponentiallyDecayingSample> samples = new ConcurrentHashMap<InetAddress, ExponentiallyDecayingSample>();

    public final IEndpointSnitch subsnitch;

    public DynamicEndpointSnitch(IEndpointSnitch snitch)
    {
        this(snitch, null);
    }
    public DynamicEndpointSnitch(IEndpointSnitch snitch, String instance)
    {
        mbeanName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";
        if (instance != null)
            mbeanName += ",instance=" + instance;
        subsnitch = snitch;
        Runnable update = new Runnable()
        {
            public void run()
            {
                updateScores();
            }
        };
        Runnable reset = new Runnable()
        {
            public void run()
            {
                // we do this so that a host considered bad has a chance to recover, otherwise would we never try
                // to read from it, which would cause its score to never change
                reset();
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(update, UPDATE_INTERVAL_IN_MS, UPDATE_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        StorageService.scheduledTasks.scheduleWithFixedDelay(reset, RESET_INTERVAL_IN_MS, RESET_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
        registerMBean();
   }

    private void registerMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void unregisterMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.unregisterMBean(new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void gossiperStarting()
    {
        subsnitch.gossiperStarting();
    }

    public String getRack(InetAddress endpoint)
    {
        return subsnitch.getRack(endpoint);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return subsnitch.getDatacenter(endpoint);
    }

    public List<InetAddress> getSortedListByProximity(final InetAddress address, Collection<InetAddress> addresses)
    {
        List<InetAddress> list = new ArrayList<InetAddress>(addresses);
        sortByProximity(address, list);
        return list;
    }

    @Override
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        assert address.equals(FBUtilities.getBroadcastAddress()); // we only know about ourself
        if (BADNESS_THRESHOLD == 0 || !DatabaseDescriptor.getScoreStrategy().equals(SelectionStrategy.DEFAULT))
        {
            sortByProximityWithScore(address, addresses);
        }
        else
        {
            sortByProximityWithBadness(address, addresses);
        }
    }

    private void sortByProximityWithScore(final InetAddress address, List<InetAddress> addresses)
    {
        super.sortByProximity(address, addresses);
    }

    private void sortByProximityWithBadness(final InetAddress address, List<InetAddress> addresses)
    {
        if (addresses.size() < 2)
            return;
        subsnitch.sortByProximity(address, addresses);
        Double first = scores.get(addresses.get(0));
        if (first == null)
            return;
        for (InetAddress addr : addresses)
        {
            Double next = scores.get(addr);
            if (next == null)
                return;
            if ((first - next) / first > BADNESS_THRESHOLD)
            {
                sortByProximityWithScore(address, addresses);
                return;
            }
        }
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) {

        Double scored1 = null;
        Double scored2 = null;

        switch (DatabaseDescriptor.getScoreStrategy()){
            case ED:
                scored1 = MessagingService.instance().getScore(a1);
                scored2 = MessagingService.instance().getScore(a2);
                break;
            case BEST_QSZ:
                break;
            case POWER_OF_TWO_QSZ:
                break;
            case DEFAULT:
                scored1 = scores.get(a1);
                scored2 = scores.get(a2);
                break;
        }

        if (scored1 == null) {
            scored1 = 0.0;
            receiveTiming(a1, 0);
        }

        if (scored2 == null) {
            scored2 = 0.0;
            receiveTiming(a2, 0);
        }

        if (scored1.equals(scored2))
            return subsnitch.compareEndpoints(target, a1, a2);
        if (scored1 < scored2)
            return -1;
        else
            return 1;
    }

    public void receiveTiming(InetAddress host, long latency) // this is cheap
    {
        lastReceived.put(host, System.nanoTime());

        ExponentiallyDecayingSample sample = samples.get(host);
        if (sample == null)
        {
            ExponentiallyDecayingSample maybeNewSample = new ExponentiallyDecayingSample(WINDOW_SIZE, ALPHA);
            sample = samples.putIfAbsent(host, maybeNewSample);
            if (sample == null)
                sample = maybeNewSample;
        }
        sample.update(latency);
    }

    private void updateScores() // this is expensive
    {
        if (!StorageService.instance.isInitialized()) 
            return;
        if (!registered)
        {
            if (MessagingService.instance() != null)
            {
                MessagingService.instance().register(this);
                registered = true;
            }

        }
        double maxLatency = 1;
        long maxPenalty = 1;
        HashMap<InetAddress, Long> penalties = new HashMap<InetAddress, Long>(samples.size());
        // We're going to weight the latency and time since last reply for each host against the worst one we see, to arrive at sort of a 'badness percentage' for both of them.
        // first, find the worst for each.
        for (Map.Entry<InetAddress, ExponentiallyDecayingSample> entry : samples.entrySet())
        {
            double mean = entry.getValue().getSnapshot().getMedian();
            if (mean > maxLatency)
                maxLatency = mean;
            long timePenalty = lastReceived.containsKey(entry.getKey()) ? lastReceived.get(entry.getKey()) : System.nanoTime();
            timePenalty = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - timePenalty);
            timePenalty = timePenalty > UPDATE_INTERVAL_IN_MS ? UPDATE_INTERVAL_IN_MS : timePenalty;
            // a convenient place to remember this since we've already calculated it and need it later
            penalties.put(entry.getKey(), timePenalty);
            if (timePenalty > maxPenalty)
                maxPenalty = timePenalty;
        }
        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddress, ExponentiallyDecayingSample> entry: samples.entrySet())
        {
            double score = entry.getValue().getSnapshot().getMedian() / maxLatency;
            if (penalties.containsKey(entry.getKey()))
                score += penalties.get(entry.getKey()) / ((double) maxPenalty);
            else
                // there's a chance a host was added to the samples after our previous loop to get the time penalties.  Add 1.0 to it, or '100% bad' for the time penalty.
                score += 1; // maxPenalty / maxPenalty
            // finally, add the severity without any weighting, since hosts scale this relative to their own load and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            score += StorageService.instance.getSeverity(entry.getKey());
            // lowest score (least amount of badness) wins.
            scores.put(entry.getKey(), score);            
        }
    }


    private void reset()
    {
        for (ExponentiallyDecayingSample sample : samples.values())
            sample.clear();
    }

    public Map<InetAddress, Double> getScores()
    {
        return scores;
    }

    public int getUpdateInterval()
    {
        return UPDATE_INTERVAL_IN_MS;
    }
    public int getResetInterval()
    {
        return RESET_INTERVAL_IN_MS;
    }
    public double getBadnessThreshold()
    {
        return BADNESS_THRESHOLD;
    }
    public String getSubsnitchClassName()
    {
        return subsnitch.getClass().getName();
    }

    public List<Double> dumpTimings(String hostname) throws UnknownHostException
    {
        InetAddress host = InetAddress.getByName(hostname);
        ArrayList<Double> timings = new ArrayList<Double>();
        ExponentiallyDecayingSample sample = samples.get(host);
        if (sample != null)
        {
            for (double time: sample.getSnapshot().getValues())
                timings.add(time);
        }
        return timings;
    }

    public void setSeverity(double severity)
    {
        StorageService.instance.reportSeverity(severity);
    }

    public double getSeverity()
    {
        return StorageService.instance.getSeverity(FBUtilities.getBroadcastAddress());
    }

    public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
    {
        if (!subsnitch.isWorthMergingForRangeQuery(merged, l1, l2))
            return false;

        // Make sure we return the subsnitch decision (i.e true if we're here) if we lack too much scores
        double maxMerged = maxScore(merged);
        double maxL1 = maxScore(l1);
        double maxL2 = maxScore(l2);
        if (maxMerged < 0 || maxL1 < 0 || maxL2 < 0)
            return true;

        return maxMerged < maxL1 + maxL2;
    }

    // Return the max score for the endpoint in the provided list, or -1.0 if no node have a score.
    private double maxScore(List<InetAddress> endpoints)
    {
        double maxScore = -1.0;
        for (InetAddress endpoint : endpoints)
        {
            Double score = scores.get(endpoint);
            if (score == null)
                continue;

            if (score > maxScore)
                maxScore = score;
        }
        return maxScore;
    }
}
