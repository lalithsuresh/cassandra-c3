package org.apache.cassandra.metrics;

import org.apache.cassandra.service.StorageService;

import java.util.concurrent.TimeUnit;

/**
 * A rate tracker .
 */
public class SlottedRateTracker {

    private double currentRate;
    private long interval;
    private long lastTick = 0;
    private long eventCount = 0;
    private double lastRate = 0.0;

    /**
     *
     * @param initialRate Initial setting for the rate parameter
     * @param interval Interval in milliseconds over which rate is calculated
     */
    public SlottedRateTracker(double initialRate, long interval) {
        this.currentRate = initialRate;
        this.interval = interval;
//        Runnable tick = new Runnable() {
//            public void run() {
//                add(0);
//            }
//        };
//
//        StorageService.scheduledTasks.scheduleWithFixedDelay(tick, interval * 4, interval * 4, TimeUnit.MILLISECONDS);
    }

    public synchronized  double getCurrentRate() {
        add(0);
        return this.currentRate;
    }

    public synchronized void setInterval(long interval) {
        this.interval = interval;
    }



    /**
     * Add to the rate counter. During an interval, an exponential
     * weighted average of the rate is maintained. Once we're outside
     * the granularity of an interval, this rate is reset. In that sense,
     * we're not really tracking a moving average.
     * @param requests
     */
    public synchronized void add(long requests) {
        final long now = System.currentTimeMillis()/interval;
        if (now - lastTick < 2) {
            eventCount += requests;
            if (now > lastTick) {
                final double alpha = (now - lastTick)/(float) interval;
                currentRate = alpha * ((double) eventCount) + (1 - alpha) * currentRate;
                lastTick = now;
                eventCount = 0;
            }
        }
        else {
            currentRate = eventCount;
            lastTick = now;
            eventCount = 0;
        }
    }
}
