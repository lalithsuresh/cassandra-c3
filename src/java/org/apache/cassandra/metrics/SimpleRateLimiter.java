package org.apache.cassandra.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lalith on 31.08.14.
 */
public class SimpleRateLimiter {
    private static final Logger logger = LoggerFactory.getLogger(SimpleRateLimiter.class);
    private long lastSent;
    private double tokens;

    private double rate;
    private double rateInterval; // in milliseconds
    private final double maxTokens;


    public SimpleRateLimiter(double initialRate, double rateInterval, double maxTokens) {
        this.rate = initialRate;
        this.rateInterval = rateInterval * 1000000;
        this.maxTokens = maxTokens;
        this.tokens = maxTokens;
        this.lastSent = System.nanoTime();
    }

    public synchronized double tryAcquire() {
        double currentTokens = Math.min(maxTokens,
                                        tokens + (rate/rateInterval * (System.nanoTime() - lastSent)));
        if (currentTokens >= 1) {
            tokens = currentTokens - 1;
            lastSent = System.nanoTime();
            return 0;
        }
        else {
            return (1 - currentTokens) * rateInterval/rate; // Nanoseconds
        }
    }

    public synchronized double getRate() {
        return rate;
    }

    public synchronized void setRate(final double rate) {
        this.rate = rate;
    }
}
