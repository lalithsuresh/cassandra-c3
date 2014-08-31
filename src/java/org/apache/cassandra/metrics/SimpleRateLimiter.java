package org.apache.cassandra.metrics;

/**
 * Created by lalith on 31.08.14.
 */
public class SimpleRateLimiter {
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
        tokens = Math.min(maxTokens,
                tokens + (rate/rateInterval * (System.nanoTime() - lastSent)));
        if (tokens >= 1) {
            tokens -= 1;
            lastSent = System.nanoTime();
            return 0;
        }
        else {
            return (1 - tokens) * rateInterval/rate; // Nanoseconds
        }
    }

    public synchronized double getRate() {
        return rate;
    }

    public synchronized void setRate(final double rate) {
        this.rate = rate;
    }
}
