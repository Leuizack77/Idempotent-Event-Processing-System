package com.example.eventprocessor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration properties for Kafka consumer retry policy and exponential backoff.
 */
@Configuration
@ConfigurationProperties(prefix = "kafka.retry")
public class KafkaRetryProperties {

    /**
     * Maximum number of processing attempts before routing to DLQ (default: 3).
     */
    private int maxAttempts = 3;

    /**
     * Initial backoff interval before first retry (default: 1 second).
     */
    private Duration initialInterval = Duration.ofSeconds(1);

    /**
     * Exponential backoff multiplier (default: 2.0).
     */
    private double multiplier = 2.0;

    /**
     * Maximum backoff interval cap (default: 10 seconds).
     */
    private Duration maxInterval = Duration.ofSeconds(10);

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getInitialInterval() {
        return initialInterval;
    }

    public void setInitialInterval(Duration initialInterval) {
        this.initialInterval = initialInterval;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

    public Duration getMaxInterval() {
        return maxInterval;
    }

    public void setMaxInterval(Duration maxInterval) {
        this.maxInterval = maxInterval;
    }
}
