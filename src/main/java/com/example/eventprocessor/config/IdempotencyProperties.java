package com.example.eventprocessor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration properties for event idempotency processing and lease management.
 */
@Configuration
@ConfigurationProperties(prefix = "idempotency")
public class IdempotencyProperties {

    /**
     * Duration an event can remain in PROCESSING status before its lease is considered expired/stale
     * and eligible for atomic recovery/reclaim. Defaults to 60 seconds.
     */
    private Duration processingLeaseDuration = Duration.ofSeconds(60);

    private CacheProperties cache = new CacheProperties();

    public Duration getProcessingLeaseDuration() {
        return processingLeaseDuration;
    }

    public void setProcessingLeaseDuration(Duration processingLeaseDuration) {
        this.processingLeaseDuration = processingLeaseDuration;
    }

    public CacheProperties getCache() {
        return cache;
    }

    public void setCache(CacheProperties cache) {
        this.cache = cache;
    }

    public static class CacheProperties {
        /**
         * TTL for completed event cache entries in Redis. Defaults to 24 hours.
         */
        private Duration ttl = Duration.ofHours(24);

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }
    }
}
