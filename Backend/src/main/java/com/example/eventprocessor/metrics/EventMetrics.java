package com.example.eventprocessor.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Micrometer metrics component tracking event processing lifecycle metrics:
 * - events.processed.total (eventType, status=SUCCESS)
 * - events.duplicate.total (eventType, source=REDIS|POSTGRES)
 * - events.dlq.total (eventType, reason)
 * - events.processing.duration (eventType, result=SUCCESS|DUPLICATE|FAILED)
 */
@Component
public class EventMetrics {

    public static final String METRIC_EVENTS_PROCESSED = "events.processed.total";
    public static final String METRIC_EVENTS_DUPLICATE = "events.duplicate.total";
    public static final String METRIC_EVENTS_DLQ = "events.dlq.total";
    public static final String METRIC_EVENTS_PROCESSING_DURATION = "events.processing.duration";

    private final MeterRegistry registry;

    public EventMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    public void incrementProcessed(String eventType) {
        Counter.builder(METRIC_EVENTS_PROCESSED)
                .description("Total number of events successfully processed")
                .tag("eventType", sanitize(eventType))
                .tag("status", "SUCCESS")
                .register(registry)
                .increment();
    }

    public void incrementDuplicate(String eventType, String source) {
        Counter.builder(METRIC_EVENTS_DUPLICATE)
                .description("Total number of duplicate events detected")
                .tag("eventType", sanitize(eventType))
                .tag("source", sanitize(source))
                .register(registry)
                .increment();
    }

    public void incrementDlq(String eventType, String reason) {
        Counter.builder(METRIC_EVENTS_DLQ)
                .description("Total number of events routed to Dead Letter Queue")
                .tag("eventType", sanitize(eventType))
                .tag("reason", sanitize(reason))
                .register(registry)
                .increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String eventType, String result) {
        if (sample != null) {
            sample.stop(Timer.builder(METRIC_EVENTS_PROCESSING_DURATION)
                    .description("Event processing duration")
                    .tag("eventType", sanitize(eventType))
                    .tag("result", sanitize(result))
                    .register(registry));
        }
    }

    public void recordDuration(String eventType, String result, long durationMs) {
        Timer.builder(METRIC_EVENTS_PROCESSING_DURATION)
                .description("Event processing duration")
                .tag("eventType", sanitize(eventType))
                .tag("result", sanitize(result))
                .register(registry)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    private String sanitize(String value) {
        return (value != null && !value.isBlank()) ? value : "UNKNOWN";
    }
}
