package com.example.eventprocessor.observability;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.config.MdcFilter;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.metrics.EventMetrics;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ObservabilityIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventMetrics eventMetrics;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    @DisplayName("Actuator: /actuator/health returns UP and includes liveness/readiness probes")
    void testActuatorHealthAndProbes() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")));

        mockMvc.perform(get("/actuator/health/liveness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")));

        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")));
    }

    @Test
    @DisplayName("Actuator: /actuator/info and /actuator/metrics endpoints are exposed and functional")
    void testActuatorInfoAndMetricsEndpoints() throws Exception {
        mockMvc.perform(get("/actuator/info"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/actuator/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.names", not(empty())));
    }

    @Test
    @DisplayName("MDC Filter: Ingestion propagates correlationId / traceId in HTTP headers and logs")
    void testMdcFilterCorrelationIdPropagation() throws Exception {
        String customCorrelationId = "test-corr-" + UUID.randomUUID().toString().substring(0, 8);
        String eventId = "EVT-MDC-" + UUID.randomUUID().toString().substring(0, 8);
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-MDC-1",
                        "userId": "USER-MDC",
                        "amount": 100.0,
                        "currency": "USD"
                    }
                }
                """.formatted(eventId);

        // Request with explicit X-Correlation-ID header
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
                        .header(MdcFilter.HEADER_CORRELATION_ID, customCorrelationId))
                .andExpect(status().isAccepted())
                .andExpect(header().string(MdcFilter.HEADER_CORRELATION_ID, customCorrelationId))
                .andExpect(header().string(MdcFilter.HEADER_TRACE_ID, customCorrelationId));

        // Request without header generates tr- prefix
        String autoEventId = "EVT-MDC-AUTO-" + UUID.randomUUID().toString().substring(0, 8);
        String autoRequestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-MDC-2",
                        "userId": "USER-MDC",
                        "amount": 200.0,
                        "currency": "USD"
                    }
                }
                """.formatted(autoEventId);

        MvcResult result = mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(autoRequestJson))
                .andExpect(status().isAccepted())
                .andExpect(header().exists(MdcFilter.HEADER_CORRELATION_ID))
                .andExpect(header().exists(MdcFilter.HEADER_TRACE_ID))
                .andReturn();

        String generatedTraceId = result.getResponse().getHeader(MdcFilter.HEADER_CORRELATION_ID);
        assertThat(generatedTraceId).isNotNull().startsWith("tr-");
    }

    @Test
    @DisplayName("Metrics: Processed event increments events.processed.total counter and records processing duration")
    void testMetricsRecordedOnEventProcessingAndDuplicate() throws Exception {
        String eventId = "EVT-METRICS-" + UUID.randomUUID().toString().substring(0, 8);
        String eventType = "PAYMENT_COMPLETED";

        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "%s",
                    "payload": {
                        "paymentId": "PAY-METRIC-1",
                        "userId": "USER-1",
                        "amount": 50.0,
                        "currency": "USD"
                    }
                }
                """.formatted(eventId, eventType);

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted());

        // Wait for asynchronous consumer processing to commit COMPLETED
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var entity = processedEventRepository.findByEventId(eventId);
            assertThat(entity).isPresent();
            assertThat(entity.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        });

        // Verify Micrometer counter: events.processed.total
        Counter processedCounter = meterRegistry.find(EventMetrics.METRIC_EVENTS_PROCESSED)
                .tag("eventType", eventType)
                .tag("status", "SUCCESS")
                .counter();
        assertThat(processedCounter).isNotNull();
        assertThat(processedCounter.count()).isGreaterThanOrEqualTo(1.0);

        // Verify Micrometer timer: events.processing.duration
        Timer durationTimer = meterRegistry.find(EventMetrics.METRIC_EVENTS_PROCESSING_DURATION)
                .tag("eventType", eventType)
                .tag("result", "SUCCESS")
                .timer();
        assertThat(durationTimer).isNotNull();
        assertThat(durationTimer.count()).isGreaterThanOrEqualTo(1L);

        // Verify Actuator exposes the metrics via HTTP
        mockMvc.perform(get("/actuator/metrics/" + EventMetrics.METRIC_EVENTS_PROCESSED))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(EventMetrics.METRIC_EVENTS_PROCESSED)))
                .andExpect(jsonPath("$.measurements[0].value", greaterThanOrEqualTo(1.0)));

        mockMvc.perform(get("/actuator/metrics/" + EventMetrics.METRIC_EVENTS_PROCESSING_DURATION))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(EventMetrics.METRIC_EVENTS_PROCESSING_DURATION)))
                .andExpect(jsonPath("$.measurements[0].value", greaterThanOrEqualTo(1.0)));

        // Test duplicate detection metric tracking
        EventEnvelope dupEnvelope = EventEnvelope.of(
                eventId,
                eventType,
                Map.of("paymentId", "PAY-METRIC-1", "userId", "USER-1", "amount", 50.0, "currency", "USD")
        );
        ProcessingResult dupResult = idempotencyEngine.process(dupEnvelope);
        assertThat(dupResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);

        // Verify duplicate counter incremented
        Counter dupCounter = meterRegistry.find(EventMetrics.METRIC_EVENTS_DUPLICATE)
                .tag("eventType", eventType)
                .counter();
        assertThat(dupCounter).isNotNull();
        assertThat(dupCounter.count()).isGreaterThanOrEqualTo(1.0);

        mockMvc.perform(get("/actuator/metrics/" + EventMetrics.METRIC_EVENTS_DUPLICATE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(EventMetrics.METRIC_EVENTS_DUPLICATE)))
                .andExpect(jsonPath("$.measurements[0].value", greaterThanOrEqualTo(1.0)));
    }

    @Test
    @DisplayName("Actuator: /actuator/prometheus exports Prometheus formatted metrics")
    void testPrometheusScrapeEndpoint() throws Exception {
        // Record at least one sample so Prometheus scrape output contains domain metrics
        eventMetrics.incrementProcessed("ORDER_CREATED");
        eventMetrics.incrementDuplicate("ORDER_CREATED", "REDIS");
        eventMetrics.recordDuration("ORDER_CREATED", "SUCCESS", 42L);

        MvcResult result = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertThat(body)
                .isNotEmpty()
                .contains("events_processed_total")
                .contains("events_duplicate_total")
                .contains("events_processing_duration_seconds");
    }

    @Test
    @DisplayName("Metrics: DLQ transition increments events.dlq.total counter")
    void testDlqMetricRecorded() throws Exception {
        String eventType = "ORDER_CREATED";
        String reason = "InvalidEventPayloadException";

        eventMetrics.incrementDlq(eventType, reason);

        Counter dlqCounter = meterRegistry.find(EventMetrics.METRIC_EVENTS_DLQ)
                .tag("eventType", eventType)
                .tag("reason", reason)
                .counter();

        assertThat(dlqCounter).isNotNull();
        assertThat(dlqCounter.count()).isGreaterThanOrEqualTo(1.0);

        mockMvc.perform(get("/actuator/metrics/" + EventMetrics.METRIC_EVENTS_DLQ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(EventMetrics.METRIC_EVENTS_DLQ)))
                .andExpect(jsonPath("$.measurements[0].value", greaterThanOrEqualTo(1.0)));
    }
}
