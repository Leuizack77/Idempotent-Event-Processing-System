package com.example.eventprocessor.controller;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class EventIngestionControllerTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Test 1 — Valid PAYMENT_COMPLETED event returns 202 Accepted and persists event as SUBMITTED")
    void testValidPaymentCompletedEventIngestion() throws Exception {
        String eventId = "EVT-PAY-1001";
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-501",
                        "userId": "USER-9921",
                        "amount": 250.00,
                        "currency": "USD"
                    }
                }
                """.formatted(eventId);

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId", is(eventId)))
                .andExpect(jsonPath("$.status", is("SUBMITTED")))
                .andExpect(jsonPath("$.message", is("Event accepted for asynchronous processing")))
                .andExpect(jsonPath("$.submittedAt", notNullValue()));

        Optional<ProcessedEventEntity> persisted = processedEventRepository.findByEventId(eventId);
        assertThat(persisted).isPresent();
        assertThat(persisted.get().getStatus()).isEqualTo(EventStatus.SUBMITTED);
        assertThat(persisted.get().getEventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(persisted.get().getRetryCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Test 2 — Missing eventId returns 400 Bad Request with RFC 7807 error response")
    void testMissingEventIdReturns400() throws Exception {
        String requestJson = """
                {
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-501",
                        "userId": "USER-9921",
                        "amount": 250.00,
                        "currency": "USD"
                    }
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.title", is("Validation Failed")))
                .andExpect(jsonPath("$.instance", is("/api/events")))
                .andExpect(jsonPath("$.errors[?(@.field == 'eventId')].message", notNullValue()));
    }

    @Test
    @DisplayName("Test 3 — Blank eventId returns 400 Bad Request")
    void testBlankEventIdReturns400() throws Exception {
        String requestJson = """
                {
                    "eventId": "   ",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-501",
                        "userId": "USER-9921",
                        "amount": 250.00,
                        "currency": "USD"
                    }
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.title", is("Validation Failed")))
                .andExpect(jsonPath("$.errors[?(@.field == 'eventId')].message", notNullValue()));
    }

    @Test
    @DisplayName("Test 4 — eventId exceeding 128 characters returns 400 Bad Request")
    void testEventIdExceeding128CharsReturns400() throws Exception {
        String longEventId = "A".repeat(129);
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-501",
                        "userId": "USER-9921",
                        "amount": 250.00,
                        "currency": "USD"
                    }
                }
                """.formatted(longEventId);

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errors[?(@.field == 'eventId')].message", notNullValue()));
    }

    @Test
    @DisplayName("Test 5 — Missing eventType returns 400 Bad Request")
    void testMissingEventTypeReturns400() throws Exception {
        String requestJson = """
                {
                    "eventId": "EVT-1002",
                    "payload": {
                        "paymentId": "PAY-501",
                        "userId": "USER-9921",
                        "amount": 250.00,
                        "currency": "USD"
                    }
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errors[?(@.field == 'eventType')].message", notNullValue()));
    }

    @Test
    @DisplayName("Test 6 — Unsupported eventType returns 400 Bad Request")
    void testUnsupportedEventTypeReturns400() throws Exception {
        String requestJson = """
                {
                    "eventId": "EVT-1003",
                    "eventType": "USER_DELETED",
                    "payload": {
                        "someField": "value"
                    }
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.title", is("Invalid Event")))
                .andExpect(jsonPath("$.detail", containsString("Unsupported eventType: 'USER_DELETED'")));
    }

    @Test
    @DisplayName("Test 7 — Missing or empty payload returns 400 Bad Request")
    void testMissingOrEmptyPayloadReturns400() throws Exception {
        // Empty payload map
        String emptyPayloadJson = """
                {
                    "eventId": "EVT-1004",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {}
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(emptyPayloadJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errors[?(@.field == 'payload')].message", notNullValue()));

        // Missing payload attribute
        String missingPayloadJson = """
                {
                    "eventId": "EVT-1005",
                    "eventType": "PAYMENT_COMPLETED"
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(missingPayloadJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.errors[?(@.field == 'payload')].message", notNullValue()));
    }

    @Test
    @DisplayName("Test 8 — Valid ORDER_CREATED event returns 202 Accepted and persists event as SUBMITTED")
    void testValidOrderCreatedEventIngestion() throws Exception {
        String eventId = "EVT-ORD-2001";
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "ORDER_CREATED",
                    "payload": {
                        "orderId": "ORD-901",
                        "customerId": "CUST-1122",
                        "totalAmount": 149.95,
                        "itemCount": 3
                    }
                }
                """.formatted(eventId);

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId", is(eventId)))
                .andExpect(jsonPath("$.status", is("SUBMITTED")))
                .andExpect(jsonPath("$.message", is("Event accepted for asynchronous processing")))
                .andExpect(jsonPath("$.submittedAt", notNullValue()));

        Optional<ProcessedEventEntity> persisted = processedEventRepository.findByEventId(eventId);
        assertThat(persisted).isPresent();
        assertThat(persisted.get().getStatus()).isEqualTo(EventStatus.SUBMITTED);
        assertThat(persisted.get().getEventType()).isEqualTo("ORDER_CREATED");
    }

    @Test
    @DisplayName("Test 9 — Submitting an event with already existing eventId returns 409 Conflict")
    void testDuplicateEventIdReturns409Conflict() throws Exception {
        String eventId = "EVT-DUP-3001";
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-999",
                        "userId": "USER-1234",
                        "amount": 75.00,
                        "currency": "EUR"
                    }
                }
                """.formatted(eventId);

        // First submission succeeds
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted());

        // Second submission with exact same eventId fails with 409 Conflict
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)))
                .andExpect(jsonPath("$.title", is("Duplicate Event")))
                .andExpect(jsonPath("$.detail", containsString("already exists")));

        // Verify only 1 record exists in database
        assertThat(processedEventRepository.count()).isEqualTo(1);
    }
}
