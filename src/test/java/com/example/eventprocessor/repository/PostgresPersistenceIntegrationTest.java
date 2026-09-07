package com.example.eventprocessor.repository;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresPersistenceIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
        // Clean tables in reverse dependency order before each test
        paymentRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Test 1 — Flyway migration: Application starts and migrations V1 and V2 execute successfully")
    void testFlywayMigrationsApplied() {
        Integer processedEventsTableCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'processed_events'",
                Integer.class
        );
        assertThat(processedEventsTableCount).isEqualTo(1);

        Integer paymentsTableCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'payments'",
                Integer.class
        );
        assertThat(paymentsTableCount).isEqualTo(1);

        Integer flywayAppliedCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history WHERE success = true",
                Integer.class
        );
        assertThat(flywayAppliedCount).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Test 2 — Processed event persistence: ProcessedEventEntity can be persisted and retrieved")
    void testProcessedEventPersistenceAndRetrieval() {
        String eventId = "EVT-TEST-001";
        ProcessedEventEntity event = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.SUBMITTED);
        event.setRetryCount(0);

        ProcessedEventEntity saved = processedEventRepository.save(event);
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();

        Optional<ProcessedEventEntity> retrieved = processedEventRepository.findByEventId(eventId);
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getEventId()).isEqualTo(eventId);
        assertThat(retrieved.get().getEventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(retrieved.get().getStatus()).isEqualTo(EventStatus.SUBMITTED);
        assertThat(retrieved.get().getRetryCount()).isEqualTo(0);
        assertThat(retrieved.get().getErrorMessage()).isNull();

        // Verify status update transition to PROCESSING and COMPLETED
        retrieved.get().setStatus(EventStatus.PROCESSING);
        retrieved.get().setProcessedAt(Instant.now());
        processedEventRepository.save(retrieved.get());

        Optional<ProcessedEventEntity> updated = processedEventRepository.findByEventId(eventId);
        assertThat(updated).isPresent();
        assertThat(updated.get().getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(updated.get().getProcessedAt()).isNotNull();
    }

    @Test
    @DisplayName("Test 3 — Unique event ID: Database uniqueness constraint uq_processed_events_event_id is strictly enforced")
    void testUniqueEventIdConstraintViolation() {
        String duplicateEventId = "EVT-DUPLICATE-999";

        // Persist first event in its own committed transaction
        transactionTemplate.executeWithoutResult(status -> {
            ProcessedEventEntity first = new ProcessedEventEntity(duplicateEventId, "PAYMENT_COMPLETED", EventStatus.SUBMITTED);
            processedEventRepository.save(first);
        });

        // Verify that the first record exists
        assertThat(processedEventRepository.findByEventId(duplicateEventId)).isPresent();

        // Attempt to persist a second event with the exact same eventId in a distinct transaction.
        // Must throw DataIntegrityViolationException due to PostgreSQL unique constraint uq_processed_events_event_id.
        assertThatThrownBy(() -> {
            transactionTemplate.executeWithoutResult(status -> {
                ProcessedEventEntity duplicate = new ProcessedEventEntity(duplicateEventId, "PAYMENT_COMPLETED", EventStatus.SUBMITTED);
                processedEventRepository.save(duplicate);
            });
        })
        .isInstanceOf(DataIntegrityViolationException.class)
        .hasMessageContaining("uq_processed_events_event_id");

        // Verify that after transaction rollback of the duplicate attempt,
        // the original record is preserved intact and exactly 1 record exists in PostgreSQL.
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ?",
                Integer.class,
                duplicateEventId
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("Test 4 — Payment persistence: PaymentEntity can be persisted and retrieved")
    void testPaymentPersistenceAndRetrieval() {
        String paymentId = "PAY-TEST-001";
        String userId = "USR-7788";
        BigDecimal amount = new BigDecimal("250.5000");
        String currency = "USD";

        PaymentEntity payment = new PaymentEntity(paymentId, userId, amount, currency);
        PaymentEntity saved = paymentRepository.save(payment);

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();

        Optional<PaymentEntity> retrieved = paymentRepository.findByPaymentId(paymentId);
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getPaymentId()).isEqualTo(paymentId);
        assertThat(retrieved.get().getUserId()).isEqualTo(userId);
        assertThat(retrieved.get().getAmount()).isEqualByComparingTo(amount);
        assertThat(retrieved.get().getCurrency()).isEqualTo(currency);

        // Verify unique payment_id constraint
        assertThatThrownBy(() -> {
            transactionTemplate.executeWithoutResult(status -> {
                PaymentEntity duplicatePayment = new PaymentEntity(paymentId, "USR-DIFFERENT", new BigDecimal("100.0000"), "EUR");
                paymentRepository.save(duplicatePayment);
            });
        })
        .isInstanceOf(DataIntegrityViolationException.class)
        .hasMessageContaining("uq_payments_payment_id");
    }
}
