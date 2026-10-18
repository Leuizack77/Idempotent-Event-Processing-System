package com.example.eventprocessor.processing;

import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.exception.DuplicateBusinessEntityException;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Business processor for PAYMENT_COMPLETED events.
 * Validates payload and persists a PaymentEntity to the database.
 * Does NOT update accounts, balances, or wallets.
 */
@Component
public class PaymentEventProcessor implements BusinessEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProcessor.class);
    public static final String EVENT_TYPE = "PAYMENT_COMPLETED";

    private final PaymentRepository paymentRepository;

    public PaymentEventProcessor(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Override
    public String getSupportedEventType() {
        return EVENT_TYPE;
    }

    @Override
    public void process(EventEnvelope envelope) {
        log.info("Processing PAYMENT_COMPLETED event: eventId={}", envelope.eventId());
        Map<String, Object> payload = envelope.payload();
        if (payload == null || payload.isEmpty()) {
            throw new InvalidEventPayloadException("Payload for PAYMENT_COMPLETED cannot be empty");
        }

        String paymentId = extractString(payload, "paymentId");
        String userId = extractString(payload, "userId");
        BigDecimal amount = extractBigDecimal(payload, "amount");
        String currency = extractString(payload, "currency");

        if (paymentRepository.existsByPaymentId(paymentId)) {
            log.warn("Payment with paymentId='{}' already exists", paymentId);
            throw new DuplicateBusinessEntityException("Payment already exists with paymentId: " + paymentId);
        }

        PaymentEntity entity = new PaymentEntity(paymentId, userId, amount, currency);
        paymentRepository.save(entity);
        log.info("Successfully persisted payment record: paymentId='{}', userId='{}', amount={}, currency='{}'",
                paymentId, userId, amount, currency);
    }

    private String extractString(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value == null) {
            throw new InvalidEventPayloadException("Missing required field: " + fieldName);
        }
        String str = value.toString().trim();
        if (str.isEmpty()) {
            throw new InvalidEventPayloadException("Field '" + fieldName + "' cannot be blank");
        }
        return str;
    }

    private BigDecimal extractBigDecimal(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value == null) {
            throw new InvalidEventPayloadException("Missing required field: " + fieldName);
        }
        BigDecimal amount;
        try {
            amount = new BigDecimal(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new InvalidEventPayloadException("Invalid numeric value for field '" + fieldName + "': " + value);
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidEventPayloadException("Field '" + fieldName + "' must be positive, got: " + amount);
        }
        return amount;
    }
}