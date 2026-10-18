package com.example.eventprocessor.processing;

import com.example.eventprocessor.entity.OrderEntity;
import com.example.eventprocessor.exception.DuplicateBusinessEntityException;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Business processor for ORDER_CREATED events.
 * Validates payload and persists an OrderEntity to the database.
 */
@Component
public class OrderEventProcessor implements BusinessEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);
    public static final String EVENT_TYPE = "ORDER_CREATED";

    private final OrderRepository orderRepository;

    public OrderEventProcessor(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Override
    public String getSupportedEventType() {
        return EVENT_TYPE;
    }

    @Override
    public void process(EventEnvelope envelope) {
        log.info("Processing ORDER_CREATED event: eventId={}", envelope.eventId());
        Map<String, Object> payload = envelope.payload();
        if (payload == null || payload.isEmpty()) {
            throw new InvalidEventPayloadException("Payload for ORDER_CREATED cannot be empty");
        }

        String orderId = extractString(payload, "orderId");
        String customerId = extractString(payload, "customerId");
        BigDecimal totalAmount = extractBigDecimal(payload, "totalAmount");
        Integer itemCount = extractInteger(payload, "itemCount");

        if (orderRepository.existsByOrderId(orderId)) {
            log.warn("Order with orderId='{}' already exists", orderId);
            throw new DuplicateBusinessEntityException("Order already exists with orderId: " + orderId);
        }

        OrderEntity entity = new OrderEntity(orderId, customerId, totalAmount, itemCount);
        orderRepository.save(entity);
        log.info("Successfully persisted order record: orderId='{}', customerId='{}', totalAmount={}, itemCount={}",
                orderId, customerId, totalAmount, itemCount);
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

    private Integer extractInteger(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value == null) {
            throw new InvalidEventPayloadException("Missing required field: " + fieldName);
        }
        int count;
        try {
            if (value instanceof Number num) {
                count = num.intValue();
            } else {
                count = Integer.parseInt(value.toString().trim());
            }
        } catch (NumberFormatException e) {
            throw new InvalidEventPayloadException("Invalid integer value for field '" + fieldName + "': " + value);
        }
        if (count <= 0) {
            throw new InvalidEventPayloadException("Field '" + fieldName + "' must be greater than zero, got: " + count);
        }
        return count;
    }
}