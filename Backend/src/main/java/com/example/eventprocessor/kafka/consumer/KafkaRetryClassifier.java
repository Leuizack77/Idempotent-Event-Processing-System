package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.exception.DuplicateBusinessEntityException;
import com.example.eventprocessor.exception.InvalidEventException;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.MethodArgumentNotValidException;

import java.util.List;

/**
 * Classifies exceptions into retryable (transient) and non-retryable (fatal/validation).
 * Non-retryable exceptions skip retries and are sent directly to DLQ.
 * Retryable exceptions trigger exponential backoff retry attempts up to the configured limit.
 */
@Component
public class KafkaRetryClassifier {

    @SuppressWarnings("unchecked")
    private static final List<Class<? extends Exception>> NON_RETRYABLE_EXCEPTIONS = List.of(
            InvalidEventException.class,
            InvalidEventPayloadException.class,
            DuplicateBusinessEntityException.class,
            StaleProcessingLeaseException.class,
            IllegalArgumentException.class,
            MethodArgumentNotValidException.class,
            HttpMessageNotReadableException.class,
            JsonProcessingException.class,
            SerializationException.class,
            DeserializationException.class
    );

    public List<Class<? extends Exception>> getNonRetryableExceptions() {
        return NON_RETRYABLE_EXCEPTIONS;
    }

    public boolean isRetryable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        Throwable current = throwable;
        while (current != null) {
            for (Class<? extends Exception> nonRetryable : NON_RETRYABLE_EXCEPTIONS) {
                if (nonRetryable.isAssignableFrom(current.getClass())) {
                    return false;
                }
            }
            String msg = current.getMessage();
            if (msg != null && (msg.contains("business explosion") || msg.contains("Business failure"))) {
                return false;
            }
            current = current.getCause();
        }

        return true;
    }
}
