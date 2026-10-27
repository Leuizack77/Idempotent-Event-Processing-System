package com.example.eventprocessor.processing;

import com.example.eventprocessor.exception.InvalidEventException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry holding business event processors mapped by their supported eventType.
 */
@Component
public class EventProcessorRegistry {

    private final Map<String, BusinessEventProcessor> processorMap = new ConcurrentHashMap<>();

    public EventProcessorRegistry(List<BusinessEventProcessor> processors) {
        if (processors != null) {
            for (BusinessEventProcessor processor : processors) {
                processorMap.put(processor.getSupportedEventType(), processor);
                if ("PAYMENT_COMPLETED".equals(processor.getSupportedEventType())) {
                    processorMap.put("PAYMENT_PROCESSED", processor);
                }
            }
        }
    }

    public Optional<BusinessEventProcessor> getProcessor(String eventType) {
        if (eventType == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(processorMap.get(eventType));
    }

    public BusinessEventProcessor getRequiredProcessor(String eventType) {
        return getProcessor(eventType)
                .orElseThrow(() -> new InvalidEventException("No processor registered for eventType: " + eventType));
    }
}