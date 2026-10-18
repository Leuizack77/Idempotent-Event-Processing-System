package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.kafka.model.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class DefaultEventProcessingDelegate implements EventProcessingDelegate {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventProcessingDelegate.class);

    private volatile Consumer<EventEnvelope> customHandler;

    public void setCustomHandler(Consumer<EventEnvelope> customHandler) {
        this.customHandler = customHandler;
    }

    public void reset() {
        this.customHandler = null;
    }

    @Override
    public void process(EventEnvelope envelope) {
        log.info("Consumer delegate received and successfully processed event: eventId={}, eventType={}",
                envelope.eventId(), envelope.eventType());
        if (customHandler != null) {
            customHandler.accept(envelope);
        }
    }
}
