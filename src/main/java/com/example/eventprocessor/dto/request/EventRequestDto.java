package com.example.eventprocessor.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * REST request DTO for incoming business events.
 */
public record EventRequestDto(
        @NotBlank(message = "eventId must not be blank")
        @Size(max = 128, message = "eventId must not exceed 128 characters")
        String eventId,

        @NotBlank(message = "eventType must not be blank")
        String eventType,

        @NotEmpty(message = "payload must not be empty")
        Map<String, Object> payload
) {
}
