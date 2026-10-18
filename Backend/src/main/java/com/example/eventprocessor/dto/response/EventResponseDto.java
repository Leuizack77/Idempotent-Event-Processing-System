package com.example.eventprocessor.dto.response;

import java.time.Instant;

/**
 * REST response DTO returned with HTTP 202 Accepted.
 */
public record EventResponseDto(
        String eventId,
        String status,
        String message,
        Instant submittedAt
) {
}
