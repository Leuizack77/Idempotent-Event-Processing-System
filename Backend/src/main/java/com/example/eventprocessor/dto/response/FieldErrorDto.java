package com.example.eventprocessor.dto.response;

/**
 * Individual field validation error details.
 */
public record FieldErrorDto(
        String field,
        Object rejectedValue,
        String message
) {
}
