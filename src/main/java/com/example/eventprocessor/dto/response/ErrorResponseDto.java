package com.example.eventprocessor.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/**
 * Standard RFC 7807 Problem Details response representation.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponseDto(
        String type,
        String title,
        int status,
        String detail,
        String instance,
        Instant timestamp,
        List<FieldErrorDto> errors
) {
    public ErrorResponseDto(String type, String title, int status, String detail, String instance, List<FieldErrorDto> errors) {
        this(type, title, status, detail, instance, Instant.now(), errors);
    }
}
