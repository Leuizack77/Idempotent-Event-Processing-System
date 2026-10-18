package com.example.eventprocessor.exception;

import com.example.eventprocessor.dto.response.ErrorResponseDto;
import com.example.eventprocessor.dto.response.FieldErrorDto;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponseDto> handleValidationException(
            MethodArgumentNotValidException ex, HttpServletRequest request) {
        log.warn("Validation failed for request to {}: {}", request.getRequestURI(), ex.getMessage());

        List<FieldErrorDto> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(err -> new FieldErrorDto(
                        err.getField(),
                        err.getRejectedValue(),
                        err.getDefaultMessage()
                ))
                .toList();

        ErrorResponseDto errorResponse = new ErrorResponseDto(
                "https://api.eventprocessor.example.com/errors/validation-failed",
                "Validation Failed",
                HttpStatus.BAD_REQUEST.value(),
                "Invalid request parameters in request body",
                request.getRequestURI(),
                errors
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<ErrorResponseDto> handleInvalidEventException(
            InvalidEventException ex, HttpServletRequest request) {
        log.warn("Invalid event request to {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponseDto errorResponse = new ErrorResponseDto(
                "https://api.eventprocessor.example.com/errors/invalid-event",
                "Invalid Event",
                HttpStatus.BAD_REQUEST.value(),
                ex.getMessage(),
                request.getRequestURI(),
                null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(DuplicateEventException.class)
    public ResponseEntity<ErrorResponseDto> handleDuplicateEventException(
            DuplicateEventException ex, HttpServletRequest request) {
        log.warn("Duplicate event submission to {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponseDto errorResponse = new ErrorResponseDto(
                "https://api.eventprocessor.example.com/errors/duplicate-event",
                "Duplicate Event",
                HttpStatus.CONFLICT.value(),
                ex.getMessage(),
                request.getRequestURI(),
                null
        );

        return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponseDto> handleMalformedJson(
            HttpMessageNotReadableException ex, HttpServletRequest request) {
        log.warn("Malformed JSON request to {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponseDto errorResponse = new ErrorResponseDto(
                "https://api.eventprocessor.example.com/errors/malformed-json",
                "Malformed JSON",
                HttpStatus.BAD_REQUEST.value(),
                "Required request body is missing or unreadable",
                request.getRequestURI(),
                null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDto> handleGenericException(
            Exception ex, HttpServletRequest request) {
        log.error("Unhandled internal error processing request to {}", request.getRequestURI(), ex);

        ErrorResponseDto errorResponse = new ErrorResponseDto(
                "https://api.eventprocessor.example.com/errors/internal-error",
                "Internal Server Error",
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "An unexpected internal error occurred",
                request.getRequestURI(),
                null
        );

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}
