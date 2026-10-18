package com.example.eventprocessor.controller;

import com.example.eventprocessor.dto.request.EventRequestDto;
import com.example.eventprocessor.dto.response.EventResponseDto;
import com.example.eventprocessor.service.EventIngestionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for synchronous event ingestion.
 */
@RestController
@RequestMapping("/api/events")
public class EventIngestionController {

    private final EventIngestionService eventIngestionService;

    public EventIngestionController(EventIngestionService eventIngestionService) {
        this.eventIngestionService = eventIngestionService;
    }

    @PostMapping
    public ResponseEntity<EventResponseDto> ingestEvent(@Valid @RequestBody EventRequestDto request) {
        EventResponseDto response = eventIngestionService.ingestEvent(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
