package com.example.eventprocessor.service;

import com.example.eventprocessor.dto.request.EventRequestDto;
import com.example.eventprocessor.dto.response.EventResponseDto;

public interface EventIngestionService {

    EventResponseDto ingestEvent(EventRequestDto request);
}
