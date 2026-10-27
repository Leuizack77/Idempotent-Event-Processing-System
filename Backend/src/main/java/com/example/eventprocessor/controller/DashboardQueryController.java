package com.example.eventprocessor.controller;

import com.example.eventprocessor.entity.OrderEntity;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.repository.OrderRepository;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controller for dashboard observability queries (Events, Payments, Orders, State Reset).
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DashboardQueryController {

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private final OrderRepository orderRepository;

    public DashboardQueryController(
            ProcessedEventRepository processedEventRepository,
            PaymentRepository paymentRepository,
            OrderRepository orderRepository) {
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
        this.orderRepository = orderRepository;
    }

    @GetMapping("/events")
    public ResponseEntity<List<ProcessedEventEntity>> getAllEvents() {
        List<ProcessedEventEntity> events = processedEventRepository.findAll(Sort.by(Sort.Direction.DESC, "id"));
        return ResponseEntity.ok(events);
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<ProcessedEventEntity> getEventById(@PathVariable String eventId) {
        return processedEventRepository.findByEventId(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/payments")
    public ResponseEntity<List<PaymentEntity>> getAllPayments() {
        List<PaymentEntity> payments = paymentRepository.findAll(Sort.by(Sort.Direction.DESC, "id"));
        return ResponseEntity.ok(payments);
    }

    @GetMapping("/orders")
    public ResponseEntity<List<OrderEntity>> getAllOrders() {
        List<OrderEntity> orders = orderRepository.findAll(Sort.by(Sort.Direction.DESC, "id"));
        return ResponseEntity.ok(orders);
    }

    @PostMapping("/events/reset")
    @Transactional
    public ResponseEntity<Map<String, String>> resetDashboardState() {
        paymentRepository.deleteAllInBatch();
        orderRepository.deleteAllInBatch();
        processedEventRepository.deleteAllInBatch();
        return ResponseEntity.ok(Map.of("message", "Dashboard state cleared successfully"));
    }
}
