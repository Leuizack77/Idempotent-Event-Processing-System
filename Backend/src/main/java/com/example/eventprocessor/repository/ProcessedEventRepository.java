package com.example.eventprocessor.repository;

import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    Optional<ProcessedEventEntity> findByEventId(String eventId);

    boolean existsByEventId(String eventId);

    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, event_type, status, retry_count, processing_token, created_at, updated_at)
            VALUES (:eventId, :eventType, :status, 0, :processingToken, :now, :now)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfNotExists(
            @Param("eventId") String eventId,
            @Param("eventType") String eventType,
            @Param("status") String status,
            @Param("processingToken") String processingToken,
            @Param("now") Instant now
    );

    @Modifying
    @Query(value = """
            UPDATE processed_events
            SET status = 'PROCESSING',
                processing_token = :newToken,
                retry_count = CASE WHEN status IN ('FAILED', 'PROCESSING') THEN retry_count + 1 ELSE retry_count END,
                updated_at = :now
            WHERE event_id = :eventId
              AND (status IN ('SUBMITTED', 'RECEIVED', 'FAILED')
                   OR (status = 'PROCESSING' AND updated_at < :staleThreshold))
            """, nativeQuery = true)
    int transitionToProcessing(
            @Param("eventId") String eventId,
            @Param("newToken") String newToken,
            @Param("staleThreshold") Instant staleThreshold,
            @Param("now") Instant now
    );

    @Modifying
    @Query("""
            UPDATE ProcessedEventEntity p
            SET p.status = :newStatus,
                p.processedAt = :processedAt,
                p.errorMessage = :errorMessage,
                p.updatedAt = :now
            WHERE p.eventId = :eventId
              AND p.status = :expectedStatus
              AND p.processingToken = :processingToken
            """)
    int updateStatus(
            @Param("eventId") String eventId,
            @Param("newStatus") EventStatus newStatus,
            @Param("expectedStatus") EventStatus expectedStatus,
            @Param("processingToken") String processingToken,
            @Param("processedAt") Instant processedAt,
            @Param("errorMessage") String errorMessage,
            @Param("now") Instant now
    );

    @Modifying
    @Query("""
            UPDATE ProcessedEventEntity p
            SET p.updatedAt = :expiredTimestamp,
                p.errorMessage = :errorMessage
            WHERE p.eventId = :eventId
              AND p.status = com.example.eventprocessor.entity.EventStatus.PROCESSING
              AND p.processingToken = :processingToken
            """)
    int prepareForRetry(
            @Param("eventId") String eventId,
            @Param("processingToken") String processingToken,
            @Param("expiredTimestamp") Instant expiredTimestamp,
            @Param("errorMessage") String errorMessage
    );
}
