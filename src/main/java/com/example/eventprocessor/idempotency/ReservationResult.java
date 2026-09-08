package com.example.eventprocessor.idempotency;

/**
 * Immutable carrier of the atomic reservation decision for an event.
 */
public record ReservationResult(ReservationStatus status, String eventId, String processingToken) {

    public static ReservationResult reserved(String eventId, String processingToken) {
        return new ReservationResult(ReservationStatus.RESERVED, eventId, processingToken);
    }

    public static ReservationResult alreadyCompleted(String eventId) {
        return new ReservationResult(ReservationStatus.ALREADY_COMPLETED, eventId, null);
    }

    public static ReservationResult activeProcessing(String eventId) {
        return new ReservationResult(ReservationStatus.ACTIVE_PROCESSING, eventId, null);
    }

    public boolean isReserved() {
        return status == ReservationStatus.RESERVED;
    }

    public boolean isAlreadyCompleted() {
        return status == ReservationStatus.ALREADY_COMPLETED;
    }

    public boolean isActiveProcessing() {
        return status == ReservationStatus.ACTIVE_PROCESSING;
    }
}
