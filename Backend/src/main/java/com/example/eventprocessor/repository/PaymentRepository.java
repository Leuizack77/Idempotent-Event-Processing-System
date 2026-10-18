package com.example.eventprocessor.repository;

import com.example.eventprocessor.entity.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {

    Optional<PaymentEntity> findByPaymentId(String paymentId);

    boolean existsByPaymentId(String paymentId);
}
