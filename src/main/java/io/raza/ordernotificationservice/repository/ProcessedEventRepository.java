package io.raza.ordernotificationservice.repository;

import io.raza.ordernotificationservice.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEventEntity, Long> {

    boolean existsByOrderIdAndEventType(
            Long orderId,
            String eventType
    );
}