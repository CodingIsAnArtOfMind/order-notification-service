package io.raza.ordernotificationservice.repository;

import io.raza.ordernotificationservice.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEventEntity, Long> {

    boolean existsByEventId(UUID eventId);
}