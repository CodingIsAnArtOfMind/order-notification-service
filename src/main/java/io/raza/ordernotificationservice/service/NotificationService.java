package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.entity.ProcessedEventEntity;
import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final ProcessedEventRepository processedEventRepository;

    public boolean isAlreadyProcessed(Long orderId) {
        return processedEventRepository
                .existsByOrderIdAndEventType(
                        orderId,
                        "ORDER_PLACED"
                );
    }

    @Transactional
    public void markAsProcessed(Long orderId) {

        ProcessedEventEntity entity =
                ProcessedEventEntity.builder()
                        .orderId(orderId)
                        .eventType("ORDER_PLACED")
                        .processedAt(LocalDateTime.now())
                        .build();

        processedEventRepository.save(entity);
    }
}