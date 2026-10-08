package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.entity.ProcessedEventEntity;
import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class NotificationService {
    private  static final String ORDER_PLACED = "ORDER_PLACED";

    private final ProcessedEventRepository processedEventRepository;

    public boolean isAlreadyProcessed(UUID eventId) {
        return processedEventRepository.existsByEventId(eventId);
    }

    @Transactional
    public void markAsProcessed(OrderPlacedEvent event) {

        ProcessedEventEntity entity =
                ProcessedEventEntity.builder()
                        .eventId(event.eventId())
                        .orderId(event.orderId())
                        .eventType(ORDER_PLACED)
                        .processedAt(LocalDateTime.now())
                        .build();

        processedEventRepository.save(entity);
    }
}