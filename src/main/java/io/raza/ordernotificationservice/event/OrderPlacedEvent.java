package io.raza.ordernotificationservice.event;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderPlacedEvent(
        UUID eventId,
        Long orderId,
        Long customerId,
        Long productId,
        Integer quantity,
        LocalDateTime placedAt
) {
}