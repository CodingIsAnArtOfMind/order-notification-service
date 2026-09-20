package io.raza.ordernotificationservice.event;

import java.time.LocalDateTime;

public record OrderPlacedEvent(
        Long orderId,
        Long customerId,
        Long productId,
        Integer quantity,
        LocalDateTime placedAt
) {
}