package io.raza.ordernotificationservice.dto;

public record OrderPlacedEmailRequest(
        Long orderId,
        Long customerId,
        Long productId,
        Integer quantity
) {
}