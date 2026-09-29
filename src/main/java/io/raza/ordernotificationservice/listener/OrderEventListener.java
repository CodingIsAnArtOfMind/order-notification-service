package io.raza.ordernotificationservice.listener;


import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.exception.InvalidOrderEventException;
import io.raza.ordernotificationservice.exception.NotificationTemporaryException;
import io.raza.ordernotificationservice.notification.EmailNotificationService;
import io.raza.ordernotificationservice.service.NotificationService;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class OrderEventListener {
    private final NotificationService notificationService;
    private final EmailNotificationService emailNotificationService;

    @KafkaListener(topics = "order-events")
    public void handleOrderPlaced(
            ConsumerRecord<Long, OrderPlacedEvent> record) {

        OrderPlacedEvent event = record.value();

        System.out.println(
                "Received event"
                        + " | key=" + record.key()
                        + " | partition=" + record.partition()
                        + " | offset=" + record.offset()
                        + " | orderId=" + event.orderId()
                        + " | customerId=" + event.customerId()
                        + " | productId=" + event.productId()
        );

        // Persistent duplicate check
        if (notificationService.isAlreadyProcessed(event.orderId())) {
            System.out.println(
                    "Duplicate event ignored for orderId="
                            + event.orderId()
            );
            return;
        }

        if (event.productId().equals(9999L)) {
            System.out.println(
                    "Simulating TEMPORARY notification failure..."
            );
            throw new NotificationTemporaryException(
                    "Notification provider temporarily unavailable"
            );
        }

        if (event.productId().equals(8888L)) {
            System.out.println(
                    "Simulating INVALID event..."
            );
            throw new InvalidOrderEventException(
                    "Invalid order event"
            );
        }
        // Real external side effect
        emailNotificationService.sendOrderPlacedEmail(event);

        System.out.println(
                "Email sent for orderId=" + event.orderId()
        );

        // Only mark processed AFTER email succeeds
        notificationService.markAsProcessed(event.orderId());
    }
}