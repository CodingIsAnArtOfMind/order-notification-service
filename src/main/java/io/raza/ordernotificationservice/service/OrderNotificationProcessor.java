package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.notification.EmailNotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderNotificationProcessor {

    private final NotificationService notificationService;
    private final EmailNotificationService emailNotificationService;

    public void process(OrderPlacedEvent event) {

        if (notificationService.isAlreadyProcessed(event.eventId())) {
            System.out.println(
                    "Duplicate ignored"
                            + " | eventId=" + event.eventId()
                            + " | orderId=" + event.orderId()
            );
            return;
        }

        /*
         * TEMPORARY FOR CONCURRENCY TEST.
         *
         * Gives two threads enough time to both pass the
         * existsByEventId() check before either inserts.
         */
        if (event.productId().equals(6666L)) {
            System.out.println(
                    Thread.currentThread().getName()
                            + " passed duplicate check. Waiting..."
            );

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        emailNotificationService.sendOrderPlacedEmail(event);

        System.out.println(
                Thread.currentThread().getName()
                        + " EMAIL SENT"
                        + " | eventId=" + event.eventId()
        );

        notificationService.markAsProcessed(event);

        System.out.println(
                Thread.currentThread().getName()
                        + " DB MARKER SAVED"
                        + " | eventId=" + event.eventId()
        );
    }
}