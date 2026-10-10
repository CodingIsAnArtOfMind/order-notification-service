package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.notification.EmailNotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderNotificationProcessor {

    private final InboxService inboxService;
    private final EmailNotificationService emailNotificationService;

    public void process(OrderPlacedEvent event) {

        boolean claimed =
                inboxService.tryClaim(event);

        if (!claimed) {

            System.out.println(
                    Thread.currentThread().getName()
                            + " EVENT ALREADY CLAIMED"
                            + " | eventId=" + event.eventId()
            );

            return;
        }

        System.out.println(
                Thread.currentThread().getName()
                        + " CLAIMED EVENT"
                        + " | eventId=" + event.eventId()
        );

        if (event.productId().equals(5555L)) {

            System.out.println(
                    "SIMULATED CRASH AFTER CLAIM, BEFORE EMAIL"
                            + " | eventId=" + event.eventId()
            );

            throw new RuntimeException(
                    "Simulated crash after inbox claim before email"
            );
        }

        // Keep this only for our concurrency experiment.
        if (event.productId().equals(6666L)) {

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

        // Keep our previous failure experiment.
        if (event.productId().equals(7777L)) {

            System.out.println(
                    "CRASH AFTER EMAIL, BEFORE COMPLETION"
            );

            throw new RuntimeException(
                    "Simulated crash after sending email"
            );
        }

        inboxService.markCompleted(event);

        System.out.println(
                Thread.currentThread().getName()
                        + " EVENT COMPLETED"
                        + " | eventId=" + event.eventId()
        );
    }
}