package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.exception.NotificationTemporaryException;
import io.raza.ordernotificationservice.notification.EmailNotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalTime;

@Service
@RequiredArgsConstructor
public class OrderNotificationProcessor {

    private final InboxService inboxService;
    private final EmailNotificationService emailNotificationService;

    public void process(OrderPlacedEvent event) {

        ClaimResult claimResult =
                inboxService.claim(event);

        System.out.println(
                LocalTime.now()
                        + " CLAIM RESULT"
                        + " | eventId=" + event.eventId()
                        + " | result=" + claimResult
        );

        if (claimResult == ClaimResult.COMPLETED) {

            System.out.println(
                    "Duplicate completed event ignored"
                            + " | eventId=" + event.eventId()
            );

            return;
        }

        if (claimResult == ClaimResult.BUSY) {

            System.out.println(
                    "Claim is still fresh. Retry later"
                            + " | eventId=" + event.eventId()
            );

            throw new NotificationTemporaryException(
                    "Event is currently owned by another worker"
            );
        }

        System.out.println(
                Thread.currentThread().getName()
                        + " OWNS EVENT"
                        + " | result=" + claimResult
                        + " | eventId=" + event.eventId()
        );

        /*
         * 5555:
         * crash only on the FIRST claim.
         *
         * On a later stale reclaim we allow processing to continue,
         * otherwise this test would crash forever.
         */
        if (event.productId().equals(5555L)
                && claimResult == ClaimResult.CLAIMED) {

            System.out.println(
                    "SIMULATED CRASH AFTER FIRST CLAIM, BEFORE EMAIL"
                            + " | eventId=" + event.eventId()
            );

            throw new NotificationTemporaryException(
                    "Simulated crash after inbox claim before email"
            );
        }

        // Existing concurrency test if you still want to keep it.
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
                LocalTime.now()
                        + " EMAIL SENT"
                        + " | eventId=" + event.eventId()
        );

        // Existing crash-after-email test.
        if (event.productId().equals(7777L)) {

            throw new NotificationTemporaryException(
                    "Simulated crash after sending email"
            );
        }

        inboxService.markCompleted(event);

        System.out.println(
                LocalTime.now()
                        + " EVENT COMPLETED"
                        + " | eventId=" + event.eventId()
        );
    }
}