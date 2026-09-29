package io.raza.ordernotificationservice.notification;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EmailNotificationService {

    private final JavaMailSender mailSender;

    public void sendOrderPlacedEmail(OrderPlacedEvent event) {

        SimpleMailMessage message = new SimpleMailMessage();

        message.setFrom("orders@poc.local");

        // Temporary for the POC because our event does not contain email yet.
        message.setTo(
                "customer-" + event.customerId() + "@poc.local"
        );

        message.setSubject(
                "Order " + event.orderId() + " placed successfully"
        );

        message.setText(
                """
                Your order has been placed successfully.

                Order ID: %d
                Customer ID: %d
                Product ID: %d
                Quantity: %d
                """.formatted(
                        event.orderId(),
                        event.customerId(),
                        event.productId(),
                        event.quantity()
                )
        );

        mailSender.send(message);
    }
}