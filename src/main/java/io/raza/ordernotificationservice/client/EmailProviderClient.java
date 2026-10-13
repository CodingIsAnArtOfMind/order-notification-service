package io.raza.ordernotificationservice.client;

import io.raza.ordernotificationservice.dto.OrderPlacedEmailRequest;
import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.exception.NotificationTemporaryException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@RequiredArgsConstructor
public class EmailProviderClient {

    private final RestClient emailProviderRestClient;

    public void sendOrderPlacedEmail(
            OrderPlacedEvent event) {

        OrderPlacedEmailRequest request =
                new OrderPlacedEmailRequest(
                        event.orderId(),
                        event.customerId(),
                        event.productId(),
                        event.quantity()
                );

        ResponseEntity<String> response =
                emailProviderRestClient
                        .post()
                        .uri("/api/emails/order-placed")
                        .header(
                                "Idempotency-Key",
                                event.eventId().toString()
                        )
                        .body(request)
                        .retrieve()
                        .toEntity(String.class);

        String result = response.getBody();

        System.out.println(
                "EMAIL PROVIDER RESPONSE"
                        + " | eventId=" + event.eventId()
                        + " | status=" + response.getStatusCode()
                        + " | result=" + result
        );

        if ("SENT".equals(result)
                || "ALREADY_SENT".equals(result)) {

            return;
        }

        if ("IN_PROGRESS".equals(result)
                || response.getStatusCode().value() == 202) {

            throw new NotificationTemporaryException(
                    "Email provider request still processing"
            );
        }

        throw new NotificationTemporaryException(
                "Unexpected email provider response: " + result
        );
    }
}