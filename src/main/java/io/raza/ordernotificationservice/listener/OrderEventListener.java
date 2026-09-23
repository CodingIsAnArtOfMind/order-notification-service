package io.raza.ordernotificationservice.listener;


import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.exception.InvalidOrderEventException;
import io.raza.ordernotificationservice.exception.NotificationTemporaryException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

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

        if (event.productId().equals(9999L)) {
            System.out.println("Simulating TEMPORARY notification failure...");

            throw new NotificationTemporaryException(
                    "Notification provider temporarily unavailable"
            );
        }

        if (event.productId().equals(8888L)) {
            System.out.println("Simulating INVALID event...");

            throw new InvalidOrderEventException(
                    "Invalid order event"
            );
        }

        System.out.println(
                "Notification: Order "
                        + event.orderId()
                        + " placed successfully!"
        );
    }
}