package io.raza.ordernotificationservice.listener;


import io.raza.ordernotificationservice.event.OrderPlacedEvent;
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
        );

        System.out.println(
                "Notification: Order "
                        + event.orderId()
                        + " placed successfully!"
        );
    }
}