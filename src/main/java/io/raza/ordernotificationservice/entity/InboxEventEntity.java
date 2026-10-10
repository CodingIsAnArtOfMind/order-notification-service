package io.raza.ordernotificationservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "inbox_events",
        schema = "order_notification"
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InboxEventEntity {

    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "claimed_at", nullable = false)
    private LocalDateTime claimedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;
}