package io.raza.ordernotificationservice.repository;

import io.raza.ordernotificationservice.entity.InboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface InboxEventRepository
        extends JpaRepository<InboxEventEntity, UUID> {

    @Modifying
    @Query(
            value = """
                    INSERT INTO order_notification.inbox_events
                        (event_id, order_id, event_type, status, claimed_at)
                    VALUES
                        (:eventId, :orderId, :eventType, 'PROCESSING', NOW())
                    ON CONFLICT (event_id) DO NOTHING
                    """,
            nativeQuery = true
    )
    int tryClaim(
            @Param("eventId") UUID eventId,
            @Param("orderId") Long orderId,
            @Param("eventType") String eventType
    );

    @Modifying
    @Query(
            value = """
                    UPDATE order_notification.inbox_events
                    SET status = 'COMPLETED',
                        completed_at = NOW()
                    WHERE event_id = :eventId
                    """,
            nativeQuery = true
    )
    int markCompleted(
            @Param("eventId") UUID eventId
    );
}