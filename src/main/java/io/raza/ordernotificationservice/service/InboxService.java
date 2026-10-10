package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.repository.InboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class InboxService {

    private static final String ORDER_PLACED = "ORDER_PLACED";

    // Short only so we can observe the behavior easily in the POC.
    private static final long CLAIM_TIMEOUT_SECONDS = 5;

    private final InboxEventRepository inboxEventRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ClaimResult claim(OrderPlacedEvent event) {

        // Case 1: no row exists
        int inserted = inboxEventRepository.tryClaim(
                event.eventId(),
                event.orderId(),
                ORDER_PLACED
        );

        if (inserted == 1) {
            return ClaimResult.CLAIMED;
        }

        // Case 2: already successfully completed
        if (inboxEventRepository.existsByEventIdAndStatus(
                event.eventId(),
                "COMPLETED"
        )) {
            return ClaimResult.COMPLETED;
        }

        // Case 3: PROCESSING row may be stale
        LocalDateTime staleBefore =
                LocalDateTime.now()
                        .minusSeconds(CLAIM_TIMEOUT_SECONDS);

        int reclaimed =
                inboxEventRepository.tryReclaim(
                        event.eventId(),
                        staleBefore
                );

        if (reclaimed == 1) {
            return ClaimResult.RECLAIMED;
        }

        // It might have completed while we were checking.
        if (inboxEventRepository.existsByEventIdAndStatus(
                event.eventId(),
                "COMPLETED"
        )) {
            return ClaimResult.COMPLETED;
        }

        // Existing PROCESSING claim is still fresh,
        // or another worker reclaimed it before us.
        return ClaimResult.BUSY;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCompleted(OrderPlacedEvent event) {

        inboxEventRepository.markCompleted(
                event.eventId()
        );
    }
}