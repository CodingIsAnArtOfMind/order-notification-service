package io.raza.ordernotificationservice.service;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.repository.InboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class InboxService {

    private static final String ORDER_PLACED = "ORDER_PLACED";

    private final InboxEventRepository inboxEventRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryClaim(OrderPlacedEvent event) {

        int inserted = inboxEventRepository.tryClaim(
                event.eventId(),
                event.orderId(),
                ORDER_PLACED
        );

        return inserted == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCompleted(OrderPlacedEvent event) {

        inboxEventRepository.markCompleted(
                event.eventId()
        );
    }
}