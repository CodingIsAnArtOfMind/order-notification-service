package io.raza.ordernotificationservice;

import io.raza.ordernotificationservice.event.OrderPlacedEvent;
import io.raza.ordernotificationservice.service.OrderNotificationProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class ConcurrentIdempotencyTest {

    @Autowired
    private OrderNotificationProcessor processor;

    @Test
    void shouldDemonstrateCheckThenActRace() throws Exception {

        int numberOfThreads = 2;

        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                999L,
                105L,
                6666L,   // activates 2-second race window
                1,
                LocalDateTime.now()
        );

        System.out.println("TEST EVENT ID = " + eventId);

        ExecutorService executor =
                Executors.newFixedThreadPool(numberOfThreads);

        CountDownLatch readyLatch =
                new CountDownLatch(numberOfThreads);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        Runnable task = () -> {
            try {

                System.out.println(
                        Thread.currentThread().getName()
                                + " READY"
                );

                // Tell main thread this worker is ready
                readyLatch.countDown();

                // Wait until main releases all workers
                startLatch.await();

                processor.process(event);

            } catch (Exception e) {

                System.out.println(
                        Thread.currentThread().getName()
                                + " FAILED"
                                + " | "
                                + e.getClass().getSimpleName()
                                + " | "
                                + e.getMessage()
                );
            }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(task);
        }

        // Wait until ALL worker threads are ready
        readyLatch.await();

        System.out.println(
                "ALL THREADS READY — STARTING TOGETHER"
        );

        // Release all waiting workers
        startLatch.countDown();

        executor.shutdown();

        while (!executor.isTerminated()) {
            Thread.sleep(100);
        }
    }
}