package io.raza.ordernotificationservice.config;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

import java.util.concurrent.ThreadLocalRandom;

public class ExponentialJitterBackOff implements BackOff {

    private final long initialInterval;
    private final double multiplier;
    private final long maxInterval;
    private final int maxRetries;
    private final double jitterFactor;

    public ExponentialJitterBackOff(
            long initialInterval,
            double multiplier,
            long maxInterval,
            int maxRetries,
            double jitterFactor
    ) {
        this.initialInterval = initialInterval;
        this.multiplier = multiplier;
        this.maxInterval = maxInterval;
        this.maxRetries = maxRetries;
        this.jitterFactor = jitterFactor;
    }

    @Override
    public BackOffExecution start() {

        return new BackOffExecution() {

            private int retryCount = 0;
            private long currentInterval = initialInterval;

            @Override
            public long nextBackOff() {

                if (retryCount >= maxRetries) {
                    return STOP;
                }

                double minMultiplier =
                        1.0 - jitterFactor;

                double maxMultiplier =
                        1.0 + jitterFactor;

                double randomMultiplier =
                        ThreadLocalRandom.current()
                                .nextDouble(
                                        minMultiplier,
                                        maxMultiplier
                                );

                long delay =
                        (long) (currentInterval
                                * randomMultiplier);

                retryCount++;

                currentInterval =
                        Math.min(
                                maxInterval,
                                (long) (currentInterval * multiplier)
                        );

                System.out.println(
                        "Kafka retry scheduled"
                                + " | retry=" + retryCount
                                + " | delayMs=" + delay
                );

                return delay;
            }
        };
    }
}