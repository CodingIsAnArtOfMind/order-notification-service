package io.raza.ordernotificationservice.config;

import io.raza.ordernotificationservice.exception.InvalidOrderEventException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<Object, Object> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(
                                record.topic() + ".DLT",
                                record.partition()
                        )
        );
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer) {

        ExponentialJitterBackOff backOff =
                new ExponentialJitterBackOff(
                        2000L,  // initial 2 sec
                        2.0,    // exponential multiplier
                        8000L,  // max delay 8 sec
                        4,      // max 4 retries
                        0.20    // +/- 20% jitter
                );

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(
                        recoverer,
                        backOff
                );

        errorHandler.addNotRetryableExceptions(
                InvalidOrderEventException.class
        );

        return errorHandler;
    }
}