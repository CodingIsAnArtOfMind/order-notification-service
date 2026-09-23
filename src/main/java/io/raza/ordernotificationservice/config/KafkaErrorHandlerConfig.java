package io.raza.ordernotificationservice.config;

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

        return new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(1000L, 2L)
        );
    }
}