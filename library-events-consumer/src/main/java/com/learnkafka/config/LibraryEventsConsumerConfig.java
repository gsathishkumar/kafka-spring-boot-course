package com.learnkafka.config;

import com.learnkafka.service.FailureRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

    @Autowired
    FailureRecordService failureRecordService;

    @Autowired
    KafkaTemplate kafkaTemplate;
    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {

        log.error("Exception in ConsumerRecordRecoverer : {}", e.getMessage(), e);
        ConsumerRecord<Integer, String> record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside Recovery Logic");
            failureRecordService.saveFailedRecord(record, e, RETRY);
        } else {
            log.info("Inside Non-Recovery Logic");
            failureRecordService.saveFailedRecord(record, e, DEAD);
        }
    };
    @Value("${topics.retry}")
    private String retryTopic;
    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in DeadLetterPublishingRecoverer : {}", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        log.error("Inside the RecoverableDataAccessException Block");
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        log.error("Inside Dead Letter Block");
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });
        return recoverer;
    }

    public DefaultErrorHandler errorHandler() {

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer,
                //publishingRecoverer(),
                //fixedBackOff
                expBackOff
        );

        List<Class<? extends Exception>> exceptionsToIgnore = List.of(IllegalArgumentException.class);
        List<Class<? extends Exception>> exceptionsToRetry = List.of(RecoverableDataAccessException.class);
        exceptionsToIgnore.forEach(errorHandler::addNotRetryableExceptions);
        //exceptionsToRetry.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {}, {}", ex.getMessage(), deliveryAttempt);
        });
        return errorHandler;
    }


    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
