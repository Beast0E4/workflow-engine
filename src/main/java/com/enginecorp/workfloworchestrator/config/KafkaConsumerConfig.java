package com.enginecorp.workfloworchestrator.config;

import com.enginecorp.workfloworchestrator.dto.messaging.CompensationResultMessage;
import com.enginecorp.workfloworchestrator.dto.messaging.TaskResultMessage;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private final String bootstrapServers;
    private final String consumerGroupId;

    public KafkaConsumerConfig(
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
        @Value("${spring.kafka.consumer.group-id}") String consumerGroupId
    ) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    private Map<String, Object> baseConsumerProperties() {
        Map<String, Object> configProperties = new HashMap<>();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProperties.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.enginecorp.workfloworchestrator.dto.messaging");
        return configProperties;
    }

    @Bean
    public ConsumerFactory<String, TaskResultMessage> taskResultConsumerFactory() {
        Map<String, Object> configProperties = baseConsumerProperties();
        configProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TaskResultMessage.class.getName());
        return new DefaultKafkaConsumerFactory<>(configProperties);
    }

    @Bean
    public ConsumerFactory<String, CompensationResultMessage> compensationResultConsumerFactory() {
        Map<String, Object> configProperties = baseConsumerProperties();
        configProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CompensationResultMessage.class.getName());
        return new DefaultKafkaConsumerFactory<>(configProperties);
    }

    @Bean
    public DefaultErrorHandler workflowResultErrorHandler(KafkaOperations<String, Object> deadLetterKafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            deadLetterKafkaOperations,
            (record, exception) -> new org.apache.kafka.common.TopicPartition(record.topic() + ".dlt", record.partition())
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskResultMessage> taskResultListenerContainerFactory(
        DefaultErrorHandler workflowResultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TaskResultMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskResultConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(workflowResultErrorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CompensationResultMessage> compensationResultListenerContainerFactory(
        DefaultErrorHandler workflowResultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, CompensationResultMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(compensationResultConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(workflowResultErrorHandler);
        return factory;
    }
}