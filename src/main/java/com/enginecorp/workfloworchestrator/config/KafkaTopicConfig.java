package com.enginecorp.workfloworchestrator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${workflow-engine.kafka.topics.task-dispatch}")
    private String taskDispatchTopic;

    @Value("${workflow-engine.kafka.topics.task-result}")
    private String taskResultTopic;

    @Value("${workflow-engine.kafka.topics.compensation-dispatch}")
    private String compensationDispatchTopic;

    @Value("${workflow-engine.kafka.topics.compensation-result}")
    private String compensationResultTopic;

    @Value("${workflow-engine.kafka.topics.partitions}")
    private int partitionCount;

    @Value("${workflow-engine.kafka.topics.replication-factor}")
    private short replicationFactor;

    @Bean
    public KafkaAdmin.NewTopics workflowEngineTopics() {
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name(taskDispatchTopic).partitions(partitionCount).replicas(replicationFactor).build(),
            TopicBuilder.name(taskResultTopic).partitions(partitionCount).replicas(replicationFactor).build(),
            TopicBuilder.name(compensationDispatchTopic).partitions(partitionCount).replicas(replicationFactor).build(),
            TopicBuilder.name(compensationResultTopic).partitions(partitionCount).replicas(replicationFactor).build()
        );
    }
}