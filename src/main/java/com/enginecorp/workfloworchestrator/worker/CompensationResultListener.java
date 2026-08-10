package com.enginecorp.workfloworchestrator.worker;

import com.enginecorp.workfloworchestrator.dto.messaging.CompensationResultMessage;
import com.enginecorp.workfloworchestrator.service.CompensationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class CompensationResultListener {

    private static final Logger logger = LoggerFactory.getLogger(CompensationResultListener.class);

    private final CompensationService compensationService;

    public CompensationResultListener(CompensationService compensationService) {
        this.compensationService = compensationService;
    }

    @KafkaListener(
        topics = "${workflow-engine.kafka.topics.compensation-result}",
        containerFactory = "compensationResultListenerContainerFactory"
    )
    public void onCompensationResult(ConsumerRecord<String, CompensationResultMessage> record, Acknowledgment acknowledgment) {
        CompensationResultMessage message = record.value();
        try {
            logger.debug(
                "Received compensation result for step {} on workflow instance {} — successful={}",
                message.stepExecutionId(),
                message.workflowInstanceId(),
                message.successful()
            );
            compensationService.handleCompensationResult(message);
            acknowledgment.acknowledge();
        } catch (Exception processingException) {
            logger.error(
                "Failed to process compensation result for step {} on workflow instance {}",
                message.stepExecutionId(),
                message.workflowInstanceId(),
                processingException
            );
            throw processingException;
        }
    }
}