package com.enginecorp.workfloworchestrator.worker;

import com.enginecorp.workfloworchestrator.dto.messaging.TaskResultMessage;
import com.enginecorp.workfloworchestrator.service.WorkflowOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TaskResultListener {

    private static final Logger logger = LoggerFactory.getLogger(TaskResultListener.class);

    private final WorkflowOrchestrationService workflowOrchestrationService;

    public TaskResultListener(WorkflowOrchestrationService workflowOrchestrationService) {
        this.workflowOrchestrationService = workflowOrchestrationService;
    }

    @KafkaListener(
        topics = "${workflow-engine.kafka.topics.task-result}",
        containerFactory = "taskResultListenerContainerFactory"
    )
    public void onTaskResult(ConsumerRecord<String, TaskResultMessage> record, Acknowledgment acknowledgment) {
        TaskResultMessage message = record.value();
        try {
            logger.debug(
                "Received task result for step {} on workflow instance {} — successful={}",
                message.stepExecutionId(),
                message.workflowInstanceId(),
                message.successful()
            );

            if (message.successful()) {
                workflowOrchestrationService.advanceAfterStepSuccess(message.workflowInstanceId(), message.stepExecutionId());
            } else {
                workflowOrchestrationService.handleStepFailure(
                    message.workflowInstanceId(),
                    message.stepExecutionId(),
                    message.errorMessage() != null ? message.errorMessage() : "Task reported failure without a message"
                );
            }

            acknowledgment.acknowledge();
        } catch (Exception processingException) {
            logger.error(
                "Failed to process task result for step {} on workflow instance {}",
                message.stepExecutionId(),
                message.workflowInstanceId(),
                processingException
            );
            throw processingException;
        }
    }
}