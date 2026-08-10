package com.enginecorp.workfloworchestrator.exception;

import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(WorkflowDefinitionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleWorkflowDefinitionNotFound(
            WorkflowDefinitionNotFoundException exception) {
        logger.warn("Workflow definition lookup failed: {}", exception.getMessage());
        return buildResponse(HttpStatus.NOT_FOUND, exception.getMessage(), List.of());
    }

    @ExceptionHandler(WorkflowInstanceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleWorkflowInstanceNotFound(
            WorkflowInstanceNotFoundException exception) {
        logger.warn("Workflow instance lookup failed: {}", exception.getMessage());
        return buildResponse(HttpStatus.NOT_FOUND, exception.getMessage(), List.of());
    }

    @ExceptionHandler(InvalidWorkflowStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidWorkflowState(
            InvalidWorkflowStateException exception) {
        logger.warn("Invalid workflow state transition attempted: {}", exception.getMessage());
        return buildResponse(HttpStatus.CONFLICT, exception.getMessage(), List.of());
    }

    @ExceptionHandler(DistributedLockAcquisitionException.class)
    public ResponseEntity<ErrorResponse> handleLockAcquisitionFailure(
            DistributedLockAcquisitionException exception) {
        logger.error("Distributed lock acquisition failed: {}", exception.getMessage());
        return buildResponse(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage(), List.of());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationFailure(
            MethodArgumentNotValidException exception) {
        List<String> fieldErrors = exception.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                .toList();

        return buildResponse(HttpStatus.BAD_REQUEST, "Request validation failed", fieldErrors);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpectedFailure(Exception exception) {
        logger.error("Unhandled exception reached the API boundary", exception);
        return buildResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred",
                List.of()
        );
    }

    private ResponseEntity<ErrorResponse> buildResponse(
            HttpStatus status,
            String message,
            List<String> details) {

        ErrorResponse errorResponse = new ErrorResponse(
                Instant.now(),
                status.value(),
                status.getReasonPhrase(),
                message,
                details
        );

        return ResponseEntity.status(status).body(errorResponse);
    }
}