package io.temporal.internal.replay;

import io.temporal.api.failure.v1.Failure;
import javax.annotation.Nullable;

/**
 * Interface for handling exceptions that occur during workflow event processing. The SDK provides
 * an implementation that captures {@code
 * WorkflowImplementationOptions.getFailWorkflowExceptionTypes()} and {@code
 * WorkflowContext.mapWorkflowExceptionToFailure()} in a closure.
 */
public interface WorkflowExceptionHandler {
  /**
   * If the exception should cause the workflow to fail, return the serialized Failure. Otherwise
   * return null to indicate the exception should be propagated normally.
   */
  @Nullable
  Failure handleException(Throwable e);

  /**
   * Check if the throwable represents a benign application failure that should not increment the
   * workflow failed counter.
   */
  boolean isBenignFailure(Throwable e);
}
