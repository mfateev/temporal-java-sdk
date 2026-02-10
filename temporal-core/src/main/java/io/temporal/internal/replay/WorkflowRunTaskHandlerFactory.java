package io.temporal.internal.replay;

import com.uber.m3.tally.Scope;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.worker.LocalActivityDispatcher;

@FunctionalInterface
public interface WorkflowRunTaskHandlerFactory {
  WorkflowRunTaskHandler create(
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponseOrBuilder workflowTask,
      Scope metricsScope,
      LocalActivityDispatcher localActivityDispatcher,
      GetSystemInfoResponse.Capabilities capabilities)
      throws Exception;
}
