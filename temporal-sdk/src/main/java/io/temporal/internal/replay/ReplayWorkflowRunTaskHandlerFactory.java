package io.temporal.internal.replay;

import com.uber.m3.tally.Scope;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.worker.LocalActivityDispatcher;
import io.temporal.internal.worker.SingleWorkerOptions;

/**
 * Factory that creates {@link ReplayWorkflowRunTaskHandler} instances. This class exists because
 * {@link ReplayWorkflowRunTaskHandler} is package-private and needs to be instantiated from outside
 * the replay package via the {@link WorkflowRunTaskHandlerFactory} interface.
 */
public final class ReplayWorkflowRunTaskHandlerFactory {

  private final SingleWorkerOptions singleWorkerOptions;

  public ReplayWorkflowRunTaskHandlerFactory(SingleWorkerOptions singleWorkerOptions) {
    this.singleWorkerOptions = singleWorkerOptions;
  }

  public WorkflowRunTaskHandler create(
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponseOrBuilder workflowTask,
      Scope metricsScope,
      LocalActivityDispatcher localActivityDispatcher,
      GetSystemInfoResponse.Capabilities capabilities)
      throws Exception {
    return new ReplayWorkflowRunTaskHandler(
        namespace,
        workflow,
        workflowTask,
        singleWorkerOptions,
        metricsScope,
        localActivityDispatcher,
        capabilities);
  }
}
