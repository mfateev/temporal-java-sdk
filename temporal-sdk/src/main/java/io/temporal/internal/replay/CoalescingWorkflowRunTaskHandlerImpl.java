package io.temporal.internal.replay;

import com.uber.m3.tally.Scope;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.worker.LocalActivityDispatcher;
import io.temporal.internal.worker.SingleWorkerOptions;

public class CoalescingWorkflowRunTaskHandlerImpl implements WorkflowRunTaskHandler {

  //  private final String namespace;
  //  private final ReplayWorkflow workflow;
  //  private final PollWorkflowTaskQueueResponseOrBuilder workflowTask;
  //  private final SingleWorkerOptions workerOptions;
  //  private final Scope metricScope;
  //  private final LocalActivityDispatcher localActivityDispatcher;
  //  private final GetSystemInfoResponse.Capabilities capabilities;
  private WorkflowRunTaskHandler next;

  public CoalescingWorkflowRunTaskHandlerImpl(
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponseOrBuilder workflowTask,
      SingleWorkerOptions workerOptions,
      Scope metricsScope,
      LocalActivityDispatcher localActivityDispatcher,
      GetSystemInfoResponse.Capabilities capabilities) {
    //    this.namespace = namespace;
    //    this.workflow = workflow;
    //    this.workflowTask = workflowTask;
    //    this.workerOptions = workerOptions;
    //    this.metricScope = metricsScope;
    //    this.localActivityDispatcher = localActivityDispatcher;
    //    this.capabilities = capabilities;
    next =
        new ReplayWorkflowRunTaskHandler(
            namespace,
            workflow,
            workflowTask,
            workerOptions,
            metricsScope,
            localActivityDispatcher,
            capabilities);
  }

  @Override
  public WorkflowTaskResult handleWorkflowTask(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, WorkflowHistoryIterator historyIterator)
      throws Throwable {
    return next.handleWorkflowTask(workflowTask, historyIterator);
  }

  @Override
  public QueryResult handleDirectQueryWorkflowTask(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, WorkflowHistoryIterator historyIterator)
      throws Throwable {
    return next.handleDirectQueryWorkflowTask(workflowTask, historyIterator);
  }

  @Override
  public void resetStartedEvenId(Long eventId) {
    next.resetStartedEvenId(eventId);
  }

  @Override
  public void close() {
    next.close();
  }
}
