package io.temporal.internal.replay;

import com.google.protobuf.util.Timestamps;
import com.uber.m3.tally.Scope;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.common.VersioningBehavior;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.common.FailureUtils;
import io.temporal.internal.common.UpdateMessage;
import io.temporal.internal.statemachines.StatesMachinesCallback;
import io.temporal.internal.statemachines.WorkflowStateMachines;
import io.temporal.internal.statemachines.WorkflowStateMachinesConfig;
import io.temporal.internal.statemachines.WorkflowStateMachinesSdkCallbacksImpl;
import io.temporal.internal.sync.WorkflowThread;
import io.temporal.internal.worker.LocalActivityDispatcher;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.worker.WorkflowImplementationOptions;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory that creates {@link ReplayWorkflowRunTaskHandler} instances. This class creates and wires
 * all components (state machines, context, executor) before passing them to the core handler.
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

    HistoryEvent startedEvent = workflowTask.getHistory().getEvents(0);
    if (!startedEvent.hasWorkflowExecutionStartedEventAttributes()) {
      throw new IllegalArgumentException(
          "First event in the history is not WorkflowExecutionStarted");
    }
    WorkflowExecutionStartedEventAttributes startedAttributes =
        startedEvent.getWorkflowExecutionStartedEventAttributes();

    // 1. Extract implementation options
    WorkflowImplementationOptions implOptions = null;
    if (workflow.getWorkflowContext() != null) {
      implOptions =
          ((WorkflowContext) workflow.getWorkflowContext()).getWorkflowImplementationOptions();
    }
    if (implOptions == null) {
      implOptions = WorkflowImplementationOptions.newBuilder().build();
    }
    final WorkflowImplementationOptions finalImplOptions = implOptions;
    WorkflowStateMachinesConfig config = finalImplOptions::isEnableUpsertVersionSearchAttributes;

    // 2. Create state machines with forwarding callback
    AtomicReference<ReplayWorkflowExecutor> executorRef = new AtomicReference<>();
    Runnable destroyCheckCallback =
        () -> WorkflowThread.await("kill workflow thread if destroy requested", () -> true);
    StatesMachinesCallback callback =
        new StatesMachinesCallback() {
          @Override
          public void start(HistoryEvent startWorkflowEvent) {
            executorRef.get().start(startWorkflowEvent);
          }

          @Override
          public void eventLoop() {
            executorRef.get().eventLoop();
          }

          @Override
          public void signal(HistoryEvent signalEvent) {
            executorRef.get().handleWorkflowExecutionSignaled(signalEvent);
          }

          @Override
          public void update(UpdateMessage message) {
            executorRef.get().handleWorkflowExecutionUpdated(message);
          }

          @Override
          public void cancel(HistoryEvent cancelEvent) {
            executorRef.get().handleWorkflowExecutionCancelRequested(cancelEvent);
          }
        };
    WorkflowStateMachines workflowStateMachines =
        new WorkflowStateMachines(
            callback,
            capabilities,
            config,
            destroyCheckCallback,
            WorkflowStateMachinesSdkCallbacksImpl.INSTANCE);

    // 3. Create context
    String fullReplayDirectQueryType =
        workflowTask.hasQuery() ? workflowTask.getQuery().getQueryType() : null;
    ReplayWorkflowContextImpl context =
        new ReplayWorkflowContextImpl(
            workflowStateMachines,
            namespace,
            startedAttributes,
            workflowTask.getWorkflowExecution(),
            Timestamps.toMillis(startedEvent.getEventTime()),
            fullReplayDirectQueryType,
            singleWorkerOptions.getCoreOptions(),
            metricsScope,
            () -> new CanceledFailure("Canceled by request"));

    // 4. Create executor and wire
    ReplayWorkflowExecutor executor =
        new ReplayWorkflowExecutor(workflow, workflowStateMachines, context);
    executorRef.set(executor);

    // 5. Create exception handler
    WorkflowExceptionHandler exceptionHandler = createExceptionHandler(workflow);

    // 6. Create versioning behavior supplier — null supplier when no context,
    // matching original: if (workflow.getWorkflowContext() != null) { ... getVersioningBehavior() }
    java.util.function.Supplier<VersioningBehavior> versioningBehavior =
        workflow.getWorkflowContext() != null
            ? () -> ((WorkflowContext) workflow.getWorkflowContext()).getVersioningBehavior()
            : null;

    // 7. Create handler with all pre-wired components
    return new ReplayWorkflowRunTaskHandler(
        workflowStateMachines,
        context,
        executor,
        startedAttributes,
        metricsScope,
        localActivityDispatcher,
        exceptionHandler,
        versioningBehavior);
  }

  private WorkflowExceptionHandler createExceptionHandler(ReplayWorkflow workflow) {
    return new WorkflowExceptionHandler() {
      @Override
      public Failure handleException(Throwable e) {
        if (workflow.getWorkflowContext() == null) {
          return null;
        }
        WorkflowImplementationOptions implementationOptions =
            ((WorkflowContext) workflow.getWorkflowContext()).getWorkflowImplementationOptions();
        Class<? extends Throwable>[] failTypes =
            implementationOptions.getFailWorkflowExceptionTypes();
        for (Class<? extends Throwable> failType : failTypes) {
          if (failType.isAssignableFrom(e.getClass())) {
            return ((WorkflowContext) workflow.getWorkflowContext())
                .mapWorkflowExceptionToFailure(e);
          }
        }
        return null;
      }

      @Override
      public boolean isBenignFailure(Throwable e) {
        return FailureUtils.isBenignApplicationFailure(e);
      }
    };
  }
}
