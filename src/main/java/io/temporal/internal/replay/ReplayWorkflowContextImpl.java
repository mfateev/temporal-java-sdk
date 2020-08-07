/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.replay;

import static io.temporal.internal.common.OptionsUtils.roundUpToSeconds;

import com.uber.m3.tally.Scope;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.SearchAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.api.enums.v1.TimeoutType;
import io.temporal.api.enums.v1.WorkflowTaskFailedCause;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionCompletedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTerminatedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTimedOutEventAttributes;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.RequestCancelExternalWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.SignalExternalWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.StartChildWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskFailedEventAttributes;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.EncodedValues;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.failure.TerminatedFailure;
import io.temporal.failure.TimeoutFailure;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.csm.WorkflowStateMachines;
import io.temporal.internal.metrics.ReplayAwareScope;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.workflow.CancelExternalWorkflowException;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
import io.temporal.workflow.Promise;
import io.temporal.workflow.SignalExternalWorkflowException;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO(maxim): callbacks usage is non consistent. It accepts Optional and Exception which can be
 * null. Either switch both to Optional or both to nullable.
 */
final class ReplayWorkflowContextImpl implements ReplayWorkflowContext {

  private static final Logger log = LoggerFactory.getLogger(ReplayWorkflowContextImpl.class);

  private final WorkflowContext workflowContext;
  private final Scope metricsScope;
  private final boolean enableLoggingInReplay;
  private final WorkflowStateMachines workflowStateMachines;

  ReplayWorkflowContextImpl(
      WorkflowStateMachines workflowStateMachines,
      String namespace,
      WorkflowExecutionStartedEventAttributes startedAttributes,
      WorkflowExecution workflowExecution,
      long runStartedTimestampMillis,
      SingleWorkerOptions options,
      Scope metricsScope) {
    this.workflowStateMachines = workflowStateMachines;
    this.workflowContext =
        new WorkflowContext(
            namespace,
            workflowExecution,
            startedAttributes,
            runStartedTimestampMillis,
            options.getContextPropagators());
    this.enableLoggingInReplay = options.getEnableLoggingInReplay();
    this.metricsScope =
        new ReplayAwareScope(metricsScope, this, workflowStateMachines::currentTimeMillis);
  }

  @Override
  public boolean getEnableLoggingInReplay() {
    return enableLoggingInReplay;
  }

  @Override
  public UUID randomUUID() {
    return workflowStateMachines.randomUUID();
  }

  @Override
  public Random newRandom() {
    return workflowStateMachines.newRandom();
  }

  @Override
  public Scope getMetricsScope() {
    return metricsScope;
  }

  @Override
  public WorkflowExecution getWorkflowExecution() {
    return workflowContext.getWorkflowExecution();
  }

  @Override
  public WorkflowExecution getParentWorkflowExecution() {
    return workflowContext.getParentWorkflowExecution();
  }

  @Override
  public Optional<String> getContinuedExecutionRunId() {
    return workflowContext.getContinuedExecutionRunId();
  }

  @Override
  public WorkflowType getWorkflowType() {
    return workflowContext.getWorkflowType();
  }

  @Override
  public boolean isCancelRequested() {
    return workflowContext.isCancelRequested();
  }

  void setCancelRequested(boolean flag) {
    workflowContext.setCancelRequested(flag);
  }

  @Override
  public ContinueAsNewWorkflowExecutionCommandAttributes getContinueAsNewOnCompletion() {
    return workflowContext.getContinueAsNewOnCompletion();
  }

  @Override
  public void setContinueAsNewOnCompletion(
      ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    workflowContext.setContinueAsNewOnCompletion(attributes);
  }

  @Override
  public Duration getWorkflowTaskTimeout() {
    return workflowContext.getWorkflowTaskTimeout();
  }

  @Override
  public String getTaskQueue() {
    return workflowContext.getTaskQueue();
  }

  @Override
  public String getNamespace() {
    return workflowContext.getNamespace();
  }

  @Override
  public String getWorkflowId() {
    return workflowContext.getWorkflowExecution().getWorkflowId();
  }

  @Override
  public String getRunId() {
    String result = workflowContext.getWorkflowExecution().getRunId();
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  @Override
  public Duration getWorkflowRunTimeout() {
    return workflowContext.getWorkflowRunTimeout();
  }

  @Override
  public Duration getWorkflowExecutionTimeout() {
    return workflowContext.getWorkflowExecutionTimeout();
  }

  @Override
  public long getRunStartedTimestampMillis() {
    return workflowContext.getRunStartedTimestampMillis();
  }

  @Override
  public long getWorkflowExecutionExpirationTimestampMillis() {
    return workflowContext.getWorkflowExecutionExpirationTimestampMillis();
  }

  @Override
  public SearchAttributes getSearchAttributes() {
    return workflowContext.getSearchAttributes();
  }

  @Override
  public List<ContextPropagator> getContextPropagators() {
    return workflowContext.getContextPropagators();
  }

  @Override
  public Map<String, Object> getPropagatedContexts() {
    return workflowContext.getPropagatedContexts();
  }

  @Override
  public Consumer<Exception> scheduleActivityTask(
      ExecuteActivityParameters parameters, BiConsumer<Optional<Payloads>, Failure> callback) {
    ScheduleActivityTaskCommandAttributes.Builder attributes = parameters.getAttributes();
    if (attributes.getActivityId().isEmpty()) {
      attributes.setActivityId(workflowStateMachines.randomUUID().toString());
    }
    Functions.Proc cancellationHandler =
        workflowStateMachines.scheduleActivityTask(parameters, callback);
    return (exception) -> cancellationHandler.apply();
  }

  @Override
  public Functions.Proc scheduleLocalActivityTask(
      ExecuteLocalActivityParameters parameters,
      Functions.Proc2<Optional<Payloads>, Failure> callback) {
    return workflowStateMachines.scheduleLocalActivityTask(parameters, callback);
  }

  @Override
  public Functions.Proc1<Exception> startChildWorkflow(
      StartChildWorkflowExecutionParameters parameters,
      Functions.Proc1<WorkflowExecution> executionCallback,
      Functions.Proc2<Optional<Payloads>, Exception> callback) {
    StartChildWorkflowExecutionCommandAttributes startAttributes = parameters.getRequest().build();
    Functions.Proc1<ChildWorkflowCancellationType> cancellationHandler =
        workflowStateMachines.newChildWorkflow(
            startAttributes,
            executionCallback,
            event -> handleChildWorkflowCallback(callback, startAttributes, event));
    return (exception) -> cancellationHandler.apply(parameters.getCancellationType());
  }

  private void handleChildWorkflowCallback(
      Functions.Proc2<Optional<Payloads>, Exception> callback,
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
        {
          StartChildWorkflowExecutionFailedEventAttributes attributes =
              event.getStartChildWorkflowExecutionFailedEventAttributes();
          Exception failure =
              new ChildWorkflowTaskFailedException(
                  event.getEventId(),
                  WorkflowExecution.newBuilder().setWorkflowId(attributes.getWorkflowId()).build(),
                  attributes.getWorkflowType(),
                  RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE,
                  null);
          failure.initCause(
              new WorkflowExecutionAlreadyStarted(
                  WorkflowExecution.newBuilder().setWorkflowId(attributes.getWorkflowId()).build(),
                  attributes.getWorkflowType().getName(),
                  null));
          callback.apply(Optional.empty(), failure);
          return;
        }
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
        {
          ChildWorkflowExecutionCompletedEventAttributes attributes =
              event.getChildWorkflowExecutionCompletedEventAttributes();
          Optional<Payloads> result =
              attributes.hasResult() ? Optional.of(attributes.getResult()) : Optional.empty();
          callback.apply(result, null);
          return;
        }
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
        {
          ChildWorkflowExecutionFailedEventAttributes attributes =
              event.getChildWorkflowExecutionFailedEventAttributes();
          RuntimeException failure =
              new ChildWorkflowTaskFailedException(
                  event.getEventId(),
                  attributes.getWorkflowExecution(),
                  attributes.getWorkflowType(),
                  attributes.getRetryState(),
                  attributes.getFailure());
          callback.apply(Optional.empty(), failure);
          return;
        }
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
        {
          ChildWorkflowExecutionTimedOutEventAttributes attributes =
              event.getChildWorkflowExecutionTimedOutEventAttributes();
          TimeoutFailure timeoutFailure =
              new TimeoutFailure(null, null, TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE);
          timeoutFailure.setStackTrace(new StackTraceElement[0]);
          RuntimeException failure =
              new ChildWorkflowFailure(
                  attributes.getInitiatedEventId(),
                  attributes.getStartedEventId(),
                  attributes.getWorkflowType().getName(),
                  attributes.getWorkflowExecution(),
                  attributes.getNamespace(),
                  attributes.getRetryState(),
                  timeoutFailure);
          callback.apply(Optional.empty(), failure);
          return;
        }
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
        {
          ChildWorkflowExecutionCanceledEventAttributes attributes =
              event.getChildWorkflowExecutionCanceledEventAttributes();
          CanceledFailure failure =
              new CanceledFailure(
                  "Child canceled", new EncodedValues(attributes.getDetails()), null);
          callback.apply(Optional.empty(), failure);
          return;
        }
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
        {
          ChildWorkflowExecutionTerminatedEventAttributes attributes =
              event.getChildWorkflowExecutionTerminatedEventAttributes();
          RuntimeException failure =
              new ChildWorkflowFailure(
                  attributes.getInitiatedEventId(),
                  attributes.getStartedEventId(),
                  attributes.getWorkflowType().getName(),
                  attributes.getWorkflowExecution(),
                  attributes.getNamespace(),
                  RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE,
                  new TerminatedFailure(null, null));
          callback.apply(Optional.empty(), failure);
          return;
        }
      default:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
  }

  @Override
  public Consumer<Exception> signalExternalWorkflowExecution(
      SignalExternalWorkflowExecutionCommandAttributes.Builder attributes,
      BiConsumer<Void, Exception> callback) {
    Functions.Proc cancellationHandler =
        workflowStateMachines.newSignalExternal(
            attributes.build(),
            (event, canceled) -> handleSignalExternalCallback(callback, event, canceled));
    return (e) -> cancellationHandler.apply();
  }

  private void handleSignalExternalCallback(
      BiConsumer<Void, Exception> callback, HistoryEvent event, boolean canceled) {
    if (canceled) {
      CanceledFailure failure = new CanceledFailure("Signal external workflow execution canceled");
      callback.accept(null, failure);
      return;
    }
    switch (event.getEventType()) {
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
        callback.accept(null, null);
        return;
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        {
          SignalExternalWorkflowExecutionFailedEventAttributes attributes =
              event.getSignalExternalWorkflowExecutionFailedEventAttributes();
          WorkflowExecution signaledExecution =
              WorkflowExecution.newBuilder()
                  .setWorkflowId(attributes.getWorkflowExecution().getWorkflowId())
                  .setRunId(attributes.getWorkflowExecution().getRunId())
                  .build();
          RuntimeException failure = new SignalExternalWorkflowException(signaledExecution, null);
          callback.accept(null, failure);
          return;
        }
      default:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
  }

  @Override
  public Promise<Void> requestCancelExternalWorkflowExecution(WorkflowExecution execution) {
    RequestCancelExternalWorkflowExecutionCommandAttributes attributes =
        RequestCancelExternalWorkflowExecutionCommandAttributes.newBuilder()
            .setWorkflowId(execution.getWorkflowId())
            .setRunId(execution.getRunId())
            .build();
    CompletablePromise<Void> result = Workflow.newPromise();
    workflowStateMachines.newCancelExternal(
        attributes, event -> handleCancelExternalCallback(result, event));
    return result;
  }

  private void handleCancelExternalCallback(CompletablePromise<Void> result, HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        result.complete(null);
        return;
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        {
          RequestCancelExternalWorkflowExecutionFailedEventAttributes attributes =
              event.getRequestCancelExternalWorkflowExecutionFailedEventAttributes();
          result.completeExceptionally(
              new CancelExternalWorkflowException(attributes.getWorkflowExecution(), "", null));
          return;
        }
      default:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
  }

  @Override
  public void continueAsNewOnCompletion(
      ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    workflowContext.setContinueAsNewOnCompletion(attributes);
  }

  long getReplayCurrentTimeMilliseconds() {
    return workflowStateMachines.currentTimeMillis();
  }

  @Override
  public boolean isReplaying() {
    return workflowStateMachines.isReplaying();
  }

  @Override
  public Functions.Proc1<RuntimeException> newTimer(
      Duration delay, Functions.Proc1<RuntimeException> callback) {
    if (delay == Duration.ZERO) {
      callback.apply(null);
      return (e) -> {};
    }
    int delaySeconds = roundUpToSeconds(delay);
    StartTimerCommandAttributes attributes =
        StartTimerCommandAttributes.newBuilder()
            .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(delay))
            .setTimerId(workflowStateMachines.randomUUID().toString())
            .build();
    Functions.Proc cancellationHandler =
        workflowStateMachines.newTimer(attributes, (event) -> handleTimerCallback(callback, event));
    return (e) -> cancellationHandler.apply();
  }

  private void handleTimerCallback(Functions.Proc1<RuntimeException> callback, HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_TIMER_FIRED:
        {
          callback.apply(null);
          return;
        }
      case EVENT_TYPE_TIMER_CANCELED:
        {
          CanceledFailure exception = new CanceledFailure("Cancelled by request");
          callback.apply(exception);
          return;
        }
      default:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
  }

  @Override
  public void sideEffect(
      Func<Optional<Payloads>> func, Functions.Proc1<Optional<Payloads>> callback) {
    workflowStateMachines.sideEffect(func, callback);
  }

  @Override
  public void mutableSideEffect(
      String id,
      Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<Optional<Payloads>> callback) {
    workflowStateMachines.mutableSideEffect(id, func, callback);
  }

  @Override
  public void getVersion(
      String changeId, int minSupported, int maxSupported, Functions.Proc1<Integer> callback) {
    workflowStateMachines.getVersion(changeId, minSupported, maxSupported, callback);
  }

  @Override
  public long currentTimeMillis() {
    return workflowStateMachines.currentTimeMillis();
  }

  public void handleWorkflowTaskFailed(HistoryEvent event) {
    WorkflowTaskFailedEventAttributes attr = event.getWorkflowTaskFailedEventAttributes();
    if (attr != null
        && attr.getCause() == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW) {
      workflowContext.setCurrentRunId(attr.getNewRunId());
    }
  }

  boolean startUnstartedLaTasks(Duration maxWaitAllowed) {
    //    return workflowClock.startUnstartedLaTasks(maxWaitAllowed);
    throw new UnsupportedOperationException("TODO");
  }

  int numPendingLaTasks() {
    //    return workflowClock.numPendingLaTasks();
    // TODO(maxim): implement
    return 0;
  }

  void awaitTaskCompletion(Duration duration) throws InterruptedException {
    //    workflowClock.awaitTaskCompletion(duration);
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void upsertSearchAttributes(SearchAttributes searchAttributes) {
    //    workflowClock.upsertSearchAttributes(searchAttributes);
    workflowContext.mergeSearchAttributes(searchAttributes);
  }

  //  @Override
  //  public void handleUpsertSearchAttributes(HistoryEvent event) {
  //    UpsertWorkflowSearchAttributesEventAttributes attr =
  //        event.getUpsertWorkflowSearchAttributesEventAttributes();
  //    if (attr != null) {
  //      SearchAttributes searchAttributes = attr.getSearchAttributes();
  //      workflowContext.mergeSearchAttributes(searchAttributes);
  //    }
  //  }
}
