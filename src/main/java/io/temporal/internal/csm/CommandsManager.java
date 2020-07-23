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

package io.temporal.internal.csm;

import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.RecordMarkerCommandAttributes;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.command.v1.UpsertWorkflowSearchAttributesCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionCancelRequestedEventAttributes;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.Functions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

public final class CommandsManager {

  /**
   * The eventId of the last event in the history which is expected to be startedEventId unless it
   * is replay from a JSON file.
   */
  private final long workflowTaskStartedEventId;

  /** The eventId of the started event of the last successfully executed workflow task. */
  private final long previousStartedEventId;

  private final Functions.Proc1<HistoryEvent> signalCallback;

  private final Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancelCallback;

  private final Functions.Proc eventLoopCallback;

  private Functions.Proc1<NewCommand> sink;

  /**
   * currentRunId is used as seed by Workflow.newRandom and randomUUID. It allows to generate them
   * deterministically.
   */
  private String currentRunId;

  /** Used Workflow.newRandom and randomUUID together with currentRunId. */
  private long idCounter;

  private long currentTimeMillis = -1;

  private long replayTimeUpdatedAtMillis;

  private final Map<Long, CommandsBase> commands = new HashMap<>();

  /** Commands generated by the currently processed workflow task. */
  private final List<NewCommand> newCommands = new ArrayList<>();

  /** EventId of the last handled WorkflowTaskStartedEvent. */
  private long startedEventId;

  public CommandsManager(
      long previousStartedEventId,
      long workflowTaskStartedEventId,
      Functions.Proc eventLoopCallback,
      Functions.Proc1<HistoryEvent> signalCallback,
      Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancelCallback) {
    System.out.println("NEW " + this);
    this.eventLoopCallback = Objects.requireNonNull(eventLoopCallback);
    this.signalCallback = Objects.requireNonNull(signalCallback);
    this.cancelCallback = Objects.requireNonNull(cancelCallback);
    this.previousStartedEventId = previousStartedEventId;
    this.workflowTaskStartedEventId = workflowTaskStartedEventId;
    sink = (command) -> newCommands.add(command);
  }

  public final void handleEvent(HistoryEvent event) {
    Long initialCommandEventId = getInitialCommandEventId(event);
    CommandsBase c = commands.get(initialCommandEventId);
    if (c != null) {
      c.handleEvent(event);
      if (c.isFinalState()) {
        System.out.println("commands.remove " + initialCommandEventId);
        commands.remove(initialCommandEventId);
      }
    } else {
      handleNonStatefulEvent(event);
    }
  }

  private void handleNonStatefulEvent(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        this.currentRunId =
            event.getWorkflowExecutionStartedEventAttributes().getOriginalExecutionRunId();
        break;
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        WorkflowTaskCommands c =
            WorkflowTaskCommands.newInstance(
                workflowTaskStartedEventId,
                eventLoopCallback,
                (t) -> this.setCurrentTimeMillis(t),
                (r) -> this.currentRunId = r);
        commands.put(event.getEventId(), c);
        break;
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
        signalCallback.apply(event);
        break;
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        cancelCallback.apply(event.getWorkflowExecutionCancelRequestedEventAttributes());
        break;
      default:
        // TODO(maxim)
    }
  }

  void setCurrentTimeMillis(long currentTimeMillis) {
    if (this.currentTimeMillis < currentTimeMillis) {
      this.currentTimeMillis = currentTimeMillis;
      this.replayTimeUpdatedAtMillis = System.currentTimeMillis();
    }
  }

  public long getLastStartedEventId() {
    return startedEventId;
  }

  public void setLastStartedEventId(long lastStartedEventId) {
    this.startedEventId = lastStartedEventId;
  }

  public List<Command> takeCommands() {
    System.out.println("TAKE COMMANDS");
    // Account for workflow task completed
    long commandEventId = startedEventId + 2;
    List<Command> result = new ArrayList<>(newCommands.size());
    for (NewCommand newCommand : newCommands) {
      Optional<Command> command = newCommand.getCommand();
      if (command.isPresent()) {
        result.add(command.get());
        newCommand.setInitialCommandEventId(commandEventId);
        System.out.println("commands.put " + commandEventId);

        commands.put(commandEventId, newCommand.getCommands());
        commandEventId++;
      }
    }
    newCommands.clear();
    return result;
  }

  /**
   * @param attributes attributes used to schedule an activity
   * @param completionCallback one of ActivityTaskCompletedEvent, ActivityTaskFailedEvent,
   *     ActivityTaskTimedOutEvent, ActivityTaskCanceledEvents
   * @return an instance of ActivityCommands
   */
  public ActivityCommands newActivity(
      ScheduleActivityTaskCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    return ActivityCommands.newInstance(attributes, completionCallback, sink);
  }

  /**
   * Creates a new timer state machine
   *
   * @param attributes timer command attributes
   * @param completionCallback invoked when timer fires or reports cancellation. One of
   *     TimerFiredEvent, TimerCanceledEvent.
   * @return cancellation callback that should be invoked to initiate timer cancellation
   */
  public Functions.Proc newTimer(
      StartTimerCommandAttributes attributes, Functions.Proc1<HistoryEvent> completionCallback) {
    System.out.println("newTimer");
    TimerCommands timer = TimerCommands.newInstance(attributes, completionCallback, sink);
    return () -> timer.cancel();
  }

  /**
   * Creates a new child state machine
   *
   * @param attributes child workflow start command attributes
   * @param completionCallback invoked when child reports completion or failure. The following types
   *     of events can be passed to the callback: StartChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionCompletedEvent, ChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionTimedOutEvent, ChildWorkflowExecutionCanceledEvent,
   *     ChildWorkflowExecutionTerminatedEvent.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public Functions.Proc1<ChildWorkflowCancellationType> newChildWorkflow(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    ChildWorkflowCommands child =
        ChildWorkflowCommands.newInstance(attributes, completionCallback, sink);
    return (cancellationType) -> child.cancel(cancellationType);
  }

  /**
   * @param attributes
   * @param completionCallback invoked when signal delivery completes of fails. The following types
   *     of events can be passed to the callback: ExternalWorkflowExecutionSignaledEvent,
   *     SignalExternalWorkflowExecutionFailedEvent>
   */
  public Functions.Proc newSignalExternal(
      SignalExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc2<HistoryEvent, Boolean> completionCallback) {
    return SignalExternalCommands.newInstance(attributes, completionCallback, sink);
  }

  /**
   * @param attributes attributes to use to cancel external worklfow
   * @param completionCallback one of ExternalWorkflowExecutionCancelRequestedEvent,
   *     RequestCancelExternalWorkflowExecutionFailedEvent
   */
  public void newCancelExternal(
      RequestCancelExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    CancelExternalCommands.newInstance(attributes, completionCallback, sink);
  }

  public void newUpsertSearchAttributes(
      UpsertWorkflowSearchAttributesCommandAttributes attributes) {
    UpsertSearchAttributesCommands.newInstance(attributes, sink);
  }

  public void newMarker(RecordMarkerCommandAttributes attributes) {
    MarkerCommands.newInstance(attributes, sink);
  }

  public void newCompleteWorkflow(Optional<Payloads> workflowOutput) {
    CompleteWorkflowCommands.newInstance(workflowOutput, sink);
  }

  public void newFailWorkflow(Failure failure) {
    FailWorkflowCommands.newInstance(failure, sink);
  }

  public void newCancelWorkflow() {
    CancelWorkflowCommands.newInstance(
        CancelWorkflowExecutionCommandAttributes.getDefaultInstance(), sink);
  }

  public void newContinueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    ContinueAsNewWorkflowCommands.newInstance(attributes, sink);
  }

  private long getInitialCommandEventId(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_ACTIVITY_TASK_STARTED:
        return event.getActivityTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        return event.getActivityTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_FAILED:
        return event.getActivityTaskFailedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        return event.getActivityTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        return event.getActivityTaskCancelRequestedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_REQUEST_CANCEL_ACTIVITY_TASK_FAILED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
      case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        return event.getActivityTaskCanceledEventAttributes().getScheduledEventId();
      case EVENT_TYPE_TIMER_FIRED:
        return event.getTimerFiredEventAttributes().getStartedEventId();
      case EVENT_TYPE_CANCEL_TIMER_FAILED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
      case EVENT_TYPE_TIMER_CANCELED:
        return event.getTimerCanceledEventAttributes().getStartedEventId();
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getRequestCancelExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        return event
            .getExternalWorkflowExecutionCancelRequestedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getStartChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
        return event.getChildWorkflowExecutionStartedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
        return event.getChildWorkflowExecutionCompletedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
        return event.getChildWorkflowExecutionCanceledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
        return event.getChildWorkflowExecutionTimedOutEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
        return event.getChildWorkflowExecutionTerminatedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getSignalExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
        return event.getExternalWorkflowExecutionSignaledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_WORKFLOW_TASK_STARTED:
        return event.getWorkflowTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
        return event.getWorkflowTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
        return event.getWorkflowTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_FAILED:
        return event.getWorkflowTaskFailedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      case EVENT_TYPE_TIMER_STARTED:
      case EVENT_TYPE_MARKER_RECORDED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
      case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
        return event.getEventId();
      case UNRECOGNIZED:
      case EVENT_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
    throw new IllegalStateException("unreachable");
  }

  public boolean isReplaying() {
    return previousStartedEventId <= startedEventId;
  }

  public long currentTimeMillis() {
    return currentTimeMillis;
  }

  public UUID randomUUID() {
    String runId = currentRunId;
    if (runId == null) {
      throw new Error("null currentRunId");
    }
    String id = runId + ":" + idCounter++;
    byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }

  public Random newRandom() {
    return new Random(randomUUID().getLeastSignificantBits());
  }
}
