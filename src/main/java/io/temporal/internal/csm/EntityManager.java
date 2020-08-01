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

import static io.temporal.internal.common.WorkflowExecutionUtils.getEventTypeForCommand;
import static io.temporal.internal.common.WorkflowExecutionUtils.isCommandEvent;
import static io.temporal.internal.csm.LocalActivityStateMachine.LOCAL_ACTIVITY_MARKER_NAME;
import static io.temporal.internal.csm.LocalActivityStateMachine.MARKER_ACTIVITY_ID_KEY;

import com.google.common.base.Strings;
import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.command.v1.UpsertWorkflowSearchAttributesCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.Functions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public final class EntityManager {

  private final DataConverter dataConverter = DataConverter.getDefaultInstance();

  /**
   * The eventId of the last event in the history which is expected to be startedEventId unless it
   * is replay from a JSON file.
   */
  private long workflowTaskStartedEventId;

  /** The eventId of the started event of the last successfully executed workflow task. */
  private long previousStartedEventId;

  private final EntityManagerListener callbacks;

  /** Callback to send new commands to. */
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

  private final Map<Long, EntityStateMachine> stateMachines = new HashMap<>();

  private final Queue<NewCommand> commands = new ArrayDeque<>();

  /**
   * Commands generated by the currently processed workflow task. It is a queue as commands can be
   * added (due to marker based commands) while iterating over already added commands.
   */
  private final Queue<NewCommand> newCommands = new ArrayDeque<>();

  /** EventId of the last handled WorkflowTaskStartedEvent. */
  private long startedEventId;

  private boolean replaying;

  /** Key is mutable side effect id */
  private final Map<String, MutableSideEffectStateMachine> mutableSideEffects = new HashMap<>();

  /** Map of local activities by their id. */
  private Map<String, LocalActivityStateMachine> localActivityMap = new HashMap<>();

  private List<ExecuteLocalActivityParameters> localActivityRequests = new ArrayList<>();

  public EntityManager(EntityManagerListener callbacks) {
    this.callbacks = Objects.requireNonNull(callbacks);
    sink = (command) -> newCommands.add(command);
    System.out.println("\nNEW EntityManager " + this);
  }

  private void setStartedEventId(long startedEventId) {
    this.startedEventId = startedEventId;
  }

  public void setStartedIds(long previousStartedEventId, long workflowTaskStartedEventId) {
    this.previousStartedEventId = previousStartedEventId;
    this.workflowTaskStartedEventId = workflowTaskStartedEventId;
    replaying = previousStartedEventId > 0;
  }

  public final void handleEvent(HistoryEvent event) {
    System.out.println(
        "ENTITY MANAGER handleEvent envet=" + event.getEventType() + ", replaying=" + replaying);
    if (isCommandEvent(event)) {
      //      if (!isReplaying()) {
      //        // takeCommands already consumed it
      //        return;
      //      }
      handleCommand(event);
      return;
    }
    if (replaying
        && startedEventId > previousStartedEventId
        && event.getEventType() != EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
      replaying = false;
      System.out.println("ENTITY MANAGER handleEvent changed replaying=" + replaying);
    }
    Long initialCommandEventId = getInitialCommandEventId(event);
    EntityStateMachine c = stateMachines.get(initialCommandEventId);
    if (c != null) {
      c.handleEvent(event);
      if (c.isFinalState()) {
        stateMachines.remove(initialCommandEventId);
      }
    } else {
      handleNonStatefulEvent(event);
    }
  }

  /**
   * Handles command event
   *
   * @param event
   */
  public void handleCommand(HistoryEvent event) {
    System.out.println(
        "HANDLE COMMAND: event="
            + event.getEventType()
            + " isReplaying="
            + isReplaying()
            + ", commands="
            + commands.stream().map((c) -> c.getCommandType()).collect(Collectors.toList()));
    if (isReplaying() && event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
      MarkerRecordedEventAttributes attr = event.getMarkerRecordedEventAttributes();
      if (attr.getMarkerName().equals(LOCAL_ACTIVITY_MARKER_NAME)) {
        handleLocalActivityMarker(event, attr);
        return;
      }
    }
    NewCommand newCommand;
    while (true) {
      newCommand = commands.poll();
      if (newCommand == null) {
        break;
      }
      // Note that handleEvent can cause a command cancellation in case
      // of MutableSideEffect
      newCommand.handleEvent(event);
      if (!newCommand.isCanceled()) {
        break;
      }
      if (newCommand.getCommandType() == CommandType.COMMAND_TYPE_RECORD_MARKER) {
        prepareCommands();
      }
    }
    if (newCommand == null) {
      throw new IllegalStateException("No command scheduled that corresponds to " + event);
    }
    Command command = newCommand.getCommand();
    validateCommand(command, event);
    newCommand.setInitialCommandEventId(event.getEventId());

    EntityStateMachine commands = newCommand.getCommands();
    if (!commands.isFinalState()) {
      stateMachines.put(event.getEventId(), commands);
    }
    if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
      prepareCommands();
    }
  }

  public List<Command> takeCommands() {
    System.out.println(
        "takeCommands commands="
            + commands.stream().map((c) -> c.getCommandType()).collect(Collectors.toList()));
    List<Command> result = new ArrayList<>(commands.size());
    for (NewCommand newCommand : commands) {
      if (newCommand.isCanceled()) {
        throw new IllegalStateException("Canceled command: " + newCommand.getCommand());
      }
      Command command = newCommand.getCommand();
      result.add(command);
    }
    return result;
  }

  private void prepareCommands() {
    System.out.println(
        "prepareCommands commands="
            + newCommands.stream().map((c) -> c.getCommandType()).collect(Collectors.toList()));

    // handleCommand can lead to code execution because of SideEffect, MutableSideEffect or local
    // activity completion. And code execution can lead to creation of new commands and
    // cancellation of existing commands. That is the reason for using Queue as a data structure for
    // commands. And the double filtering on isCanceled is needed.
    //
    List<NewCommand> filteredCommands = new ArrayList<>(newCommands.size());
    while (true) {
      NewCommand newCommand = newCommands.poll();
      if (newCommand == null) {
        break;
      }
      // handleCommand should be called even on cancelled ones to support mutableSideEffect
      newCommand.getCommands().handleCommand(newCommand.getCommandType());
      if (!newCommand.isCanceled()) {
        filteredCommands.add(newCommand);
      }
    }
    for (NewCommand newCommand : filteredCommands) {
      if (!newCommand.isCanceled()) {
        commands.add(newCommand);
      }
    }
    System.out.println(
        "end prepareCommands commands="
            + commands.stream().map((c) -> c.getCommandType()).collect(Collectors.toList()));
  }

  private void handleLocalActivityMarker(HistoryEvent event, MarkerRecordedEventAttributes attr) {
    Map<String, Payloads> detailsMap = attr.getDetailsMap();
    Optional<Payloads> idPayloads = Optional.ofNullable(detailsMap.get(MARKER_ACTIVITY_ID_KEY));
    String id = dataConverter.fromPayloads(0, idPayloads, String.class, String.class);
    LocalActivityStateMachine commands = localActivityMap.remove(id);
    if (commands == null) {
      throw new IllegalStateException("Unexpected local activity id: " + id);
    }
    commands.handleEvent(event);
    eventLoop();
    prepareCommands();
  }

  private void eventLoop() {
    System.out.println("eventLoop");
    callbacks.eventLoop();
  }

  private void handleNonStatefulEvent(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        this.currentRunId =
            event.getWorkflowExecutionStartedEventAttributes().getOriginalExecutionRunId();
        callbacks.start(event);
        break;
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        WorkflowTaskStateMachine c =
            WorkflowTaskStateMachine.newInstance(
                workflowTaskStartedEventId, new WorkflowTaskCommandsListener());
        stateMachines.put(event.getEventId(), c);
        break;
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
        callbacks.signal(event);
        break;
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        callbacks.cancel(event);
        break;
      case UNRECOGNIZED:
        break;
      default:
        throw new IllegalArgumentException("Unexpected event:" + event);
        // TODO(maxim)
    }
  }

  private long setCurrentTimeMillis(long currentTimeMillis) {
    if (this.currentTimeMillis < currentTimeMillis) {
      this.currentTimeMillis = currentTimeMillis;
      this.replayTimeUpdatedAtMillis = System.currentTimeMillis();
    }
    return this.currentTimeMillis;
  }

  public long getLastStartedEventId() {
    return startedEventId;
  }

  /** Validates that command matches the event during replay. */
  private void validateCommand(Command command, HistoryEvent event) {
    if (!equals(command.getCommandType(), event.getEventType())) {
      throw new IllegalStateException(command + " doesn't match " + event);
    }
  }

  /**
   * Compares command to its correpsonding event.
   *
   * @return true if matches
   */
  private boolean equals(CommandType commandType, EventType eventType) {
    return getEventTypeForCommand(commandType) == eventType;
  }

  /**
   * @param attributes attributes used to schedule an activity
   * @param completionCallback one of ActivityTaskCompletedEvent, ActivityTaskFailedEvent,
   *     ActivityTaskTimedOutEvent, ActivityTaskCanceledEvents
   * @return an instance of ActivityCommands
   */
  public ActivityStateMachine newActivity(
      ScheduleActivityTaskCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    return ActivityStateMachine.newInstance(attributes, completionCallback, sink);
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
    TimerStateMachine timer = TimerStateMachine.newInstance(attributes, completionCallback, sink);
    return () -> timer.cancel();
  }

  /**
   * Creates a new child state machine
   *
   * @param attributes child workflow start command attributes
   * @param startedCallback callback that is notified about child start
   * @param completionCallback invoked when child reports completion or failure. The following types
   *     of events can be passed to the callback: StartChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionCompletedEvent, ChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionTimedOutEvent, ChildWorkflowExecutionCanceledEvent,
   *     ChildWorkflowExecutionTerminatedEvent.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public Functions.Proc1<ChildWorkflowCancellationType> newChildWorkflow(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<WorkflowExecution> startedCallback,
      Functions.Proc1<HistoryEvent> completionCallback) {
    ChildWorkflowStateMachine child =
        ChildWorkflowStateMachine.newInstance(
            attributes, startedCallback, completionCallback, sink);
    return (cancellationType) -> {
      // The only time child can be cancelled directly is before its start command
      // was sent out to the service. After that RequestCancelExternal should be used.
      if (child.isCancellable()) {
        if (cancellationType == ChildWorkflowCancellationType.ABANDON) {
          notifyChildCancelled(attributes, completionCallback);
          return;
        }
        child.cancel();
      } else if (!child.isFinalState()) {
        if (cancellationType == ChildWorkflowCancellationType.ABANDON) {
          notifyChildCancelled(attributes, completionCallback);
          return;
        }
        newCancelExternal(
            RequestCancelExternalWorkflowExecutionCommandAttributes.newBuilder()
                .setWorkflowId(attributes.getWorkflowId())
                .setNamespace(attributes.getNamespace())
                .build(),
            (event) -> {
              if (cancellationType == ChildWorkflowCancellationType.WAIT_CANCELLATION_REQUESTED) {
                notifyChildCancelled(attributes, completionCallback);
              }
            });
        if (cancellationType == ChildWorkflowCancellationType.TRY_CANCEL) {
          notifyChildCancelled(attributes, completionCallback);
        }
      }
    };
  }

  private static void notifyChildCancelled(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    completionCallback.apply(
        HistoryEvent.newBuilder()
            .setEventType(EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)
            .setChildWorkflowExecutionCanceledEventAttributes(
                ChildWorkflowExecutionCanceledEventAttributes.newBuilder()
                    .setWorkflowType(attributes.getWorkflowType())
                    .setNamespace(attributes.getNamespace())
                    .setWorkflowExecution(
                        WorkflowExecution.newBuilder().setWorkflowId(attributes.getWorkflowId())))
            .build());
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
    return SignalExternalStateMachine.newInstance(attributes, completionCallback, sink);
  }

  /**
   * @param attributes attributes to use to cancel external worklfow
   * @param completionCallback one of ExternalWorkflowExecutionCancelRequestedEvent,
   *     RequestCancelExternalWorkflowExecutionFailedEvent
   */
  public void newCancelExternal(
      RequestCancelExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    CancelExternalStateMachine.newInstance(attributes, completionCallback, sink);
  }

  public void newUpsertSearchAttributes(
      UpsertWorkflowSearchAttributesCommandAttributes attributes) {
    UpsertSearchAttributesStateMachine.newInstance(attributes, sink);
  }

  public void newCompleteWorkflow(Optional<Payloads> workflowOutput) {
    CompleteWorkflowStateMachine.newInstance(workflowOutput, sink);
  }

  public void newFailWorkflow(Failure failure) {
    FailWorkflowStateMachine.newInstance(failure, sink);
  }

  public void newCancelWorkflow() {
    CancelWorkflowStateMachine.newInstance(
        CancelWorkflowExecutionCommandAttributes.getDefaultInstance(), sink);
  }

  public void newContinueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    ContinueAsNewWorkflowStateMachine.newInstance(attributes, sink);
  }

  public boolean isReplaying() {
    return replaying;
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

  public void sideEffect(
      Functions.Func<Optional<Payloads>> func, Functions.Proc1<Optional<Payloads>> callback) {
    SideEffectStateMachine.newInstance(
        this::isReplaying,
        func,
        (payloads) -> {
          callback.apply(payloads);
          // callback unblocked sideEffect call. Give workflow code chance to make progress.
          eventLoop();
        },
        sink);
  }

  /**
   * @param id mutable side effect id
   * @param func given the value from the last marker returns value to store. If result is empty
   *     nothing is recorded into the history.
   * @param callback used to report result or failure
   */
  public void mutableSideEffect(
      String id,
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<Optional<Payloads>> callback) {
    MutableSideEffectStateMachine stateMachine =
        mutableSideEffects.computeIfAbsent(
            id,
            (idKey) -> MutableSideEffectStateMachine.newInstance(idKey, this::isReplaying, sink));
    stateMachine.mutableSideEffect(
        func,
        (r) -> {
          callback.apply(r);
          // callback unblocked mutableSideEffect call. Give workflow code chance to make progress.
          eventLoop();
        });
  }

  public List<ExecuteLocalActivityParameters> takeLocalActivityRequests() {
    List<ExecuteLocalActivityParameters> result = localActivityRequests;
    localActivityRequests = new ArrayList<>();
    return result;
  }

  public void handleLocalActivityCompletion(ActivityTaskHandler.Result laCompletion) {
    LocalActivityStateMachine commands = localActivityMap.get(laCompletion.getActivityId());
    if (commands == null) {
      throw new IllegalStateException("Unknown local activity: " + laCompletion.getActivityId());
    }
    commands.handleCompletion(laCompletion);
    eventLoop();
    prepareCommands();
  }

  public Functions.Proc scheduleLocalActivityTask(
      ExecuteLocalActivityParameters parameters,
      Functions.Proc2<Optional<Payloads>, Failure> callback) {
    String activityId = parameters.getActivityTask().getActivityId();
    if (Strings.isNullOrEmpty(activityId)) {
      throw new IllegalArgumentException("Missing activityId: " + activityId);
    }
    if (localActivityMap.containsKey(activityId)) {
      throw new IllegalArgumentException("Duplicated local activity id: " + activityId);
    }
    LocalActivityStateMachine commands =
        LocalActivityStateMachine.newInstance(
            this::isReplaying,
            this::setCurrentTimeMillis,
            parameters,
            (r, e) -> {
              callback.apply(r, e);
              // callback unblocked local activity call. Give workflow code chance to make progress.
              eventLoop();
            },
            sink);
    localActivityMap.put(activityId, commands);
    if (!isReplaying()) {
      localActivityRequests.add(commands.getRequest());
    }
    return () -> commands.cancel();
  }

  private class WorkflowTaskCommandsListener implements WorkflowTaskStateMachine.Listener {
    @Override
    public void workflowTaskStarted(long startedEventId, long currentTimeMillis) {
      setStartedEventId(startedEventId);
      setCurrentTimeMillis(currentTimeMillis);
      eventLoop();
      prepareCommands();
    }

    @Override
    public void updateRunId(String currentRunId) {
      EntityManager.this.currentRunId = currentRunId;
    }
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
      case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        return event.getActivityTaskCanceledEventAttributes().getScheduledEventId();
      case EVENT_TYPE_TIMER_FIRED:
        return event.getTimerFiredEventAttributes().getStartedEventId();
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
}
