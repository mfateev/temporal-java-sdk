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

import io.temporal.activity.ActivityCancellationType;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.RequestCancelActivityTaskCommandAttributes;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ActivityTaskCanceledEventAttributes;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.internal.replay.ExecuteActivityParameters;
import io.temporal.workflow.Functions;

public final class ActivityStateMachine
    extends EntityStateMachineInitialCommand<
        ActivityStateMachine.State, ActivityStateMachine.Action, ActivityStateMachine> {

  enum Action {
    SCHEDULE,
    CANCEL
  }

  enum State {
    CREATED,
    SCHEDULE_COMMAND_CREATED,
    SCHEDULED_EVENT_RECORDED,
    STARTED,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    CANCELED,
    SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
    SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
    STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
    STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
    COMPLETED_CANCEL_REQUESTED,
    FAILED_CANCEL_REQUESTED,
    CANCELED_CANCEL_REQUESTED,
  }

  private static StateMachine<State, Action, ActivityStateMachine> newStateMachine() {
    return StateMachine.<State, Action, ActivityStateMachine>newInstance(
            "Activity",
            State.CREATED,
            State.COMPLETED,
            State.FAILED,
            State.TIMED_OUT,
            State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SCHEDULE_COMMAND_CREATED,
            ActivityStateMachine::createScheduleActivityTaskCommand)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
            State.SCHEDULE_COMMAND_CREATED)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            State.SCHEDULED_EVENT_RECORDED)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            ActivityStateMachine::cancelScheduleCommand)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
            State.TIMED_OUT,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
            State.TIMED_OUT,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            Action.CANCEL,
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            ActivityStateMachine::createRequestCancelActivityTaskCommand)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
            ActivityStateMachine::notifyCanceledIfTryCancel)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
            State.CANCELED,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            ActivityStateMachine::notifyCanceledIfTryCancel)
        .add(
            State.STARTED,
            Action.CANCEL,
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            ActivityStateMachine::createRequestCancelActivityTaskCommand)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityStateMachine::cancelScheduleCommandNotifyCompletion)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityStateMachine::cancelScheduleCommandNotifyCompletion)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityStateMachine::notifyCompletion)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
            State.CANCELED,
            ActivityStateMachine::notifyCancellationFromEvent);
  }

  private final ExecuteActivityParameters parameters;

  private final Functions.Proc1<HistoryEvent> completionCallback;

  /**
   * @param parameters attributes used to schedule an activity
   * @param completionCallback one of ActivityTaskCompletedEvent, ActivityTaskFailedEvent,
   *     ActivityTaskTimedOutEvent, ActivityTaskCanceledEvents
   * @param commandSink sink to send commands
   * @return an instance of ActivityCommands
   */
  public static ActivityStateMachine newInstance(
      ExecuteActivityParameters parameters,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new ActivityStateMachine(parameters, completionCallback, commandSink);
  }

  private ActivityStateMachine(
      ExecuteActivityParameters parameters,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.parameters = parameters;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  public void createScheduleActivityTaskCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK)
            .setScheduleActivityTaskCommandAttributes(parameters.getAttributes())
            .build());
  }

  public void cancel() {
    if (parameters.getCancellationType() == ActivityCancellationType.ABANDON) {
      notifyCanceled();
    } else {
      action(Action.CANCEL);
    }
  }

  private void cancelScheduleCommand() {
    cancelInitialCommand();
    if (parameters.getCancellationType() != ActivityCancellationType.ABANDON) {
      notifyCanceled();
    }
  }

  private void notifyCanceledIfTryCancel() {
    if (parameters.getCancellationType() == ActivityCancellationType.TRY_CANCEL) {
      notifyCanceled();
    }
  }

  private void notifyCanceled() {
    completionCallback.apply(
        HistoryEvent.newBuilder()
            .setEventType(EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
            .setActivityTaskCanceledEventAttributes(
                ActivityTaskCanceledEventAttributes.newBuilder().setIdentity("workflow"))
            .build());
  }

  private void notifyCompletion() {
    completionCallback.apply(currentEvent);
  }

  private void notifyCancellationFromEvent() {
    if (parameters.getCancellationType() == ActivityCancellationType.WAIT_CANCELLATION_COMPLETED) {
      completionCallback.apply(currentEvent);
    }
  }

  private void cancelScheduleCommandNotifyCompletion() {
    cancelScheduleCommand();
    notifyCompletion();
  }

  private void createRequestCancelActivityTaskCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK)
            .setRequestCancelActivityTaskCommandAttributes(
                RequestCancelActivityTaskCommandAttributes.newBuilder()
                    .setScheduledEventId(getInitialCommandEventId()))
            .build());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
