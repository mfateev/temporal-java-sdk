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

import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.RequestCancelActivityTaskCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ActivityTaskCanceledEventAttributes;
import io.temporal.api.history.v1.ActivityTaskCompletedEventAttributes;
import io.temporal.api.history.v1.ActivityTaskFailedEventAttributes;
import io.temporal.api.history.v1.ActivityTaskTimedOutEventAttributes;
import io.temporal.workflow.Functions;

public final class ActivityCommands
    extends CommandsBase<ActivityCommands.State, ActivityCommands.Action, ActivityCommands> {

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

  private static StateMachine<State, Action, ActivityCommands> newStateMachine() {
    return StateMachine.<State, Action, ActivityCommands>newInstance(
            State.CREATED, State.COMPLETED, State.FAILED, State.TIMED_OUT, State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SCHEDULE_COMMAND_CREATED,
            ActivityCommands::createScheduleActivityTaskCommand)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            State.SCHEDULED_EVENT_RECORDED)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            ActivityCommands::cancelScheduleCommand)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityCommands::activityTaskCompleted)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityCommands::activityTaskFailed)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
            State.TIMED_OUT,
            ActivityCommands::activityTaskTimedOut)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            Action.CANCEL,
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            ActivityCommands::createRequestCancelActivityTaskCommand)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
            State.CANCELED,
            ActivityCommands::activityTaskCanceled)
        .add(
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED)
        .add(
            State.STARTED,
            Action.CANCEL,
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            ActivityCommands::createRequestCancelActivityTaskCommand)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityCommands::cancelScheduleCommandCompleteActivity)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityCommands::cancelScheduleCommandFailActivity)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityCommands::activityTaskFailed)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ActivityCommands::activityTaskCompleted)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
            State.CANCELED,
            ActivityCommands::activityTaskCanceled);
  }

  private final ScheduleActivityTaskCommandAttributes scheduleAttr;

  private final Functions.Proc4<
          ActivityTaskCompletedEventAttributes,
          ActivityTaskFailedEventAttributes,
          ActivityTaskTimedOutEventAttributes,
          ActivityTaskCanceledEventAttributes>
      completionCallback;

  public static void newInstance(
      ScheduleActivityTaskCommandAttributes scheduleAttr,
      Functions.Proc4<
              ActivityTaskCompletedEventAttributes,
              ActivityTaskFailedEventAttributes,
              ActivityTaskTimedOutEventAttributes,
              ActivityTaskCanceledEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    new ActivityCommands(scheduleAttr, completionCallback, commandSink);
  }

  private ActivityCommands(
      ScheduleActivityTaskCommandAttributes scheduleAttr,
      Functions.Proc4<
              ActivityTaskCompletedEventAttributes,
              ActivityTaskFailedEventAttributes,
              ActivityTaskTimedOutEventAttributes,
              ActivityTaskCanceledEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.scheduleAttr = scheduleAttr;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  public void createScheduleActivityTaskCommand() {
    addCommand(Command.newBuilder().setScheduleActivityTaskCommandAttributes(scheduleAttr).build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void cancelScheduleCommand() {
    cancelInitialCommand();
    completionCallback.apply(
        null,
        null,
        null,
        ActivityTaskCanceledEventAttributes.newBuilder().setIdentity("workflow").build());
  }

  private void activityTaskCompleted() {
    completionCallback.apply(
        currentEvent.getActivityTaskCompletedEventAttributes(), null, null, null);
  }

  private void activityTaskFailed() {
    completionCallback.apply(null, currentEvent.getActivityTaskFailedEventAttributes(), null, null);
  }

  private void activityTaskTimedOut() {
    completionCallback.apply(
        null, null, currentEvent.getActivityTaskTimedOutEventAttributes(), null);
  }

  private void activityTaskCanceled() {
    completionCallback.apply(
        null, null, null, currentEvent.getActivityTaskCanceledEventAttributes());
  }

  private void cancelScheduleCommandFailActivity() {
    throw new UnsupportedOperationException("unimplemented");
  }

  private void cancelScheduleCommandCompleteActivity() {
    throw new UnsupportedOperationException("unimplemented");
  }

  private void createRequestCancelActivityTaskCommand() {
    addCommand(
        Command.newBuilder()
            .setRequestCancelActivityTaskCommandAttributes(
                RequestCancelActivityTaskCommandAttributes.newBuilder()
                    .setScheduledEventId(getInitialCommandEventId()))
            .build());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
