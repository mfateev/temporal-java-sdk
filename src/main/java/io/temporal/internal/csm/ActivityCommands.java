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
    // Cancelled without sending schedule command to the server
    CANCELED_CREATED_ACTIVITY,
    SCHEDULED_EVENT_RECORDED,
    STARTED,
    COMPLETED,
    FAILED,
    CANCELED,
    CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
    SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
    CANCEL_REQUESTED_STARTED_ACTIVITY,
    STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
    COMPLETED_CANCEL_REQUESTED,
    FAILED_CANCEL_REQUESTED,
    CANCELED_CANCEL_REQUESTED,
  }

  private static StateMachine<State, Action, ActivityCommands> newStateMachine() {
    return StateMachine.<State, Action, ActivityCommands>newInstance(
            State.CREATED,
            State.COMPLETED,
            State.FAILED,
            State.CANCELED,
            State.CANCELED_CREATED_ACTIVITY)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SCHEDULE_COMMAND_CREATED,
            ActivityCommands::scheduleActivityTask)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            State.SCHEDULED_EVENT_RECORDED)
        .add(
            State.CREATED,
            Action.CANCEL,
            State.CANCELED_CREATED_ACTIVITY,
            ActivityCommands::cancelCreated)
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
            State.SCHEDULED_EVENT_RECORDED,
            Action.CANCEL,
            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
            ActivityCommands::addRequestCancelCommand)
        .add(
            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED)
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
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ActivityCommands::activityTaskFailed)
        .add(
            State.STARTED,
            Action.CANCEL,
            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
            ActivityCommands::addRequestCancelCommand)
        .add(
            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED_CANCEL_REQUESTED,
            ActivityCommands::activityTaskCompleted)
        .add(
            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED_CANCEL_REQUESTED,
            ActivityCommands::activityTaskFailed)
        .add(
            State.COMPLETED_CANCEL_REQUESTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.COMPLETED)
        .add(
            State.FAILED_CANCEL_REQUESTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
            State.FAILED);
  }

  private final ScheduleActivityTaskCommandAttributes scheduleAttr;

  private final Functions.Proc3<
          ActivityTaskCompletedEventAttributes,
          ActivityTaskFailedEventAttributes,
          ActivityTaskCanceledEventAttributes>
      completionCallback;

  public static void newInstance(
      ScheduleActivityTaskCommandAttributes scheduleAttr,
      Functions.Proc3<
              ActivityTaskCompletedEventAttributes,
              ActivityTaskFailedEventAttributes,
              ActivityTaskCanceledEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    new ActivityCommands(scheduleAttr, completionCallback, commandSink);
  }

  private ActivityCommands(
      ScheduleActivityTaskCommandAttributes scheduleAttr,
      Functions.Proc3<
              ActivityTaskCompletedEventAttributes,
              ActivityTaskFailedEventAttributes,
              ActivityTaskCanceledEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.scheduleAttr = scheduleAttr;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  public void scheduleActivityTask() {
    addCommand(Command.newBuilder().setScheduleActivityTaskCommandAttributes(scheduleAttr).build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void cancelCreated() {
    cancelInitialCommand();
    completionCallback.apply(
        null,
        null,
        ActivityTaskCanceledEventAttributes.newBuilder().setIdentity("workflow").build());
  }

  private void activityTaskCanceled() {
    completionCallback.apply(null, null, currentEvent.getActivityTaskCanceledEventAttributes());
  }

  private void activityTaskFailed() {
    completionCallback.apply(null, currentEvent.getActivityTaskFailedEventAttributes(), null);
  }

  private void activityTaskCompleted() {
    completionCallback.apply(currentEvent.getActivityTaskCompletedEventAttributes(), null, null);
  }

  private void addRequestCancelCommand() {
    addCommand(
        Command.newBuilder()
            .setRequestCancelActivityTaskCommandAttributes(
                RequestCancelActivityTaskCommandAttributes.newBuilder()
                    .setScheduledEventId(getInitialCommandEventId()))
            .build());
  }
}
