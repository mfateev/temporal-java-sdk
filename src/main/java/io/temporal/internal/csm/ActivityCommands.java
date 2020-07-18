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
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.workflow.Functions;

public class ActivityCommands {

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
    CANCEL_REQUESTED_STARTED_ACTIVITY;
  }

  private final StateMachine<State, Action> stateMachine =
      StateMachine.newInstance(
              State.CREATED,
              State.COMPLETED,
              State.FAILED,
              State.CANCELED,
              State.CANCELED_CREATED_ACTIVITY)
          .add(
              State.CREATED,
              Action.SCHEDULE,
              State.SCHEDULE_COMMAND_CREATED,
              this::scheduleActivityTask)
          .add(
              State.SCHEDULE_COMMAND_CREATED,
              EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
              State.SCHEDULED_EVENT_RECORDED)
          .add(State.CREATED, Action.CANCEL, State.CANCELED_CREATED_ACTIVITY, this::cancelCreated)
          .add(
              State.SCHEDULED_EVENT_RECORDED,
              EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
              State.STARTED)
          .add(
              State.STARTED,
              EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              State.COMPLETED,
              this::activityTaskCompleted)
          .add(
              State.STARTED,
              EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
              State.FAILED,
              this::activityTaskFailed)
          .add(
              State.SCHEDULED_EVENT_RECORDED,
              Action.CANCEL,
              State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
              this::addRequestCancelCommand)
          .add(
              State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
              State.CANCELED,
              this::activityTaskCanceled)
          .add(
              State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              State.COMPLETED,
              this::activityTaskCompleted)
          .add(
              State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
              State.FAILED,
              this::activityTaskFailed)
          .add(
              State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
              State.CANCEL_REQUESTED_STARTED_ACTIVITY,
              this::activityTaskFailed)
          .add(
              State.STARTED,
              Action.CANCEL,
              State.CANCEL_REQUESTED_STARTED_ACTIVITY,
              this::addRequestCancelCommand)
          .add(
              State.CANCEL_REQUESTED_STARTED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
              State.CANCELED,
              this::activityTaskCanceled)
          .add(
              State.CANCEL_REQUESTED_STARTED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              State.COMPLETED,
              this::activityTaskCompleted)
          .add(
              State.CANCEL_REQUESTED_STARTED_ACTIVITY,
              EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
              State.FAILED,
              this::activityTaskFailed);

  private final ScheduleActivityTaskCommandAttributes scheduleAttr;

  private final Functions.Proc3<
          ActivityTaskCompletedEventAttributes,
          ActivityTaskFailedEventAttributes,
          ActivityTaskCanceledEventAttributes>
      completionCallback;

  private final Functions.Proc1<NewCommand> commandSink;

  private HistoryEvent currentEvent;

  private NewCommand scheduleCommand;

  public ActivityCommands(
      ScheduleActivityTaskCommandAttributes scheduleAttr,
      Functions.Proc3<
              ActivityTaskCompletedEventAttributes,
              ActivityTaskFailedEventAttributes,
              ActivityTaskCanceledEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    this.scheduleAttr = scheduleAttr;
    this.completionCallback = completionCallback;
    this.commandSink = commandSink;
    stateMachine.action(Action.SCHEDULE);
  }

  void scheduleActivityTask() {
    scheduleCommand =
        new NewCommand(
            Command.newBuilder().setScheduleActivityTaskCommandAttributes(scheduleAttr).build());
    commandSink.apply(scheduleCommand);
  }

  private void cancelCreated() {
    scheduleCommand.cancel();
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
    NewCommand cancelCommand =
        new NewCommand(
            Command.newBuilder()
                .setRequestCancelActivityTaskCommandAttributes(
                    RequestCancelActivityTaskCommandAttributes.newBuilder()
                        .setScheduledEventId(scheduleCommand.getInitialCommandEventId()))
                .build());
    commandSink.apply(cancelCommand);
  }

  void handleEvent(HistoryEvent event) {
    this.currentEvent = event;
    try {
      stateMachine.handleEvent(event.getEventType());
    } finally {
      this.currentEvent = null;
    }
  }

  void cancel() {
    stateMachine.action(Action.CANCEL);
  }

  public String toPlantUML() {
    return stateMachine.toPlantUML();
  }
}
