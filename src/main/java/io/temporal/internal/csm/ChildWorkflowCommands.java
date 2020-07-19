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

public final class ChildWorkflowCommands
    extends CommandsBase<ChildWorkflowCommands.State, ChildWorkflowCommands.Action, ChildWorkflowCommands> {

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
    CANCELED,
    SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
    SCHEDULED_ACTIVITY_CANCEL_EVENT_RECORDED,
    STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
    STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
    COMPLETED_CANCEL_REQUESTED,
    FAILED_CANCEL_REQUESTED,
    CANCELED_CANCEL_REQUESTED,
  }

  private static StateMachine<State, Action, ChildWorkflowCommands> newStateMachine() {
    return StateMachine.<State, Action, ChildWorkflowCommands>newInstance(
            State.CREATED, State.COMPLETED, State.FAILED, State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SCHEDULE_COMMAND_CREATED,
            ChildWorkflowCommands::createScheduleActivityTaskCommand)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            State.SCHEDULED_EVENT_RECORDED)
        .add(
            State.SCHEDULE_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            ChildWorkflowCommands::cancelScheduleCommand)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            State.STARTED)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ChildWorkflowCommands::activityTaskCompleted)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ChildWorkflowCommands::activityTaskFailed)
        .add(
            State.SCHEDULED_EVENT_RECORDED,
            Action.CANCEL,
            State.SCHEDULED_ACTIVITY_CANCEL_COMMAND_CREATED,
            ChildWorkflowCommands::createRequestCancelActivityTaskCommand)
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
            ChildWorkflowCommands::activityTaskCanceled)
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
            ChildWorkflowCommands::createRequestCancelActivityTaskCommand)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ChildWorkflowCommands::cancelScheduleCommandCompleteActivity)
        .add(
            State.STARTED_ACTIVITY_CANCEL_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ChildWorkflowCommands::cancelScheduleCommandFailActivity)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            State.FAILED,
            ChildWorkflowCommands::activityTaskFailed)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            State.COMPLETED,
            ChildWorkflowCommands::activityTaskCompleted)
        .add(
            State.STARTED_ACTIVITY_CANCEL_EVENT_RECORDED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
            State.CANCELED,
            ChildWorkflowCommands::activityTaskCanceled);
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
    new ChildWorkflowCommands(scheduleAttr, completionCallback, commandSink);
  }

  private ChildWorkflowCommands(
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
        ActivityTaskCanceledEventAttributes.newBuilder().setIdentity("workflow").build());
  }

  private void activityTaskCompleted() {
    completionCallback.apply(currentEvent.getActivityTaskCompletedEventAttributes(), null, null);
  }

  private void activityTaskFailed() {
    completionCallback.apply(null, currentEvent.getActivityTaskFailedEventAttributes(), null);
  }

  private void activityTaskCanceled() {
    completionCallback.apply(null, null, currentEvent.getActivityTaskCanceledEventAttributes());
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
