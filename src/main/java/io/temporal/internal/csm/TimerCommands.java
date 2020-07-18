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

import io.temporal.workflow.Functions;

public final class TimerCommands
    extends CommandsBase<TimerCommands.State, TimerCommands.Action, TimerCommands> {
  public TimerCommands(
      StateMachine<TimerCommands.State, TimerCommands.Action, TimerCommands> stateMachine,
      Functions.Proc1<NewCommand> commandSink) {
    super(stateMachine, commandSink);
  }

  enum Action {
    START,
    CANCEL
  }

  enum State {
    CREATED,
    START_CREATED,
    // Cancelled without sending start command to the server
    CANCELED_CREATED_TIMER,
    STARTED,
    CANCELED,
  }
  //
  //  private static StateMachine<State, Action, TimerCommands> newStateMachine() {
  //    return StateMachine.<State, Action, TimerCommands>newInstance(
  //            State.CREATED,
  //            State.COMPLETED,
  //            State.FAILED,
  //            State.CANCELED,
  //            State.CANCELED_CREATED_TIMER)
  //        .add(State.CREATED, Action.START, State.START_CREATED,
  // TimerCommands::scheduleActivityTask)
  //        .add(
  //            State.START_CREATED,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
  //            State.SCHEDULED_EVENT_RECORDED)
  //        .add(
  //            State.CREATED,
  //            Action.CANCEL,
  //            State.CANCELED_CREATED_TIMER,
  //            TimerCommands::cancelCreated)
  //        .add(
  //            State.SCHEDULED_EVENT_RECORDED,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
  //            State.STARTED)
  //        .add(
  //            State.STARTED,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
  //            State.COMPLETED,
  //            TimerCommands::activityTaskCompleted)
  //        .add(
  //            State.STARTED,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
  //            State.FAILED,
  //            TimerCommands::activityTaskFailed)
  //        .add(
  //            State.SCHEDULED_EVENT_RECORDED,
  //            Action.CANCEL,
  //            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
  //            TimerCommands::addRequestCancelCommand)
  //        .add(
  //            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
  //            State.CANCELED,
  //            TimerCommands::activityTaskCanceled)
  //        .add(
  //            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
  //            State.COMPLETED,
  //            TimerCommands::activityTaskCompleted)
  //        .add(
  //            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
  //            State.FAILED,
  //            TimerCommands::activityTaskFailed)
  //        .add(
  //            State.CANCEL_REQUESTED_SCHEDULED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
  //            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
  //            TimerCommands::activityTaskFailed)
  //        .add(
  //            State.STARTED,
  //            Action.CANCEL,
  //            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
  //            TimerCommands::addRequestCancelCommand)
  //        .add(
  //            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
  //            State.CANCELED,
  //            TimerCommands::activityTaskCanceled)
  //        .add(
  //            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
  //            State.COMPLETED,
  //            TimerCommands::activityTaskCompleted)
  //        .add(
  //            State.CANCEL_REQUESTED_STARTED_ACTIVITY,
  //            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
  //            State.FAILED,
  //            TimerCommands::activityTaskFailed);
  //  }
  //
  //  private final ScheduleActivityTaskCommandAttributes scheduleAttr;
  //
  //  private final Functions.Proc3<
  //          ActivityTaskCompletedEventAttributes,
  //          ActivityTaskFailedEventAttributes,
  //          ActivityTaskCanceledEventAttributes>
  //      completionCallback;
  //
  //  public static void newInstance(
  //      ScheduleActivityTaskCommandAttributes scheduleAttr,
  //      Functions.Proc3<
  //              ActivityTaskCompletedEventAttributes,
  //              ActivityTaskFailedEventAttributes,
  //              ActivityTaskCanceledEventAttributes>
  //          completionCallback,
  //      Functions.Proc1<NewCommand> commandSink) {
  //    new TimerCommands(scheduleAttr, completionCallback, commandSink);
  //  }
  //
  //  private TimerCommands(
  //      ScheduleActivityTaskCommandAttributes scheduleAttr,
  //      Functions.Proc3<
  //              ActivityTaskCompletedEventAttributes,
  //              ActivityTaskFailedEventAttributes,
  //              ActivityTaskCanceledEventAttributes>
  //          completionCallback,
  //      Functions.Proc1<NewCommand> commandSink) {
  //    super(newStateMachine(), commandSink);
  //    this.scheduleAttr = scheduleAttr;
  //    this.completionCallback = completionCallback;
  //    action(Action.START);
  //  }
  //
  //  public void scheduleActivityTask() {
  //
  // addCommand(Command.newBuilder().setScheduleActivityTaskCommandAttributes(scheduleAttr).build());
  //  }
  //
  //  public void cancel() {
  //    action(Action.CANCEL);
  //  }
  //
  //  private void cancelCreated() {
  //    cancelInitialCommand();
  //    completionCallback.apply(
  //        null,
  //        null,
  //        ActivityTaskCanceledEventAttributes.newBuilder().setIdentity("workflow").build());
  //  }
  //
  //  private void activityTaskCanceled() {
  //    completionCallback.apply(null, null, currentEvent.getActivityTaskCanceledEventAttributes());
  //  }
  //
  //  private void activityTaskFailed() {
  //    completionCallback.apply(null, currentEvent.getActivityTaskFailedEventAttributes(), null);
  //  }
  //
  //  private void activityTaskCompleted() {
  //    completionCallback.apply(currentEvent.getActivityTaskCompletedEventAttributes(), null,
  // null);
  //  }
  //
  //  private void addRequestCancelCommand() {
  //    addCommand(
  //        Command.newBuilder()
  //            .setRequestCancelActivityTaskCommandAttributes(
  //                RequestCancelActivityTaskCommandAttributes.newBuilder()
  //                    .setScheduledEventId(getInitialCommandEventId()))
  //            .build());
  //  }
}
