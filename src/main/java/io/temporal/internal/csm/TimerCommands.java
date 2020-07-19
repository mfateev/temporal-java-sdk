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

import io.temporal.api.command.v1.CancelTimerCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.TimerCanceledEventAttributes;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.workflow.Functions;

public final class TimerCommands
    extends CommandsBase<TimerCommands.State, TimerCommands.Action, TimerCommands> {

  private final StartTimerCommandAttributes startAttributes;

  private final Functions.Proc2<TimerFiredEventAttributes, TimerCanceledEventAttributes>
      completionCallback;

  public static void newInstance(
      StartTimerCommandAttributes startAttributes,
      Functions.Proc2<TimerFiredEventAttributes, TimerCanceledEventAttributes> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    new TimerCommands(startAttributes, completionCallback, commandSink);
  }

  private TimerCommands(
      StartTimerCommandAttributes startAttributes,
      Functions.Proc2<TimerFiredEventAttributes, TimerCanceledEventAttributes> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.startAttributes = startAttributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
    START,
    CANCEL
  }

  enum State {
    CREATED,
    START_COMMAND_CREATED,
    START_COMMAND_RECORDED,
    CANCEL_TIMER_COMMAND_CREATED,
    FIRED,
    CANCELED,
  }

  private static StateMachine<State, Action, TimerCommands> newStateMachine() {
    return StateMachine.<State, Action, TimerCommands>newInstance(
            State.CREATED, State.FIRED, State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.START_COMMAND_CREATED,
            TimerCommands::createStartTimerCommand)
        .add(
            State.START_COMMAND_CREATED,
            EventType.EVENT_TYPE_TIMER_STARTED,
            State.START_COMMAND_RECORDED)
        .add(
            State.START_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            TimerCommands::cancelStartTimerCommand)
        .add(
            State.START_COMMAND_RECORDED,
            EventType.EVENT_TYPE_TIMER_FIRED,
            State.FIRED,
            TimerCommands::fireTimer)
        .add(
            State.START_COMMAND_RECORDED,
            Action.CANCEL,
            State.CANCEL_TIMER_COMMAND_CREATED,
            TimerCommands::createCancelTimerCommand)
        .add(
            State.CANCEL_TIMER_COMMAND_CREATED,
            EventType.EVENT_TYPE_TIMER_CANCELED,
            State.CANCELED,
            TimerCommands::timerCanceled)
        .add(
            State.CANCEL_TIMER_COMMAND_CREATED,
            EventType.EVENT_TYPE_TIMER_FIRED,
            State.FIRED,
            TimerCommands::cancelTimerCommandFireTimer);
  }

  private void createStartTimerCommand() {
    addCommand(Command.newBuilder().setStartTimerCommandAttributes(startAttributes).build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void cancelStartTimerCommand() {
    cancelInitialCommand();
    completionCallback.apply(
        null,
        TimerCanceledEventAttributes.newBuilder()
            .setIdentity("workflow")
            .setTimerId(startAttributes.getTimerId())
            .build());
  }

  private void fireTimer() {
    completionCallback.apply(currentEvent.getTimerFiredEventAttributes(), null);
  }

  private void createCancelTimerCommand() {
    addCommand(
        Command.newBuilder()
            .setCancelTimerCommandAttributes(
                CancelTimerCommandAttributes.newBuilder().setTimerId(startAttributes.getTimerId()))
            .build());
  }

  private void timerCanceled() {
    completionCallback.apply(null, currentEvent.getTimerCanceledEventAttributes());
  }

  private void cancelTimerCommandFireTimer() {
    cancelInitialCommand();
    fireTimer();
  }

  public static String asPlantUMLStateDiagram() {
    StringBuilder result = new StringBuilder();
    TimerCommands.newInstance(
        StartTimerCommandAttributes.getDefaultInstance(),
        (f, c) -> {},
        (c) -> {
          c.setInitialCommandEventId(0);
          result.append(c.getCommands().toPlantUML());
        });
    return result.toString();
  }
}
