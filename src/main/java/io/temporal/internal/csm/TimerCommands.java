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
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerCanceledEventAttributes;
import io.temporal.workflow.Functions;

public final class TimerCommands
    extends CommandsBase<TimerCommands.State, TimerCommands.Action, TimerCommands> {

  private final StartTimerCommandAttributes startAttributes;

  private final Functions.Proc1<HistoryEvent> completionCallback;

  /**
   * Creates a new timer state machine
   *
   * @param attributes timer command attributes
   * @param completionCallback invoked when timer fires or reports cancellation. One of
   *     TimerFiredEvent, TimerCanceledEvent.
   * @return cancellation callback that should be invoked to initiate timer cancellation
   */
  public static TimerCommands newInstance(
      StartTimerCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new TimerCommands(attributes, completionCallback, commandSink);
  }

  private TimerCommands(
      StartTimerCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.startAttributes = attributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
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
            "Timer", State.CREATED, State.FIRED, State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.START_COMMAND_CREATED,
            TimerCommands::createStartTimerCommand)
        .add(
            State.START_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_START_TIMER,
            State.START_COMMAND_CREATED)
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
            TimerCommands::notifyCompletion)
        .add(
            State.START_COMMAND_RECORDED,
            Action.CANCEL,
            State.CANCEL_TIMER_COMMAND_CREATED,
            TimerCommands::createCancelTimerCommand)
        .add(
            State.CANCEL_TIMER_COMMAND_CREATED,
            EventType.EVENT_TYPE_TIMER_CANCELED,
            State.CANCELED,
            TimerCommands::notifyCompletion)
        .add(
            State.CANCEL_TIMER_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_CANCEL_TIMER,
            State.CANCEL_TIMER_COMMAND_CREATED)
        .add(
            State.CANCEL_TIMER_COMMAND_CREATED,
            EventType.EVENT_TYPE_TIMER_FIRED,
            State.FIRED,
            TimerCommands::cancelTimerCommandFireTimer)
        .add(State.FIRED, Action.CANCEL, State.FIRED);
  }

  private void createStartTimerCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_START_TIMER)
            .setStartTimerCommandAttributes(startAttributes)
            .build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void cancelStartTimerCommand() {
    cancelInitialCommand();
    completionCallback.apply(
        HistoryEvent.newBuilder()
            .setEventType(EventType.EVENT_TYPE_TIMER_CANCELED)
            .setTimerCanceledEventAttributes(
                TimerCanceledEventAttributes.newBuilder()
                    .setIdentity("workflow")
                    .setTimerId(startAttributes.getTimerId()))
            .build());
  }

  private void notifyCompletion() {
    completionCallback.apply(currentEvent);
  }

  private void createCancelTimerCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_CANCEL_TIMER)
            .setCancelTimerCommandAttributes(
                CancelTimerCommandAttributes.newBuilder().setTimerId(startAttributes.getTimerId()))
            .build());
  }

  private void cancelTimerCommandFireTimer() {
    cancelInitialCommand();
    notifyCompletion();
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
