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
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ExternalWorkflowExecutionSignaledEventAttributes;
import io.temporal.api.history.v1.SignalExternalWorkflowExecutionFailedEventAttributes;
import io.temporal.workflow.Functions;

public final class SignalExternalCommands
    extends CommandsBase<
        SignalExternalCommands.State, SignalExternalCommands.Action, SignalExternalCommands> {

  private final SignalExternalWorkflowExecutionCommandAttributes signalAttributes;

  private final Functions.Proc2<
          ExternalWorkflowExecutionSignaledEventAttributes,
          SignalExternalWorkflowExecutionFailedEventAttributes>
      completionCallback;

  public static void newInstance(
      SignalExternalWorkflowExecutionCommandAttributes signalAttributes,
      Functions.Proc2<
              ExternalWorkflowExecutionSignaledEventAttributes,
              SignalExternalWorkflowExecutionFailedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    new SignalExternalCommands(signalAttributes, completionCallback, commandSink);
  }

  private SignalExternalCommands(
      SignalExternalWorkflowExecutionCommandAttributes signalAttributes,
      Functions.Proc2<
              ExternalWorkflowExecutionSignaledEventAttributes,
              SignalExternalWorkflowExecutionFailedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.signalAttributes = signalAttributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE
  }

  enum State {
    CREATED,
    SIGNAL_EXTERNAL_COMMAND_CREATED,
    SIGNAL_EXTERNAL_COMMAND_RECORDED,
    SIGNALED,
    FAILED,
  }

  private static StateMachine<State, Action, SignalExternalCommands> newStateMachine() {
    return StateMachine.<State, Action, SignalExternalCommands>newInstance(
            State.CREATED, State.SIGNALED, State.FAILED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            SignalExternalCommands::createSignalExternalCommand)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
            State.SIGNALED,
            SignalExternalCommands::reportSignaled)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
            State.FAILED,
            SignalExternalCommands::reportFailed);
  }

  private void createSignalExternalCommand() {
    addCommand(
        Command.newBuilder()
            .setSignalExternalWorkflowExecutionCommandAttributes(signalAttributes)
            .build());
  }

  private void reportSignaled() {
    completionCallback.apply(
        currentEvent.getExternalWorkflowExecutionSignaledEventAttributes(), null);
  }

  private void reportFailed() {
    completionCallback.apply(
        null, currentEvent.getSignalExternalWorkflowExecutionFailedEventAttributes());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
