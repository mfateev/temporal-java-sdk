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

package io.temporal.internal.statemachines;

import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.SignalExternalWorkflowExecutionFailedEventAttributes;
import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.Functions;
import io.temporal.workflow.SignalExternalWorkflowException;

final class SignalExternalStateMachine
    extends EntityStateMachineInitialCommand<
        SignalExternalStateMachine.State,
        SignalExternalStateMachine.Action,
        SignalExternalStateMachine> {

  private final SignalExternalWorkflowExecutionCommandAttributes signalAttributes;

  private final Functions.Proc2<Void, Exception> completionCallback;

  /**
   * Register new instance of the signal commands
   *
   * @param signalAttributes attributes used to signal an external workflow
   * @param completionCallback either SignalExternalWorkflowExecutionFailed,
   *     ExternalWorkflowExecutionSignaled or true value of the second parameter to indicate
   *     immediate cancellation.
   * @param commandSink sink to send commands
   * @return cancellation handler
   */
  public static Functions.Proc newInstance(
      SignalExternalWorkflowExecutionCommandAttributes signalAttributes,
      Functions.Proc2<Void, Exception> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    SignalExternalStateMachine commands =
        new SignalExternalStateMachine(signalAttributes, completionCallback, commandSink);
    return commands::cancel;
  }

  private SignalExternalStateMachine(
      SignalExternalWorkflowExecutionCommandAttributes signalAttributes,
      Functions.Proc2<Void, Exception> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.signalAttributes = signalAttributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
    CANCEL
  }

  enum State {
    CREATED,
    SIGNAL_EXTERNAL_COMMAND_CREATED,
    SIGNAL_EXTERNAL_COMMAND_RECORDED,
    SIGNALED,
    FAILED,
    CANCELED,
  }

  private static StateMachine<State, Action, SignalExternalStateMachine> newStateMachine() {
    return StateMachine.<State, Action, SignalExternalStateMachine>newInstance(
            "SignalExternal", State.CREATED, State.SIGNALED, State.FAILED, State.CANCELED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            SignalExternalStateMachine::createSignalExternalCommand)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            SignalExternalStateMachine::cancelSignalExternalCommand)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
            State.SIGNAL_EXTERNAL_COMMAND_CREATED)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_CREATED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
            State.SIGNALED,
            SignalExternalStateMachine::notifyCompleted)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED,
            Action.CANCEL,
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED)
        .add(
            State.SIGNAL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
            State.FAILED,
            SignalExternalStateMachine::notifyFailed);
  }

  private void createSignalExternalCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION)
            .setSignalExternalWorkflowExecutionCommandAttributes(signalAttributes)
            .build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void notifyCompleted() {
    completionCallback.apply(null, null);
  }

  private void notifyFailed() {
    SignalExternalWorkflowExecutionFailedEventAttributes attributes =
        currentEvent.getSignalExternalWorkflowExecutionFailedEventAttributes();
    WorkflowExecution signaledExecution =
        WorkflowExecution.newBuilder()
            .setWorkflowId(attributes.getWorkflowExecution().getWorkflowId())
            .setRunId(attributes.getWorkflowExecution().getRunId())
            .build();
    RuntimeException failure = new SignalExternalWorkflowException(signaledExecution, null);
    completionCallback.apply(null, failure);
  }

  private void cancelSignalExternalCommand() {
    cancelInitialCommand();
    CanceledFailure failure = new CanceledFailure("Signal external workflow execution canceled");
    completionCallback.apply(null, failure);
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
