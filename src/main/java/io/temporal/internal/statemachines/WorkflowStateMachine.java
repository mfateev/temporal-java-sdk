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

import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.CompleteWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.FailWorkflowExecutionCommandAttributes;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.WorkflowExecutionCancelRequestedEventAttributes;
import io.temporal.workflow.Functions;

public final class WorkflowStateMachine
    extends EntityStateMachineInitialCommand<
        WorkflowStateMachine.State, WorkflowStateMachine.Action, WorkflowStateMachine> {

  enum Action {
    COMPLETE,
    FAIL,
    REPORT_CANCELLATION,
  }

  enum State {
    STARTED,
    CANCEL_REQUESTED,
    COMPLETED,
    FAILED,
    CANCELED,
  }

  private static StateMachine<State, Action, WorkflowStateMachine> newStateMachine() {
    return StateMachine.<State, Action, WorkflowStateMachine>newInstance(
            "Workflow", State.STARTED, State.COMPLETED, State.FAILED, State.CANCELED)
        .add(State.STARTED, Action.COMPLETE, State.COMPLETED)
        .add(State.STARTED, Action.FAIL, State.FAILED)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
            State.CANCEL_REQUESTED,
            WorkflowStateMachine::cancelRequested)
        .add(State.CANCEL_REQUESTED, Action.REPORT_CANCELLATION, State.CANCELED)
        .add(State.CANCEL_REQUESTED, Action.COMPLETE, State.COMPLETED)
        .add(State.CANCEL_REQUESTED, Action.FAIL, State.FAILED);
  }

  private final Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes>
      cancellationCallback;

  public static void newInstance(
      Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancellationCallback,
      Functions.Proc1<NewCommand> commandSink) {
    new WorkflowStateMachine(cancellationCallback, commandSink);
  }

  private WorkflowStateMachine(
      Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancellationCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.cancellationCallback = cancellationCallback;
  }

  public void complete(CompleteWorkflowExecutionCommandAttributes attr) {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION)
            .setCompleteWorkflowExecutionCommandAttributes(attr)
            .build());
    action(Action.COMPLETE);
  }

  public void fail(FailWorkflowExecutionCommandAttributes attr) {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION)
            .setFailWorkflowExecutionCommandAttributes(attr)
            .build());
    action(Action.FAIL);
  }

  public void reportCancellation(CancelWorkflowExecutionCommandAttributes attr) {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION)
            .setCancelWorkflowExecutionCommandAttributes(attr)
            .build());
    action(Action.REPORT_CANCELLATION);
  }

  private void cancelRequested() {
    cancellationCallback.apply(currentEvent.getWorkflowExecutionCancelRequestedEventAttributes());
  }
}
