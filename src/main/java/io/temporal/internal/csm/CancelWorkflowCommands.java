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
import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.workflow.Functions;

public final class CancelWorkflowCommands
    extends CommandsBase<
        CancelWorkflowCommands.State, CancelWorkflowCommands.Action, CancelWorkflowCommands> {

  private final CancelWorkflowExecutionCommandAttributes cancelWorkflowAttributes;

  public static void newInstance(
      CancelWorkflowExecutionCommandAttributes cancelWorkflowAttributes,
      Functions.Proc1<NewCommand> commandSink) {
    new CancelWorkflowCommands(cancelWorkflowAttributes, commandSink);
  }

  private CancelWorkflowCommands(
      CancelWorkflowExecutionCommandAttributes cancelWorkflowAttributes,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.cancelWorkflowAttributes = cancelWorkflowAttributes;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE
  }

  enum State {
    CREATED,
    CANCEL_WORKFLOW_COMMAND_CREATED,
    CANCEL_WORKFLOW_COMMAND_RECORDED,
  }

  private static StateMachine<State, Action, CancelWorkflowCommands> newStateMachine() {
    return StateMachine.<State, Action, CancelWorkflowCommands>newInstance(
            State.CREATED, State.CANCEL_WORKFLOW_COMMAND_RECORDED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.CANCEL_WORKFLOW_COMMAND_CREATED,
            CancelWorkflowCommands::createCancelWorkflowCommand)
        .add(
            State.CANCEL_WORKFLOW_COMMAND_CREATED,
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
            State.CANCEL_WORKFLOW_COMMAND_RECORDED);
  }

  private void createCancelWorkflowCommand() {
    addCommand(
        Command.newBuilder()
            .setCancelWorkflowExecutionCommandAttributes(cancelWorkflowAttributes)
            .build());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
