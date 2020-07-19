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
import io.temporal.api.command.v1.CompleteWorkflowExecutionCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.workflow.Functions;

public final class CompleteWorkflowCommands
    extends CommandsBase<
        CompleteWorkflowCommands.State, CompleteWorkflowCommands.Action, CompleteWorkflowCommands> {

  private final CompleteWorkflowExecutionCommandAttributes completeWorkflowAttributes;

  public static void newInstance(
      CompleteWorkflowExecutionCommandAttributes completeWorkflowAttributes,
      Functions.Proc1<NewCommand> commandSink) {
    new CompleteWorkflowCommands(completeWorkflowAttributes, commandSink);
  }

  private CompleteWorkflowCommands(
      CompleteWorkflowExecutionCommandAttributes completeWorkflowAttributes,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.completeWorkflowAttributes = completeWorkflowAttributes;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE
  }

  enum State {
    CREATED,
    COMPLETE_WORKFLOW_COMMAND_CREATED,
    COMPLETE_WORKFLOW_COMMAND_RECORDED,
  }

  private static StateMachine<State, Action, CompleteWorkflowCommands> newStateMachine() {
    return StateMachine.<State, Action, CompleteWorkflowCommands>newInstance(
            State.CREATED, State.COMPLETE_WORKFLOW_COMMAND_RECORDED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.COMPLETE_WORKFLOW_COMMAND_CREATED,
            CompleteWorkflowCommands::createCompleteWorkflowCommand)
        .add(
            State.COMPLETE_WORKFLOW_COMMAND_CREATED,
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            State.COMPLETE_WORKFLOW_COMMAND_RECORDED);
  }

  private void createCompleteWorkflowCommand() {
    addCommand(
        Command.newBuilder()
            .setCompleteWorkflowExecutionCommandAttributes(completeWorkflowAttributes)
            .build());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
