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
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionCompletedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTerminatedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTimedOutEventAttributes;
import io.temporal.api.history.v1.StartChildWorkflowExecutionFailedEventAttributes;
import io.temporal.workflow.Functions;

public final class ChildWorkflowCommands
    extends CommandsBase<
        ChildWorkflowCommands.State, ChildWorkflowCommands.Action, ChildWorkflowCommands> {

  enum Action {
    SCHEDULE,
    CANCEL
  }

  enum State {
    CREATED,
    START_COMMAND_CREATED,
    START_EVENT_RECORDED,
    STARTED,
    START_FAILED,
    COMPLETED,
    FAILED,
    CANCELED,
    TIMED_OUT,
    TERMINATED,
  }

  private static StateMachine<State, Action, ChildWorkflowCommands> newStateMachine() {
    return StateMachine.<State, Action, ChildWorkflowCommands>newInstance(
            State.CREATED,
            State.START_FAILED,
            State.COMPLETED,
            State.FAILED,
            State.CANCELED,
            State.TIMED_OUT,
            State.TERMINATED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.START_COMMAND_CREATED,
            ChildWorkflowCommands::createStartChildCommand)
        .add(
            State.START_COMMAND_CREATED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            State.START_EVENT_RECORDED)
        .add(
            State.START_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            ChildWorkflowCommands::cancelStartChildCommand)
        .add(
            State.START_EVENT_RECORDED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
            State.STARTED)
        .add(
            State.START_EVENT_RECORDED,
            EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
            State.START_FAILED,
            ChildWorkflowCommands::startChildWorklfowFailed)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
            State.COMPLETED,
            ChildWorkflowCommands::childWorklfowCompleted)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
            State.FAILED,
            ChildWorkflowCommands::childWorkflowFailed)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
            State.TIMED_OUT,
            ChildWorkflowCommands::childWorkflowTimedOut)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
            State.CANCELED,
            ChildWorkflowCommands::childWorkflowCanceled)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
            State.TERMINATED,
            ChildWorkflowCommands::childWorkflowTerminated);
  }

  private final StartChildWorkflowExecutionCommandAttributes startAttributes;

  private final Functions.Proc6<
          StartChildWorkflowExecutionFailedEventAttributes,
          ChildWorkflowExecutionCompletedEventAttributes,
          ChildWorkflowExecutionFailedEventAttributes,
          ChildWorkflowExecutionTimedOutEventAttributes,
          ChildWorkflowExecutionCanceledEventAttributes,
          ChildWorkflowExecutionTerminatedEventAttributes>
      completionCallback;

  public static ChildWorkflowCommands newInstance(
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      Functions.Proc6<
              StartChildWorkflowExecutionFailedEventAttributes,
              ChildWorkflowExecutionCompletedEventAttributes,
              ChildWorkflowExecutionFailedEventAttributes,
              ChildWorkflowExecutionTimedOutEventAttributes,
              ChildWorkflowExecutionCanceledEventAttributes,
              ChildWorkflowExecutionTerminatedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new ChildWorkflowCommands(startAttributes, completionCallback, commandSink);
  }

  private ChildWorkflowCommands(
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      Functions.Proc6<
              StartChildWorkflowExecutionFailedEventAttributes,
              ChildWorkflowExecutionCompletedEventAttributes,
              ChildWorkflowExecutionFailedEventAttributes,
              ChildWorkflowExecutionTimedOutEventAttributes,
              ChildWorkflowExecutionCanceledEventAttributes,
              ChildWorkflowExecutionTerminatedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.startAttributes = startAttributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  public void createStartChildCommand() {
    addCommand(
        Command.newBuilder()
            .setStartChildWorkflowExecutionCommandAttributes(startAttributes)
            .build());
  }

  public void cancel() {
    action(Action.CANCEL);
  }

  private void cancelStartChildCommand() {
    cancelInitialCommand();
    completionCallback.apply(
        null,
        null,
        null,
        null,
        ChildWorkflowExecutionCanceledEventAttributes.newBuilder()
            .setWorkflowType(startAttributes.getWorkflowType())
            .setNamespace(startAttributes.getNamespace())
            .setWorkflowExecution(
                WorkflowExecution.newBuilder()
                    .setWorkflowId(startAttributes.getWorkflowId())
                    .build())
            .build(),
        null);
  }

  private void startChildWorklfowFailed() {
    completionCallback.apply(
        currentEvent.getStartChildWorkflowExecutionFailedEventAttributes(),
        null,
        null,
        null,
        null,
        null);
  }

  private void childWorklfowCompleted() {
    completionCallback.apply(
        null,
        currentEvent.getChildWorkflowExecutionCompletedEventAttributes(),
        null,
        null,
        null,
        null);
  }

  private void childWorkflowFailed() {
    completionCallback.apply(
        null,
        null,
        currentEvent.getChildWorkflowExecutionFailedEventAttributes(),
        null,
        null,
        null);
  }

  private void childWorkflowTimedOut() {
    completionCallback.apply(
        null,
        null,
        null,
        currentEvent.getChildWorkflowExecutionTimedOutEventAttributes(),
        null,
        null);
  }

  private void childWorkflowCanceled() {
    completionCallback.apply(
        null,
        null,
        null,
        null,
        currentEvent.getChildWorkflowExecutionCanceledEventAttributes(),
        null);
  }

  private void childWorkflowTerminated() {
    completionCallback.apply(
        null,
        null,
        null,
        null,
        null,
        currentEvent.getChildWorkflowExecutionTerminatedEventAttributes());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
