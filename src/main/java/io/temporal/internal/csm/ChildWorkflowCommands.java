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
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.workflow.ChildWorkflowCancellationType;
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
            ChildWorkflowCommands::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
            State.COMPLETED,
            ChildWorkflowCommands::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
            State.FAILED,
            ChildWorkflowCommands::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
            State.TIMED_OUT,
            ChildWorkflowCommands::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
            State.CANCELED,
            ChildWorkflowCommands::notifyCompletion)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
            State.TERMINATED,
            ChildWorkflowCommands::notifyCompletion);
  }

  private final StartChildWorkflowExecutionCommandAttributes startAttributes;

  private final Functions.Proc1<HistoryEvent> completionCallback;

  /**
   * Creates a new child workflow state machine
   *
   * @param attributes child workflow start command attributes
   * @param completionCallback invoked when child reports completion or failure. The following types
   *     of events can be passed to the callback: StartChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionCompletedEvent, ChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionTimedOutEvent, ChildWorkflowExecutionCanceledEvent,
   *     ChildWorkflowExecutionTerminatedEvent.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public static ChildWorkflowCommands newInstance(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new ChildWorkflowCommands(attributes, completionCallback, commandSink);
  }

  private ChildWorkflowCommands(
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      Functions.Proc1<HistoryEvent> completionCallback,
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

  public void cancel(ChildWorkflowCancellationType cancellationType) {
    // TODO: Cancellation type
    action(Action.CANCEL);
  }

  private void cancelStartChildCommand() {
    cancelInitialCommand();
    completionCallback.apply(
        HistoryEvent.newBuilder()
            .setEventType(EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)
            .setChildWorkflowExecutionCanceledEventAttributes(
                ChildWorkflowExecutionCanceledEventAttributes.newBuilder()
                    .setWorkflowType(startAttributes.getWorkflowType())
                    .setNamespace(startAttributes.getNamespace())
                    .setWorkflowExecution(
                        WorkflowExecution.newBuilder()
                            .setWorkflowId(startAttributes.getWorkflowId())))
            .build());
  }

  private void notifyCompletion() {
    completionCallback.apply(currentEvent);
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
