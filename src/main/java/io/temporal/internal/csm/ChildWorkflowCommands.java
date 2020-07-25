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
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes;
import io.temporal.api.history.v1.HistoryEvent;
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
            "ChildWorkflow",
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
            CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
            State.START_COMMAND_CREATED)
        .add(
            State.START_COMMAND_CREATED,
            EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
            State.START_EVENT_RECORDED)
        .add(
            State.START_COMMAND_CREATED,
            Action.CANCEL,
            State.CANCELED,
            ChildWorkflowCommands::cancelStartChildCommand)
        .add(
            State.START_EVENT_RECORDED,
            EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
            State.STARTED,
            ChildWorkflowCommands::notifyStarted)
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

  private final Functions.Proc1<WorkflowExecution> startedCallback;

  private final Functions.Proc1<HistoryEvent> completionCallback;

  /**
   * Creates a new child workflow state machine
   *
   * @param attributes child workflow start command attributes
   * @param startedCallback
   * @param completionCallback invoked when child reports completion or failure. The following types
   *     of events can be passed to the callback: StartChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionCompletedEvent, ChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionTimedOutEvent, ChildWorkflowExecutionCanceledEvent,
   *     ChildWorkflowExecutionTerminatedEvent.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public static ChildWorkflowCommands newInstance(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<WorkflowExecution> startedCallback,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new ChildWorkflowCommands(attributes, startedCallback, completionCallback, commandSink);
  }

  private ChildWorkflowCommands(
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      Functions.Proc1<WorkflowExecution> startedCallback,
      Functions.Proc1<HistoryEvent> completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.startAttributes = startAttributes;
    this.startedCallback = startedCallback;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  public void createStartChildCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION)
            .setStartChildWorkflowExecutionCommandAttributes(startAttributes)
            .build());
  }

  public boolean isCancellable() {
    return State.START_COMMAND_CREATED == getState();
  }

  /**
   * Cancellation through this class is valid only when start child workflow command is not sent
   * yet. Cancellation of an initiated child workflow is done through CancelExternalCommands. So all
   * of the types besides ABANDON are treated differently.
   */
  public void cancel() {
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

  private void notifyStarted() {
    startedCallback.apply(
        currentEvent.getChildWorkflowExecutionStartedEventAttributes().getWorkflowExecution());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
