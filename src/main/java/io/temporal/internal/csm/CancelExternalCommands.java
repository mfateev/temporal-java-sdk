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
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.ExternalWorkflowExecutionCancelRequestedEventAttributes;
import io.temporal.api.history.v1.RequestCancelExternalWorkflowExecutionFailedEventAttributes;
import io.temporal.workflow.Functions;

public final class CancelExternalCommands
    extends CommandsBase<
        CancelExternalCommands.State, CancelExternalCommands.Action, CancelExternalCommands> {

  private final RequestCancelExternalWorkflowExecutionCommandAttributes requestCancelAttributes;

  private final Functions.Proc2<
          ExternalWorkflowExecutionCancelRequestedEventAttributes,
          RequestCancelExternalWorkflowExecutionFailedEventAttributes>
      completionCallback;

  public static CancelExternalCommands newInstance(
      RequestCancelExternalWorkflowExecutionCommandAttributes signalAttributes,
      Functions.Proc2<
              ExternalWorkflowExecutionCancelRequestedEventAttributes,
              RequestCancelExternalWorkflowExecutionFailedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    return new CancelExternalCommands(signalAttributes, completionCallback, commandSink);
  }

  private CancelExternalCommands(
      RequestCancelExternalWorkflowExecutionCommandAttributes requestCancelAttributes,
      Functions.Proc2<
              ExternalWorkflowExecutionCancelRequestedEventAttributes,
              RequestCancelExternalWorkflowExecutionFailedEventAttributes>
          completionCallback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.requestCancelAttributes = requestCancelAttributes;
    this.completionCallback = completionCallback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE
  }

  enum State {
    CREATED,
    REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
    REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
    CANCEL_REQUESTED,
    REQUEST_CANCEL_FAILED,
  }

  private static StateMachine<State, Action, CancelExternalCommands> newStateMachine() {
    return StateMachine.<State, Action, CancelExternalCommands>newInstance(
            State.CREATED, State.CANCEL_REQUESTED, State.REQUEST_CANCEL_FAILED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
            CancelExternalCommands::createCancelExternalCommand)
        .add(
            State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
            State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED)
        .add(
            State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
            State.CANCEL_REQUESTED,
            CancelExternalCommands::reportCancelRequested)
        .add(
            State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
            State.REQUEST_CANCEL_FAILED,
            CancelExternalCommands::reportRequestCancelFailed);
  }

  private void createCancelExternalCommand() {
    addCommand(
        Command.newBuilder()
            .setRequestCancelExternalWorkflowExecutionCommandAttributes(requestCancelAttributes)
            .build());
  }

  private void reportCancelRequested() {
    completionCallback.apply(
        currentEvent.getExternalWorkflowExecutionCancelRequestedEventAttributes(), null);
  }

  private void reportRequestCancelFailed() {
    completionCallback.apply(
        null, currentEvent.getRequestCancelExternalWorkflowExecutionFailedEventAttributes());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
