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
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.workflow.Functions;
import java.util.Optional;

public class CommandsBase<State, Action, Data> {

  private final StateMachine<State, Action, Data> stateMachine;

  private final Functions.Proc1<NewCommand> commandSink;

  private NewCommand initialCommand;

  protected HistoryEvent currentEvent;

  public CommandsBase(
      StateMachine<State, Action, Data> stateMachine, Functions.Proc1<NewCommand> commandSink) {
    this.stateMachine = stateMachine;
    this.commandSink = commandSink;
  }

  public void handleEvent(HistoryEvent event) {
    this.currentEvent = event;
    try {
      stateMachine.handleEvent(event.getEventType(), (Data) this);
    } finally {
      this.currentEvent = null;
    }
  }

  public final String toPlantUML() {
    return stateMachine.asPlantUMLStateDiagram();
  }

  protected final void action(Action action) {
    stateMachine.action(action, (Data) this);
  }

  protected final void addCommand(Command command) {
    addCommand(command, null);
  }

  protected final void addCommand(
      Command command, Functions.Proc1<HistoryEvent> matchingEventCallback) {
    if (command.getCommandType() == CommandType.COMMAND_TYPE_UNSPECIFIED) {
      throw new IllegalArgumentException("unspecified command type");
    }
    initialCommand = new NewCommand(command, this, matchingEventCallback);
    commandSink.apply(initialCommand);
  }

  protected final void cancelInitialCommand() {
    initialCommand.cancel();
  }

  protected final long getInitialCommandEventId() {
    Optional<Long> eventId = initialCommand.getInitialCommandEventId();
    if (!eventId.isPresent()) {
      throw new IllegalArgumentException("Initial eventId is not set yet");
    }
    return eventId.get();
  }

  public boolean isFinalState() {
    return stateMachine.isFinalState();
  }

  protected State getState() {
    return stateMachine.getState();
  }
}
