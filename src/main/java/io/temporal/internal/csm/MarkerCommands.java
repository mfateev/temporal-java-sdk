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
import io.temporal.api.command.v1.RecordMarkerCommandAttributes;
import io.temporal.api.enums.v1.EventType;
import io.temporal.workflow.Functions;

public final class MarkerCommands
    extends CommandsBase<MarkerCommands.State, MarkerCommands.Action, MarkerCommands> {

  private final RecordMarkerCommandAttributes markerAttributes;

  public static void newInstance(
      RecordMarkerCommandAttributes markerAttributes, Functions.Proc1<NewCommand> commandSink) {
    new MarkerCommands(markerAttributes, commandSink);
  }

  private MarkerCommands(
      RecordMarkerCommandAttributes markerAttributes, Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.markerAttributes = markerAttributes;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE
  }

  enum State {
    CREATED,
    MARKER_COMMAND_CREATED,
    MARKER_COMMAND_RECORDED,
  }

  private static StateMachine<State, Action, MarkerCommands> newStateMachine() {
    return StateMachine.<State, Action, MarkerCommands>newInstance(
            State.CREATED, State.MARKER_COMMAND_RECORDED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            State.MARKER_COMMAND_CREATED,
            MarkerCommands::createMarkerCommand)
        .add(
            State.MARKER_COMMAND_CREATED,
            EventType.EVENT_TYPE_MARKER_RECORDED,
            State.MARKER_COMMAND_RECORDED);
  }

  private void createMarkerCommand() {
    addCommand(Command.newBuilder().setRecordMarkerCommandAttributes(markerAttributes).build());
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
