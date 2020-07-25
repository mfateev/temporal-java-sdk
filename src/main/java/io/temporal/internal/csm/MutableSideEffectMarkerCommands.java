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

import io.temporal.api.command.v1.RecordMarkerCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.workflow.Functions;
import java.util.Map;
import java.util.Optional;

public final class MutableSideEffectMarkerCommands
    extends CommandsBase<
        MutableSideEffectMarkerCommands.State,
        MutableSideEffectMarkerCommands.Action,
        MutableSideEffectMarkerCommands> {

  private static final String MARKER_HEADER_KEY = "header";
  private static final String MARKER_DATA_KEY = "data";
  private static final String MARKER_ACCESS_COUNT_KEY = "accessCount";
  private static final String MUTABLE_SIDE_EFFECT_MARKER_NAME = "MutableSideEffect";

  private Functions.Proc2<Optional<Payloads>, RuntimeException> callback;
  private Functions.Func1<Optional<Payloads>, Optional<Payloads>> func;

  private Optional<Payloads> result = Optional.empty();
  private int accessCount;

  /**
   * Creates new MutableSideEffect Marker
   *
   * @param commandSink callback to send commands to
   */
  public static void newInstance(Functions.Proc1<NewCommand> commandSink) {
    new MutableSideEffectMarkerCommands(commandSink);
  }

  private MutableSideEffectMarkerCommands(Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
  }

  enum State {
    CREATED,
    MARKER_COMMAND_CREATED,
    MARKER_COMMAND_RECORDED,
    SKIP_MARKER_COMMAND_CREATED,
  }

  private static StateMachine<State, Action, MutableSideEffectMarkerCommands> newStateMachine() {
    return StateMachine.<State, Action, MutableSideEffectMarkerCommands>newInstance(
            "Marker", State.CREATED, State.MARKER_COMMAND_RECORDED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            new State[] {State.MARKER_COMMAND_CREATED, State.SKIP_MARKER_COMMAND_CREATED},
            MutableSideEffectMarkerCommands::createMarkerCommand)
        //        .add(
        //            State.SKIP_MARKER_COMMAND_CREATED,
        //            Action.SKIP_MARKER_COMMAND_REMOVED,
        //            State.CREATED,
        //            MutableSideEffectMarkerCommands::skipMarkerCommandRemoved)
        .add(
            State.MARKER_COMMAND_CREATED,
            EventType.EVENT_TYPE_MARKER_RECORDED,
            State.MARKER_COMMAND_RECORDED);
  }

  /**
   * @param func used to produce side effect value. null if replaying.
   * @param callback returns side effect value or failure
   */
  public void mutableSideEffect(
      Functions.Proc2<Optional<Payloads>, RuntimeException> callback,
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func) {
    this.callback = callback;
    this.func = func;
    accessCount++;
    if (func == null) {
      //      action(A);
    }
  }

  private void createSkipMarkerCommand() {}

  private State createMarkerCommand() {
    RecordMarkerCommandAttributes markerAttributes;
    //    if (func == null) {
    //      // replaying
    //      markerAttributes = RecordMarkerCommandAttributes.getDefaultInstance();
    //    } else {
    //      // executing first time
    //      Optional<Payloads> updated;
    //      try {
    //        updated = func.apply(result);
    //      } catch (RuntimeException e) {
    //        callback.apply(Optional.empty(), e);
    //        return;
    //      }
    //      if (!updated.isPresent()) {
    //        accessCount++;
    //        return;
    //      }
    //      DataConverter dataConverter = DataConverter.getDefaultInstance();
    //      Map<String, Payloads> details = new HashMap<>();
    //      details.put(MARKER_DATA_KEY, result.get());
    //      details.put(MARKER_ACCESS_COUNT_KEY, dataConverter.toPayloads(accessCount).get());
    //      markerAttributes =
    //          RecordMarkerCommandAttributes.newBuilder()
    //              .setMarkerName(MUTABLE_SIDE_EFFECT_MARKER_NAME)
    //              .putAllDetails(details)
    //              .build();
    //      accessCount = 0;
    //    }
    //    addCommand(
    //        Command.newBuilder()
    //            .setCommandType(CommandType.COMMAND_TYPE_RECORD_MARKER)
    //            .setRecordMarkerCommandAttributes(markerAttributes)
    //            .build(),
    //        (event -> handleMakerEvent(event, result)));
    return null;
  }

  private void handleMakerEvent(HistoryEvent event, Optional<Payloads> result) {
    // Event is null when callback is called during initial execution (non replay).
    if (event == null) {
      callback.apply(result, null);
      return;
    }
    MarkerRecordedEventAttributes attributes = event.getMarkerRecordedEventAttributes();
    if (!attributes.getMarkerName().equals(MUTABLE_SIDE_EFFECT_MARKER_NAME)) {
      throw new IllegalStateException(
          "Expected " + MUTABLE_SIDE_EFFECT_MARKER_NAME + ", received: " + attributes);
    }
    Map<String, Payloads> map = attributes.getDetailsMap();
    Optional<Payloads> fromMaker = Optional.ofNullable(map.get(MARKER_DATA_KEY));
    callback.apply(fromMaker, null);
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
