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
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.workflow.Functions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class MutableSideEffectMarkerCommands
    extends CommandsBase<
        MutableSideEffectMarkerCommands.State,
        MutableSideEffectMarkerCommands.Action,
        MutableSideEffectMarkerCommands> {

  private static final String MARKER_HEADER_KEY = "header";
  private static final String MARKER_DATA_KEY = "data";
  private static final String MARKER_ACCESS_COUNT_KEY = "accessCount";
  private static final String MARKER_ID_KEY = "id";
  private static final String MUTABLE_SIDE_EFFECT_MARKER_NAME = "MutableSideEffect";

  private final DataConverter dataConverter = DataConverter.getDefaultInstance();
  private final String id;
  private Functions.Proc1<MutableSideEffectResult> callback;
  private Functions.Func1<Optional<Payloads>, Optional<Payloads>> func;
  private final Functions.Func<Boolean> replaying;

  private MutableSideEffectResult result;

  /**
   * Creates new MutableSideEffect Marker
   *
   * @param result
   * @param commandSink callback to send commands to
   * @return
   */
  public static void newInstance(
      String id,
      Functions.Func<Boolean> replaying,
      MutableSideEffectResult result,
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<MutableSideEffectResult> callback,
      Functions.Proc1<NewCommand> commandSink) {
    new MutableSideEffectMarkerCommands(id, replaying, result, func, callback, commandSink);
  }

  private MutableSideEffectMarkerCommands(
      String id,
      Functions.Func<Boolean> replaying,
      MutableSideEffectResult result,
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<MutableSideEffectResult> callback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    this.id = Objects.requireNonNull(id);
    this.replaying = Objects.requireNonNull(replaying);
    this.result = result;
    this.func = Objects.requireNonNull(func);
    this.callback = Objects.requireNonNull(callback);
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
    NO_MARKER_EVENT
  }

  enum State {
    CREATED,
    MARKER_COMMAND_CREATED,
    MARKER_COMMAND_RECORDED,
    CANCELLED_MARKER_COMMAND_CREATED,
    MARKER_CANCELLED,
  }

  private static StateMachine<State, Action, MutableSideEffectMarkerCommands> newStateMachine() {
    return StateMachine.<State, Action, MutableSideEffectMarkerCommands>newInstance(
            "MutableSideEffect",
            State.CREATED,
            State.MARKER_COMMAND_RECORDED,
            State.MARKER_CANCELLED)
        .add(
            State.CREATED,
            Action.SCHEDULE,
            new State[] {State.MARKER_COMMAND_CREATED, State.CANCELLED_MARKER_COMMAND_CREATED},
            MutableSideEffectMarkerCommands::createMarkerCommand)
        .add(
            State.MARKER_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_RECORD_MARKER,
            State.MARKER_COMMAND_CREATED,
            MutableSideEffectMarkerCommands::notifyResult)
        .add(
            State.CANCELLED_MARKER_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_RECORD_MARKER,
            State.MARKER_CANCELLED,
            MutableSideEffectMarkerCommands::notifyCachedResult)
        .add(
            State.MARKER_COMMAND_CREATED,
            Action.NO_MARKER_EVENT,
            State.MARKER_CANCELLED,
            MutableSideEffectMarkerCommands::notifyCachedResult)
        .add(
            State.MARKER_COMMAND_CREATED,
            EventType.EVENT_TYPE_MARKER_RECORDED,
            State.MARKER_COMMAND_RECORDED,
            MutableSideEffectMarkerCommands::markerResultFromEvent);
  }

  /**
   * Reject event by calling NO_MARKER_EVENT action (which calls cancelInitialCommand during
   * handling). Event is not rejected only if:
   *
   * <ul>
   *   <li>It is marker
   *   <li>It has MutableSideEffect marker name
   *   <li>Its access count matches. Not matching access count means that recorded event is for a
   *       some future mutableSideEffect call.
   * </ul>
   *
   * @param event
   */
  @Override
  public void handleEvent(HistoryEvent event) {
    if (event.getEventType() != EventType.EVENT_TYPE_MARKER_RECORDED
        || !event
            .getMarkerRecordedEventAttributes()
            .getMarkerName()
            .equals(MUTABLE_SIDE_EFFECT_MARKER_NAME)) {
      action(Action.NO_MARKER_EVENT);
      return;
    }
    Map<String, Payloads> detailsMap = event.getMarkerRecordedEventAttributes().getDetailsMap();
    Optional<Payloads> idPayloads = Optional.ofNullable(detailsMap.get(MARKER_ID_KEY));
    String expectedId = dataConverter.fromPayloads(idPayloads, String.class, String.class);
    if (!id.equals(expectedId)) {
      action(Action.NO_MARKER_EVENT);
      return;
    }
    int count =
        dataConverter.fromPayloads(
            Optional.ofNullable(detailsMap.get(MARKER_ACCESS_COUNT_KEY)),
            Integer.class,
            Integer.class);
    if (count > result.getAccessCount()) {
      action(Action.NO_MARKER_EVENT);
      return;
    }
    super.handleEvent(event);
  }

  private State createMarkerCommand() {
    State transitionsTo;
    RecordMarkerCommandAttributes markerAttributes;
    result.incrementAccessCount();
    if (replaying.apply()) {
      // replaying
      markerAttributes = RecordMarkerCommandAttributes.getDefaultInstance();
      transitionsTo = State.MARKER_COMMAND_CREATED;
    } else {
      // executing first time
      Optional<Payloads> updated;
      updated = func.apply(result.getData());
      if (!updated.isPresent()) {
        markerAttributes = RecordMarkerCommandAttributes.getDefaultInstance();
        transitionsTo = State.CANCELLED_MARKER_COMMAND_CREATED;
      } else {
        int accessCount = result.getAccessCount();
        result = new MutableSideEffectResult(updated);
        DataConverter dataConverter = DataConverter.getDefaultInstance();
        Map<String, Payloads> details = new HashMap<>();
        details.put(MARKER_ID_KEY, dataConverter.toPayloads(id).get());
        details.put(MARKER_DATA_KEY, updated.get());
        details.put(MARKER_ACCESS_COUNT_KEY, dataConverter.toPayloads(accessCount).get());
        markerAttributes =
            RecordMarkerCommandAttributes.newBuilder()
                .setMarkerName(MUTABLE_SIDE_EFFECT_MARKER_NAME)
                .putAllDetails(details)
                .build();
        transitionsTo = State.MARKER_COMMAND_CREATED;
      }
    }
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_RECORD_MARKER)
            .setRecordMarkerCommandAttributes(markerAttributes)
            .build());
    if (transitionsTo == State.CANCELLED_MARKER_COMMAND_CREATED) {
      cancelInitialCommand();
    }
    return transitionsTo;
  }

  private void markerResultFromEvent() {
    MarkerRecordedEventAttributes attributes = currentEvent.getMarkerRecordedEventAttributes();
    if (!attributes.getMarkerName().equals(MUTABLE_SIDE_EFFECT_MARKER_NAME)) {
      throw new IllegalStateException(
          "Expected " + MUTABLE_SIDE_EFFECT_MARKER_NAME + ", received: " + attributes);
    }
    Map<String, Payloads> map = attributes.getDetailsMap();
    Optional<Payloads> oid = Optional.ofNullable(map.get(MARKER_ID_KEY));
    String idFromMarker = dataConverter.fromPayloads(oid, String.class, String.class);
    if (!id.equals(idFromMarker)) {
      throw new UnsupportedOperationException(
          "TODO: deal with multiple side effects with different id");
    }
    Optional<Payloads> fromMaker = Optional.ofNullable(map.get(MARKER_DATA_KEY));
    int count =
        dataConverter.fromPayloads(
            Optional.ofNullable(map.get(MARKER_ACCESS_COUNT_KEY)), Integer.class, Integer.class);
    System.out.println(
        "markerResultFromEvent count=" + count + ", accessCount=" + result.getAccessCount());
    callback.apply(new MutableSideEffectResult(fromMaker));
  }

  private void notifyResult() {
    callback.apply(result);
  }

  private void notifyCachedResult() {
    System.out.println("notifyCachedResult  accessCount=" + result.getAccessCount());
    cancelInitialCommand();
    notifyResult();
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
