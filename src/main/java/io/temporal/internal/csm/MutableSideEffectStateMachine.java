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
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

public final class MutableSideEffectStateMachine implements EntityStateMachine {

  private static final String MARKER_HEADER_KEY = "header";
  private static final String MARKER_DATA_KEY = "data";
  private static final String MARKER_SKIP_COUNT_KEY = "skipCount";
  private static final String MARKER_ID_KEY = "id";
  private static final String MUTABLE_SIDE_EFFECT_MARKER_NAME = "MutableSideEffect";

  private final DataConverter dataConverter = DataConverter.getDefaultInstance();
  private final String id;
  private final Functions.Func<Boolean> replaying;
  private final Functions.Proc1<NewCommand> commandSink;

  private Functions.Proc1<Optional<Payloads>> currentCallback;
  private Functions.Func1<Optional<Payloads>, Optional<Payloads>> currentFunc;

  private Queue<NewCommand> newCommands = new ArrayDeque<>();
  private Optional<Payloads> result = Optional.empty();

  private int currentSkipCount;

  private int skipCountFromMarker = Integer.MAX_VALUE;

  /**
   * Creates new MutableSideEffect Marker
   *
   * @return
   */
  public static MutableSideEffectStateMachine newInstance(
      String id, Functions.Func<Boolean> replaying, Functions.Proc1<NewCommand> commandSink) {
    return new MutableSideEffectStateMachine(id, replaying, commandSink);
  }

  private MutableSideEffectStateMachine(
      String id, Functions.Func<Boolean> replaying, Functions.Proc1<NewCommand> commandSink) {
    this.id = Objects.requireNonNull(id);
    this.replaying = Objects.requireNonNull(replaying);
    this.commandSink = Objects.requireNonNull(commandSink);
  }

  public void mutableSideEffect(
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<Optional<Payloads>> callback) {
    this.currentFunc = Objects.requireNonNull(func);
    this.currentCallback = Objects.requireNonNull(callback);
    if (replaying.apply()) {
      createFakeMarkerCommand();
    } else {
      createMarkerCommand();
    }
  }

  @Override
  public void handleCommand(CommandType commandType) {
    if (replaying.apply()) {
      return;
    }
    NewCommand command = newCommands.poll();
    if (command == null) {
      throw new IllegalStateException("Unexpected handleCommand call");
    }
    if (!command.isCanceled()) {
      RecordMarkerCommandAttributes attributes =
          command.getCommand().getRecordMarkerCommandAttributes();
      Payloads payloads = attributes.getDetailsOrDefault(MARKER_DATA_KEY, null);
      result = Optional.ofNullable(payloads);
    }
    currentCallback.apply(result);
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
      notifyCachedResult();
      return;
    }
    Map<String, Payloads> detailsMap = event.getMarkerRecordedEventAttributes().getDetailsMap();
    Optional<Payloads> idPayloads = Optional.ofNullable(detailsMap.get(MARKER_ID_KEY));
    String expectedId = dataConverter.fromPayloads(0, idPayloads, String.class, String.class);
    if (!id.equals(expectedId)) {
      notifyCachedResult();
      return;
    }
    int count =
        dataConverter.fromPayloads(
            0,
            Optional.ofNullable(detailsMap.get(MARKER_SKIP_COUNT_KEY)),
            Integer.class,
            Integer.class);
    skipCountFromMarker = count;
    notifyCachedResult();
    markerResultFromEvent(event);
  }

  private <T> void notifyCachedResult() {
    while (true) {
      if (++currentSkipCount > skipCountFromMarker) {
        skipCountFromMarker = Integer.MAX_VALUE;
        break;
      }
      NewCommand command = newCommands.poll();
      if (command == null) {
        return;
      }
      command.cancel();
      currentCallback.apply(result);
    }
  }

  @Override
  public boolean isFinalState() {
    // Always true to avoid storing in EntityManager.stateMachines
    return true;
  }

  private void createFakeMarkerCommand() {
    RecordMarkerCommandAttributes markerAttributes =
        RecordMarkerCommandAttributes.getDefaultInstance();
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_RECORD_MARKER)
            .setRecordMarkerCommandAttributes(markerAttributes)
            .build(),
        false);
  }

  private void createMarkerCommand() {
    RecordMarkerCommandAttributes markerAttributes;
    boolean cancelled = false;
    Optional<Payloads> updated;
    updated = currentFunc.apply(result);
    if (!updated.isPresent()) {
      markerAttributes = RecordMarkerCommandAttributes.getDefaultInstance();
      cancelled = true;
      currentSkipCount++;
    } else {
      result = updated;
      DataConverter dataConverter = DataConverter.getDefaultInstance();
      Map<String, Payloads> details = new HashMap<>();
      details.put(MARKER_ID_KEY, dataConverter.toPayloads(id).get());
      details.put(MARKER_DATA_KEY, updated.get());
      details.put(MARKER_SKIP_COUNT_KEY, dataConverter.toPayloads(currentSkipCount).get());
      markerAttributes =
          RecordMarkerCommandAttributes.newBuilder()
              .setMarkerName(MUTABLE_SIDE_EFFECT_MARKER_NAME)
              .putAllDetails(details)
              .build();
      currentSkipCount = 0;
    }
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_RECORD_MARKER)
            .setRecordMarkerCommandAttributes(markerAttributes)
            .build(),
        cancelled);
  }

  private void markerResultFromEvent(HistoryEvent event) {
    MarkerRecordedEventAttributes attributes = event.getMarkerRecordedEventAttributes();
    if (!attributes.getMarkerName().equals(MUTABLE_SIDE_EFFECT_MARKER_NAME)) {
      throw new IllegalStateException(
          "Expected " + MUTABLE_SIDE_EFFECT_MARKER_NAME + ", received: " + attributes);
    }
    Map<String, Payloads> map = attributes.getDetailsMap();
    Optional<Payloads> oid = Optional.ofNullable(map.get(MARKER_ID_KEY));
    String idFromMarker = dataConverter.fromPayloads(0, oid, String.class, String.class);
    if (!id.equals(idFromMarker)) {
      throw new UnsupportedOperationException(
          "TODO: deal with multiple side effects with different id");
    }
    currentSkipCount = 0;
    newCommands.poll();
    result = Optional.ofNullable(map.get(MARKER_DATA_KEY));
    currentCallback.apply(result);
  }

  private void notifyResult() {
    currentCallback.apply(result);
  }

  protected final void addCommand(Command command, boolean cancelled) {
    if (command.getCommandType() != CommandType.COMMAND_TYPE_RECORD_MARKER) {
      throw new IllegalArgumentException("invalid command type");
    }
    NewCommand newCommand = new NewCommand(command, this);
    if (cancelled) {
      newCommand.cancel();
    }
    newCommands.add(newCommand);
    commandSink.apply(newCommand);
  }
}
