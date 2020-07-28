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
import io.temporal.api.enums.v1.RetryState;
import io.temporal.api.failure.v1.ActivityFailureInfo;
import io.temporal.api.failure.v1.CanceledFailureInfo;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.api.workflowservice.v1.RespondActivityTaskCanceledRequest;
import io.temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest;
import io.temporal.common.converter.DataConverter;
import io.temporal.failure.FailureConverter;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.workflow.Functions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class LocalActivityCommands
    extends CommandsBase<
        LocalActivityCommands.State, LocalActivityCommands.Action, LocalActivityCommands> {

  static final String LOCAL_ACTIVITY_MARKER_NAME = "LocalActivity";
  static final String MARKER_ACTIVITY_ID_KEY = "activityId";
  private static final String MARKER_TIME_KEY = "time";
  private static final String MARKER_DATA_KEY = "data";

  private final DataConverter dataConverter = DataConverter.getDefaultInstance();

  private final Functions.Proc2<Optional<Payloads>, Failure> callback;
  private final ExecuteLocalActivityParameters localActivityParameters;
  private final Functions.Func<Boolean> replaying;
  /** Accepts proposed current time. Returns accepted current time. */
  private final Functions.Func1<Long, Long> setCurrentTimeCallback;

  private Failure failure;
  private ActivityTaskHandler.Result result;
  private Optional<Payloads> laResult;

  /**
   * Creates new local activity marker
   *
   * @param localActivityParameters used to produce side effect value. null if replaying.
   * @param callback returns side effect value or failure
   * @param commandSink callback to send commands to
   * @return
   */
  public static LocalActivityCommands newInstance(
      Functions.Func<Boolean> replaying,
      Functions.Func1<Long, Long> setCurrentTimeCallback,
      ExecuteLocalActivityParameters localActivityParameters,
      Functions.Proc2<Optional<Payloads>, Failure> callback,
      Functions.Proc1<NewCommand> commandSink) {
    return new LocalActivityCommands(
        replaying, setCurrentTimeCallback, localActivityParameters, callback, commandSink);
  }

  private LocalActivityCommands(
      Functions.Func<Boolean> replaying,
      Functions.Func1<Long, Long> setCurrentTimeCallback,
      ExecuteLocalActivityParameters localActivityParameters,
      Functions.Proc2<Optional<Payloads>, Failure> callback,
      Functions.Proc1<NewCommand> commandSink) {
    super(newStateMachine(), commandSink);
    System.out.println("NEW LocalActivityCommands: " + this);
    this.replaying = replaying;
    this.setCurrentTimeCallback = setCurrentTimeCallback;
    this.localActivityParameters = localActivityParameters;
    this.callback = callback;
    action(Action.SCHEDULE);
  }

  enum Action {
    SCHEDULE,
    GET_REQUEST,
    HANDLE_RESPONSE
  }

  enum State {
    CREATED,
    REQUEST_PREPARED,
    REQUEST_SENT,
    MARKER_COMMAND_CREATED,
    MARKER_COMMAND_RECORDED,
  }

  private static StateMachine<State, Action, LocalActivityCommands> newStateMachine() {
    return StateMachine.<State, Action, LocalActivityCommands>newInstance(
            "LocalActivity", State.CREATED, State.MARKER_COMMAND_RECORDED)
        .add(State.CREATED, Action.SCHEDULE, State.REQUEST_PREPARED)
        .add(State.REQUEST_PREPARED, Action.GET_REQUEST, State.REQUEST_SENT)
        .add(
            State.REQUEST_SENT,
            Action.HANDLE_RESPONSE,
            State.MARKER_COMMAND_CREATED,
            LocalActivityCommands::addMarkerCommand)
        .add(
            State.MARKER_COMMAND_CREATED,
            CommandType.COMMAND_TYPE_RECORD_MARKER,
            State.MARKER_COMMAND_CREATED,
            LocalActivityCommands::notifyResultFromResponse)
        .add(
            State.MARKER_COMMAND_CREATED,
            EventType.EVENT_TYPE_MARKER_RECORDED,
            State.MARKER_COMMAND_RECORDED,
            LocalActivityCommands::empty)
        .add(
            State.REQUEST_PREPARED,
            EventType.EVENT_TYPE_MARKER_RECORDED,
            State.MARKER_COMMAND_RECORDED,
            LocalActivityCommands::notifyResultFromEvent);
  }

  private void empty() {
    System.out.println("Marke recorded no completion");
  }

  public void cancel() {
    throw new UnsupportedOperationException("not implemented yet");
    //    action(Action.CANCEL);
  }

  public ExecuteLocalActivityParameters getRequest() {
    action(Action.GET_REQUEST);
    return localActivityParameters;
  }

  public void handleCompletion(ActivityTaskHandler.Result result) {
    this.result = result;
    action(Action.HANDLE_RESPONSE);
  }

  private void addMarkerCommand() {
    RecordMarkerCommandAttributes.Builder markerAttributes =
        RecordMarkerCommandAttributes.newBuilder();
    Map<String, Payloads> details = new HashMap<>();
    if (!replaying.apply()) {
      markerAttributes.setMarkerName(LOCAL_ACTIVITY_MARKER_NAME);
      Payloads id =
          dataConverter
              .toPayloads(this.localActivityParameters.getActivityTask().getActivityId())
              .get();
      details.put(MARKER_ACTIVITY_ID_KEY, id);
      // TODO(maxim): Consider using elapsed since start instead of Sytem.currentTimeMillis
      long currentTime = setCurrentTimeCallback.apply(System.currentTimeMillis());
      Payloads t = dataConverter.toPayloads(currentTime).get();
      details.put(MARKER_TIME_KEY, t);
      if (result.getTaskCompleted() != null) {
        RespondActivityTaskCompletedRequest completed = result.getTaskCompleted();
        if (completed.hasResult()) {
          Payloads p = completed.getResult();
          laResult = Optional.of(p);
          details.put(MARKER_DATA_KEY, p);
        } else {
          laResult = Optional.empty();
        }
      } else if (result.getTaskFailed() != null) {
        // TODO(maxim): Result should contain Failure, not an exception
        ActivityTaskHandler.Result.TaskFailedResult failed = result.getTaskFailed();
        // TODO(maxim): Return RetryState in the result
        PollActivityTaskQueueResponse.Builder task = localActivityParameters.getActivityTask();
        RetryState retryState =
            task.hasRetryPolicy()
                ? RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
                : RetryState.RETRY_STATE_RETRY_POLICY_NOT_SET;
        failure =
            Failure.newBuilder()
                .setActivityFailureInfo(
                    ActivityFailureInfo.newBuilder()
                        .setRetryState(retryState)
                        .setActivityId(task.getActivityId())
                        .setActivityType(task.getActivityType()))
                .setCause(FailureConverter.exceptionToFailure(failed.getFailure()))
                .build();
        markerAttributes.setFailure(failure);
      } else if (result.getTaskCancelled() != null) {
        RespondActivityTaskCanceledRequest failed = result.getTaskCancelled();
        markerAttributes.setFailure(
            Failure.newBuilder()
                .setCanceledFailureInfo(
                    CanceledFailureInfo.newBuilder().setDetails(failed.getDetails())));
      }
      markerAttributes.putAllDetails(details);
    }
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_RECORD_MARKER)
            .setRecordMarkerCommandAttributes(markerAttributes.build())
            .build());
  }

  private void notifyResultFromEvent() {
    MarkerRecordedEventAttributes attributes = currentEvent.getMarkerRecordedEventAttributes();
    if (!attributes.getMarkerName().equals(LOCAL_ACTIVITY_MARKER_NAME)) {
      throw new IllegalStateException(
          "Expected " + LOCAL_ACTIVITY_MARKER_NAME + ", received: " + attributes);
    }
    Map<String, Payloads> map = attributes.getDetailsMap();
    Optional<Payloads> timePayloads = Optional.ofNullable(map.get(MARKER_TIME_KEY));
    long time = dataConverter.fromPayloads(timePayloads, Long.class, Long.class);
    setCurrentTimeCallback.apply(time);
    if (attributes.hasFailure()) {
      callback.apply(null, attributes.getFailure());
      return;
    }
    Optional<Payloads> fromMaker = Optional.ofNullable(map.get(MARKER_DATA_KEY));
    callback.apply(fromMaker, null);
  }

  private void notifyResultFromResponse() {
    callback.apply(laResult, failure);
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}