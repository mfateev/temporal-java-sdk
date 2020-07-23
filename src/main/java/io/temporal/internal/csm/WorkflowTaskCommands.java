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

import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.WorkflowTaskFailedCause;
import io.temporal.api.history.v1.WorkflowTaskFailedEventAttributes;
import io.temporal.workflow.Functions;
import java.util.concurrent.TimeUnit;

public final class WorkflowTaskCommands
    extends CommandsBase<
        WorkflowTaskCommands.State, WorkflowTaskCommands.Action, WorkflowTaskCommands> {

  private final Functions.Proc1<Long> timeCallback;

  private final Functions.Proc1<String> runIdCallback;

  private final long workflowTaskStartedEventId;
  private final Functions.Proc eventLoopCallback;

  private long currentTimeMillis;

  public static WorkflowTaskCommands newInstance(
      long workflowTaskStartedEventId,
      Functions.Proc eventLoopCallback,
      Functions.Proc1<Long> timeCallback,
      Functions.Proc1<String> runIdCallback) {
    return new WorkflowTaskCommands(
        workflowTaskStartedEventId, eventLoopCallback, timeCallback, runIdCallback);
  }

  private WorkflowTaskCommands(
      long workflowTaskStartedEventId,
      Functions.Proc eventLoopCallback,
      Functions.Proc1<Long> timeCallback,
      Functions.Proc1<String> runIdCallback) {
    super(
        newStateMachine(),
        (c) -> {
          throw new UnsupportedOperationException("doesn't generate commands");
        });
    this.workflowTaskStartedEventId = workflowTaskStartedEventId;
    this.eventLoopCallback = eventLoopCallback;
    this.timeCallback = timeCallback;
    this.runIdCallback = runIdCallback;
  }

  enum Action {}

  enum State {
    SCHEDULED,
    STARTED,
    COMPLETED,
    TIMED_OUT,
    FAILED,
  }

  private static StateMachine<State, Action, WorkflowTaskCommands> newStateMachine() {
    return StateMachine.<State, Action, WorkflowTaskCommands>newInstance(
            "WorkflowTask", State.SCHEDULED, State.COMPLETED, State.TIMED_OUT, State.FAILED)
        .add(
            State.SCHEDULED,
            EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
            State.STARTED,
            WorkflowTaskCommands::updateCurrentTime)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
            State.COMPLETED,
            WorkflowTaskCommands::runEventLoop)
        .add(
            State.STARTED,
            EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
            State.FAILED,
            WorkflowTaskCommands::mayBeUpdateCurrentRunId)
        .add(State.STARTED, EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, State.TIMED_OUT);
  }

  private void updateCurrentTime() {
    currentTimeMillis = TimeUnit.NANOSECONDS.toMillis(currentEvent.getTimestamp());
    // The last started event in the history
    if (currentEvent.getEventId() == workflowTaskStartedEventId) {
      runEventLoop();
    } else if (currentEvent.getEventId() > workflowTaskStartedEventId) {
      throw new IllegalStateException("Unexpected history event: " + currentEvent.getEventId());
    }
  }

  /** Only update current time if a decision task has completed successfully. */
  private void runEventLoop() {
    timeCallback.apply(currentTimeMillis);
    eventLoopCallback.apply();
  }

  private void mayBeUpdateCurrentRunId() {
    // Reset creates a new run of a workflow. The tricky part is that that the replay
    // of the reset workflow has to use the original runId up to the reset point to
    // maintain the same results. This code resets the id to the new one after the reset to
    // ensure that the new random and UUID are generated form this point.
    WorkflowTaskFailedEventAttributes attr = currentEvent.getWorkflowTaskFailedEventAttributes();
    if (attr.getCause() == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW) {
      this.runIdCallback.apply(attr.getNewRunId());
    }
  }

  public static String asPlantUMLStateDiagram() {
    return newStateMachine().asPlantUMLStateDiagram();
  }
}
