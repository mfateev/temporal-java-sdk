/*
 * Copyright (C) 2022 Temporal Technologies, Inc. All Rights Reserved.
 *
 * Copyright (C) 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this material except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.internal.statemachines;

import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.api.enums.v1.TimeoutType;
import io.temporal.api.failure.v1.CanceledFailureInfo;
import io.temporal.api.failure.v1.ChildWorkflowExecutionFailureInfo;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.failure.v1.TerminatedFailureInfo;
import io.temporal.api.failure.v1.TimeoutFailureInfo;
import io.temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionCompletedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionFailedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTerminatedEventAttributes;
import io.temporal.api.history.v1.ChildWorkflowExecutionTimedOutEventAttributes;
import io.temporal.api.history.v1.StartChildWorkflowExecutionFailedEventAttributes;
import io.temporal.api.sdk.v1.UserMetadata;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class ChildWorkflowStateMachine
    extends EntityStateMachineInitialCommand<
        ChildWorkflowStateMachine.State,
        ChildWorkflowStateMachine.ExplicitEvent,
        ChildWorkflowStateMachine> {

  private static final String JAVA_SDK = "JavaSDK";
  static final String CHILD_WORKFLOW_FAILED_MESSAGE = "Child workflow execution failed";
  static final String CHILD_WORKFLOW_TIMED_OUT_MESSAGE = "Child workflow execution timed out";
  static final String CHILD_WORKFLOW_CANCELED_MESSAGE = "Child workflow execution canceled";
  static final String CHILD_WORKFLOW_TERMINATED_MESSAGE = "Child workflow execution terminated";
  static final String CHILD_WORKFLOW_START_FAILED_MESSAGE =
      "Child workflow execution failed to start";

  private String workflowType;
  private String namespace;
  private String workflowId;

  enum ExplicitEvent {
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

  public static final StateMachineDefinition<State, ExplicitEvent, ChildWorkflowStateMachine>
      STATE_MACHINE_DEFINITION =
          StateMachineDefinition.<State, ExplicitEvent, ChildWorkflowStateMachine>newInstance(
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
                  ExplicitEvent.SCHEDULE,
                  State.START_COMMAND_CREATED,
                  ChildWorkflowStateMachine::createStartChildCommand)
              .add(
                  State.START_COMMAND_CREATED,
                  CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
                  State.START_COMMAND_CREATED)
              .add(
                  State.START_COMMAND_CREATED,
                  EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
                  State.START_EVENT_RECORDED,
                  EntityStateMachineInitialCommand::setInitialCommandEventId)
              .add(
                  State.START_COMMAND_CREATED,
                  ExplicitEvent.CANCEL,
                  State.CANCELED,
                  ChildWorkflowStateMachine::cancelStartChildCommand)
              .add(
                  State.START_EVENT_RECORDED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
                  State.STARTED,
                  ChildWorkflowStateMachine::notifyStarted)
              .add(
                  State.START_EVENT_RECORDED,
                  EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
                  State.START_FAILED,
                  ChildWorkflowStateMachine::notifyStartFailed)
              .add(
                  State.STARTED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
                  State.COMPLETED,
                  ChildWorkflowStateMachine::notifyCompleted)
              .add(
                  State.STARTED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
                  State.FAILED,
                  ChildWorkflowStateMachine::notifyFailed)
              .add(
                  State.STARTED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
                  State.TIMED_OUT,
                  ChildWorkflowStateMachine::notifyTimedOut)
              .add(
                  State.STARTED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
                  State.CANCELED,
                  ChildWorkflowStateMachine::notifyCanceled)
              .add(
                  State.STARTED,
                  EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
                  State.TERMINATED,
                  ChildWorkflowStateMachine::notifyTerminated);

  private StartChildWorkflowExecutionCommandAttributes startAttributes;

  private UserMetadata metadata;

  private final BiConsumer<WorkflowExecution, Failure> startedCallback;

  private final BiConsumer<Optional<Payloads>, Failure> completionCallback;

  /**
   * Creates a new child workflow state machine
   *
   * @param attributes child workflow start command attributes
   * @param metadata user metadata to be associated with the child workflow
   * @param startedCallback callback that is notified about child start. If failure is non-null, the
   *     child failed to start.
   * @param completionCallback invoked when child reports completion or failure. If failure is
   *     non-null, the child failed.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public static ChildWorkflowStateMachine newInstance(
      StartChildWorkflowExecutionCommandAttributes attributes,
      UserMetadata metadata,
      BiConsumer<WorkflowExecution, Failure> startedCallback,
      BiConsumer<Optional<Payloads>, Failure> completionCallback,
      Consumer<CancellableCommand> commandSink,
      Consumer<StateMachine> stateMachineSink) {
    return new ChildWorkflowStateMachine(
        attributes, metadata, startedCallback, completionCallback, commandSink, stateMachineSink);
  }

  private ChildWorkflowStateMachine(
      StartChildWorkflowExecutionCommandAttributes startAttributes,
      UserMetadata metadata,
      BiConsumer<WorkflowExecution, Failure> startedCallback,
      BiConsumer<Optional<Payloads>, Failure> completionCallback,
      Consumer<CancellableCommand> commandSink,
      Consumer<StateMachine> stateMachineSink) {
    super(STATE_MACHINE_DEFINITION, commandSink, stateMachineSink);
    this.startAttributes = startAttributes;
    this.metadata = metadata;
    this.workflowType = startAttributes.getWorkflowType().getName();
    this.namespace = startAttributes.getNamespace();
    this.workflowId = startAttributes.getWorkflowId();
    this.startedCallback = startedCallback;
    this.completionCallback = completionCallback;
    explicitEvent(ExplicitEvent.SCHEDULE);
  }

  public void createStartChildCommand() {
    Command.Builder command =
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION)
            .setStartChildWorkflowExecutionCommandAttributes(startAttributes);

    if (metadata != null) {
      command.setUserMetadata(metadata);
      metadata = null;
    }
    addCommand(command.build());
    startAttributes = null; // avoiding retaining large input for the duration of the child
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
    if (!isFinalState()) {
      explicitEvent(ExplicitEvent.CANCEL);
    }
  }

  private void cancelStartChildCommand() {
    cancelCommand();
    Failure canceledCause =
        Failure.newBuilder()
            .setSource(JAVA_SDK)
            .setMessage("Child immediately canceled")
            .setCanceledFailureInfo(CanceledFailureInfo.getDefaultInstance())
            .build();
    Failure failure =
        createChildWorkflowFailure(
            0, 0, canceledCause, RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE);
    startedCallback.accept(null, failure);
    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyCompleted() {
    ChildWorkflowExecutionCompletedEventAttributes attributes =
        currentEvent.getChildWorkflowExecutionCompletedEventAttributes();
    Optional<Payloads> result =
        attributes.hasResult() ? Optional.of(attributes.getResult()) : Optional.empty();
    completionCallback.accept(result, null);
  }

  private void notifyStartFailed() {
    StartChildWorkflowExecutionFailedEventAttributes attributes =
        currentEvent.getStartChildWorkflowExecutionFailedEventAttributes();
    // The start failed - create a failure with the cause info
    // TODO should use attributes.startChildWorkflowExecutionFailedCause here and add handling for
    // NAMESPACE_NOT_FOUND
    Failure startFailedCause =
        Failure.newBuilder()
            .setSource(JAVA_SDK)
            .setMessage("Workflow execution already started with ID: " + attributes.getWorkflowId())
            .build();

    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(attributes.getNamespace())
            .setWorkflowExecution(
                WorkflowExecution.newBuilder().setWorkflowId(attributes.getWorkflowId()).build())
            .setWorkflowType(attributes.getWorkflowType())
            .setInitiatedEventId(attributes.getInitiatedEventId())
            .setRetryState(RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE)
            .build();

    Failure failure =
        Failure.newBuilder()
            .setMessage(CHILD_WORKFLOW_START_FAILED_MESSAGE)
            .setChildWorkflowExecutionFailureInfo(failureInfo)
            .setCause(startFailedCause)
            .build();

    startedCallback.accept(null, failure);
    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyFailed() {
    ChildWorkflowExecutionFailedEventAttributes attributes =
        currentEvent.getChildWorkflowExecutionFailedEventAttributes();

    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(attributes.getNamespace())
            .setWorkflowExecution(attributes.getWorkflowExecution())
            .setWorkflowType(attributes.getWorkflowType())
            .setInitiatedEventId(attributes.getInitiatedEventId())
            .setStartedEventId(attributes.getStartedEventId())
            .setRetryState(attributes.getRetryState())
            .build();

    Failure failure =
        Failure.newBuilder()
            .setMessage(CHILD_WORKFLOW_FAILED_MESSAGE)
            .setChildWorkflowExecutionFailureInfo(failureInfo)
            .setCause(attributes.getFailure())
            .build();

    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyTimedOut() {
    ChildWorkflowExecutionTimedOutEventAttributes attributes =
        currentEvent.getChildWorkflowExecutionTimedOutEventAttributes();

    Failure timeoutCause =
        Failure.newBuilder()
            .setSource(JAVA_SDK)
            .setTimeoutFailureInfo(
                TimeoutFailureInfo.newBuilder()
                    .setTimeoutType(TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE)
                    .build())
            .build();

    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(attributes.getNamespace())
            .setWorkflowExecution(attributes.getWorkflowExecution())
            .setWorkflowType(attributes.getWorkflowType())
            .setInitiatedEventId(attributes.getInitiatedEventId())
            .setStartedEventId(attributes.getStartedEventId())
            .setRetryState(attributes.getRetryState())
            .build();

    Failure failure =
        Failure.newBuilder()
            .setMessage(CHILD_WORKFLOW_TIMED_OUT_MESSAGE)
            .setChildWorkflowExecutionFailureInfo(failureInfo)
            .setCause(timeoutCause)
            .build();

    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyCanceled() {
    ChildWorkflowExecutionCanceledEventAttributes attributes =
        currentEvent.getChildWorkflowExecutionCanceledEventAttributes();

    Failure canceledCause =
        Failure.newBuilder()
            .setSource(JAVA_SDK)
            .setMessage("Child canceled")
            .setCanceledFailureInfo(
                CanceledFailureInfo.newBuilder().setDetails(attributes.getDetails()).build())
            .build();

    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(attributes.getNamespace())
            .setWorkflowExecution(attributes.getWorkflowExecution())
            .setWorkflowType(attributes.getWorkflowType())
            .setInitiatedEventId(attributes.getInitiatedEventId())
            .setStartedEventId(attributes.getStartedEventId())
            .setRetryState(RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE)
            .build();

    Failure failure =
        Failure.newBuilder()
            .setMessage(CHILD_WORKFLOW_CANCELED_MESSAGE)
            .setChildWorkflowExecutionFailureInfo(failureInfo)
            .setCause(canceledCause)
            .build();

    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyTerminated() {
    ChildWorkflowExecutionTerminatedEventAttributes attributes =
        currentEvent.getChildWorkflowExecutionTerminatedEventAttributes();

    Failure terminatedCause =
        Failure.newBuilder()
            .setSource(JAVA_SDK)
            .setTerminatedFailureInfo(TerminatedFailureInfo.getDefaultInstance())
            .build();

    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(attributes.getNamespace())
            .setWorkflowExecution(attributes.getWorkflowExecution())
            .setWorkflowType(attributes.getWorkflowType())
            .setInitiatedEventId(attributes.getInitiatedEventId())
            .setStartedEventId(attributes.getStartedEventId())
            .setRetryState(RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE)
            .build();

    Failure failure =
        Failure.newBuilder()
            .setMessage(CHILD_WORKFLOW_TERMINATED_MESSAGE)
            .setChildWorkflowExecutionFailureInfo(failureInfo)
            .setCause(terminatedCause)
            .build();

    completionCallback.accept(Optional.empty(), failure);
  }

  private void notifyStarted() {
    startedCallback.accept(
        currentEvent.getChildWorkflowExecutionStartedEventAttributes().getWorkflowExecution(),
        null);
  }

  private Failure createChildWorkflowFailure(
      long initiatedEventId, long startedEventId, Failure cause, RetryState retryState) {
    ChildWorkflowExecutionFailureInfo failureInfo =
        ChildWorkflowExecutionFailureInfo.newBuilder()
            .setNamespace(namespace)
            .setWorkflowExecution(WorkflowExecution.newBuilder().setWorkflowId(workflowId).build())
            .setWorkflowType(
                io.temporal.api.common.v1.WorkflowType.newBuilder().setName(workflowType).build())
            .setInitiatedEventId(initiatedEventId)
            .setStartedEventId(startedEventId)
            .setRetryState(retryState)
            .build();

    return Failure.newBuilder()
        .setChildWorkflowExecutionFailureInfo(failureInfo)
        .setCause(cause)
        .build();
  }
}
