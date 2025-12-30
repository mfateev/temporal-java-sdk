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

package io.temporal.internal.replay;

import io.temporal.api.common.v1.Header;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.internal.statemachines.UpdateProtocolCallback;
import io.temporal.internal.worker.WorkflowImplementationFactory;
import java.util.Optional;

/**
 * Internal interface for workflow execution implementations.
 *
 * <p>This interface defines the contract that workflow execution engines must implement to
 * integrate with the Temporal Java SDK worker infrastructure. It manages the event loop, workflow
 * method execution, and provides a communication interface for workflow operations such as Start,
 * Signal, Query, and Update.
 *
 * <h2>Purpose</h2>
 *
 * <p>Implementations of this interface are responsible for:
 *
 * <ul>
 *   <li>Managing the workflow execution lifecycle (start, event loop, completion)
 *   <li>Handling incoming signals and updates
 *   <li>Processing queries against workflow state
 *   <li>Managing workflow cancellation
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Custom workflow execution models (such as the Kotlin coroutine-based model) implement this
 * interface and are returned by {@link WorkflowImplementationFactory#getWorkflow}. The worker
 * infrastructure calls methods on this interface to drive workflow execution.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Implementations must be prepared for methods to be called from the worker's workflow task
 * processing thread. The event loop and handler methods are called sequentially within a single
 * workflow task, but different workflow tasks may be processed on different threads.
 *
 * @see WorkflowImplementationFactory
 */
public interface ReplayWorkflow {

  /**
   * Starts the workflow execution.
   *
   * <p>Called when a workflow is first started or when replaying from history. The implementation
   * should initialize the workflow state and begin execution of the workflow method.
   *
   * @param event the WorkflowExecutionStarted history event containing workflow input and metadata
   * @param context the replay context providing access to workflow operations and state
   */
  void start(HistoryEvent event, ReplayWorkflowContext context);

  /**
   * Handle an external signal event.
   *
   * <p>Called when a signal is received for the workflow. The implementation should deliver the
   * signal to the appropriate signal handler in the workflow code.
   *
   * @param signalName the name of the signal
   * @param input the signal payload, if any
   * @param eventId the event ID of the signal event in the workflow history
   * @param header the signal header containing metadata
   */
  void handleSignal(String signalName, Optional<Payloads> input, long eventId, Header header);

  /**
   * Handle an update workflow execution event.
   *
   * <p>Called when an update request is received for the workflow. The implementation should
   * validate and execute the update, reporting results through the provided callbacks.
   *
   * @param updateName the name of the update handler
   * @param updateId the unique identifier for this update request
   * @param input the update payload, if any
   * @param eventId the event ID in the workflow history
   * @param header the update header containing metadata
   * @param callbacks callbacks to report update validation and completion results
   */
  void handleUpdate(
      String updateName,
      String updateId,
      Optional<Payloads> input,
      long eventId,
      Header header,
      UpdateProtocolCallback callbacks);

  /**
   * Executes the workflow event loop.
   *
   * <p>Called repeatedly to advance workflow execution. The implementation should process available
   * events and run workflow code until it either completes, fails, or is waiting for more events.
   *
   * @return true if the workflow method execution has finished (completed, failed, or explicitly
   *     exited), false if the workflow is still running and waiting for more events
   */
  boolean eventLoop();

  /**
   * Returns the workflow output if available.
   *
   * <p>Called after the workflow completes to retrieve the result.
   *
   * @return the workflow output payload, or {@link Optional#empty()} if the workflow has not yet
   *     produced output
   */
  Optional<Payloads> getOutput();

  /**
   * Requests cancellation of the workflow.
   *
   * <p>Called when a cancellation request is received for the workflow. The implementation should
   * propagate the cancellation to the workflow code.
   *
   * @param reason the reason for cancellation, if provided
   */
  void cancel(String reason);

  /**
   * Closes and cleans up workflow resources.
   *
   * <p>Called when the workflow execution is being evicted from the cache or the worker is shutting
   * down. The implementation should release any resources held by the workflow.
   */
  void close();

  /**
   * Executes a query against the workflow state.
   *
   * <p>Called after all history is replayed and the workflow cannot make any further progress, when
   * the workflow task is a query task. The implementation should execute the query handler and
   * return the result.
   *
   * @param query the query request containing the query type and arguments
   * @return the query result payload, or {@link Optional#empty()} if the query returns void
   */
  Optional<Payloads> query(WorkflowQuery query);

  /**
   * Returns the workflow context.
   *
   * <p>Provides access to the fullest context of the workflow, which may be needed for certain
   * operations or diagnostics.
   *
   * @return the workflow context
   */
  // TODO we should inverse the control. WorkflowContext should have and expose a reference to
  //  ReplayWorkflow, not the other way around.
  WorkflowContext getWorkflowContext();
}
