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

package io.temporal.kotlin.workflow

import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2

/**
 * Handle to a child workflow execution for sending signals and awaiting results.
 *
 * Use [KWorkflow.startChildWorkflow] to obtain a handle, or [KWorkflow.getChildWorkflowHandle]
 * to get a handle to an existing child workflow.
 *
 * Example:
 * ```kotlin
 * val handle = KWorkflow.startChildWorkflow(
 *   ChildWorkflow::processOrder,
 *   ChildWorkflowOptions { workflowId = "child-123" },
 *   order
 * )
 *
 * // Send a signal to the child
 * handle.signal(ChildWorkflow::updatePriority, Priority.HIGH)
 *
 * // Wait for the result
 * val result = handle.result()
 * ```
 *
 * @param T the child workflow interface type
 * @param R the result type of the child workflow
 */
public interface KChildWorkflowHandle<T, R> {

  /**
   * The workflow ID of the child workflow.
   */
  public val workflowId: String

  /**
   * The run ID of the first execution of this child workflow.
   * This remains constant across continue-as-new.
   */
  public val firstExecutionRunId: String

  /**
   * Waits for the child workflow to complete and returns the result.
   *
   * @return the result of the child workflow
   * @throws ChildWorkflowException if the child workflow fails
   */
  public suspend fun result(): R

  /**
   * Sends a signal to the child workflow using a method reference.
   *
   * Example:
   * ```kotlin
   * handle.signal(ChildWorkflow::updateStatus, "processing")
   * ```
   *
   * @param signal the signal method reference
   */
  public suspend fun signal(signal: KFunction1<T, Unit>)

  /**
   * Sends a signal with one argument to the child workflow.
   *
   * @param signal the signal method reference
   * @param arg the signal argument
   */
  public suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A)

  /**
   * Sends a signal by name to the child workflow.
   *
   * @param signalName the name of the signal
   * @param args the signal arguments
   */
  public suspend fun signal(signalName: String, vararg args: Any?)

  /**
   * Requests cancellation of the child workflow.
   *
   * This is a request; the child workflow may choose to ignore it
   * or perform cleanup before terminating.
   */
  public suspend fun cancel()
}
