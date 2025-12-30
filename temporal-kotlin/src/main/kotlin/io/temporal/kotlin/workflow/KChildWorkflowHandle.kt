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

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.workflow.ChildWorkflowStub
import io.temporal.workflow.Promise

/**
 * Handle for an asynchronously executing child workflow.
 *
 * This handle is returned when starting a child workflow without waiting for completion,
 * allowing parallel child workflow execution patterns.
 *
 * Example:
 * ```kotlin
 * val handle1 = KWorkflow.startChildWorkflow<String>("ChildWorkflow1", options, "arg1")
 * val handle2 = KWorkflow.startChildWorkflow<Int>("ChildWorkflow2", options, 42)
 *
 * // Child workflows run in parallel
 * val result1 = handle1.await()
 * val result2 = handle2.await()
 * ```
 *
 * @param R the result type of the child workflow
 */
public interface KChildWorkflowHandle<R> {

  /**
   * Returns `true` if the child workflow has completed.
   *
   * Completion may be due to normal termination, an exception, or cancellation.
   */
  public val isCompleted: Boolean

  /**
   * Waits for the child workflow to complete and returns its result.
   *
   * @return the child workflow result
   * @throws Exception if the child workflow failed
   */
  public suspend fun await(): R

  /**
   * Returns the workflow execution information once the child workflow has started.
   *
   * Note: This may block until the child workflow has actually started.
   *
   * @return the workflow execution containing workflowId and runId
   */
  public suspend fun getExecution(): WorkflowExecution

  /**
   * Sends a signal to the child workflow.
   *
   * @param signalName the name of the signal handler
   * @param args arguments to pass to the signal handler
   */
  public fun signal(signalName: String, vararg args: Any?)
}

/**
 * Internal implementation of [KChildWorkflowHandle] that wraps a [Promise].
 */
internal class PromiseChildWorkflowHandle<R>(
  private val promise: Promise<R>,
  private val stub: ChildWorkflowStub
) : KChildWorkflowHandle<R> {

  override val isCompleted: Boolean
    get() = promise.isCompleted

  override suspend fun await(): R = promise.await()

  override suspend fun getExecution(): WorkflowExecution {
    return stub.execution.await()
  }

  override fun signal(signalName: String, vararg args: Any?) {
    stub.signal(signalName, *args)
  }
}
