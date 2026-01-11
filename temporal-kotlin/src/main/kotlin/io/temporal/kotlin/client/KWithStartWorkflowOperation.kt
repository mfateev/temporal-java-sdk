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

package io.temporal.kotlin.client

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.kotlin.internal.InternalTemporalApi
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Represents a workflow start operation for use with update-with-start.
 *
 * Created via [KWorkflowClient.newWithStartWorkflowOperation].
 * This class captures the workflow method, arguments, and options for atomic execution
 * with an update operation.
 *
 * Example:
 * ```kotlin
 * val startOp = client.newWithStartWorkflowOperation(
 *     OrderWorkflow::processOrder,
 *     KWorkflowOptions(
 *         workflowId = "order-123",
 *         taskQueue = "orders",
 *         workflowIdConflictPolicy = WorkflowIdConflictPolicy.USE_EXISTING
 *     ),
 *     order
 * )
 *
 * val updateResult = client.executeUpdateWithStart(
 *     OrderWorkflow::addItem,
 *     KUpdateWithStartOptions(startWorkflowOperation = startOp),
 *     newItem
 * )
 *
 * // Access workflow result after operation completes
 * val workflowResult = startOp.getResult()
 * ```
 *
 * @param T the workflow interface type
 * @param R the workflow result type
 */
public class KWithStartWorkflowOperation<T, R> @InternalTemporalApi internal constructor(
  internal val workflowType: String,
  internal val workflowClass: Class<T>,
  internal val resultClass: Class<R>,
  internal val options: WorkflowOptions,
  internal val args: Array<out Any?>
) {
  private val invoked = AtomicBoolean(false)
  private val stubRef = AtomicReference<WorkflowStub?>()

  /**
   * Get the workflow result after the operation completes.
   *
   * @return the result of the workflow
   * @throws IllegalStateException if the operation has not been executed yet
   */
  public fun getResult(): R {
    val stub = stubRef.get()
      ?: throw IllegalStateException("WithStartWorkflowOperation has not been executed yet")
    return stub.getResult(resultClass)
  }

  /**
   * Gets the workflow stub after the operation has been executed.
   *
   * @return the workflow stub
   * @throws IllegalStateException if the operation has not been executed yet
   */
  internal fun getStub(): WorkflowStub {
    return stubRef.get()
      ?: throw IllegalStateException("WithStartWorkflowOperation has not been executed yet")
  }

  /**
   * Mark the operation as having been invoked.
   *
   * @return false if the operation was already invoked
   */
  internal fun markInvoked(): Boolean {
    return invoked.compareAndSet(false, true)
  }

  /**
   * Creates and stores the workflow stub for this operation.
   * This is called internally when executing the update-with-start operation.
   */
  @InternalTemporalApi
  internal fun createStub(workflowClient: WorkflowClient): WorkflowStub {
    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options)
    stubRef.set(stub)
    return stub
  }

  override fun toString(): String {
    return "KWithStartWorkflowOperation{workflowType='$workflowType', args=${args.contentToString()}, resultClass=$resultClass}"
  }
}
