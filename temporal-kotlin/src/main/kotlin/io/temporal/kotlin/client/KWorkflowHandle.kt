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

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.client.WorkflowExecutionDescription
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateException
import io.temporal.kotlin.TemporalDsl
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2

/**
 * Handle for interacting with a workflow execution.
 *
 * Provides methods to signal, query, update, and cancel workflows.
 *
 * @param T the workflow interface type
 */
public interface KWorkflowHandle<T> {

  /**
   * The workflow ID.
   */
  public val workflowId: String

  /**
   * The run ID of the current execution, if known.
   */
  public val runId: String?

  /**
   * The workflow execution containing workflowId and runId.
   */
  public val execution: WorkflowExecution

  /**
   * Sends a signal to the workflow using a method reference.
   *
   * Example:
   * ```kotlin
   * handle.signal(MyWorkflow::updateStatus, "processing")
   * ```
   *
   * @param signal the signal method reference
   */
  public suspend fun signal(signal: KFunction1<T, Unit>)

  /**
   * Sends a signal with one argument to the workflow.
   */
  public suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A)

  /**
   * Sends a signal by name.
   *
   * @param signalName the signal name
   * @param args the signal arguments
   */
  public suspend fun signal(signalName: String, vararg args: Any?)

  /**
   * Queries the workflow using a method reference.
   *
   * Example:
   * ```kotlin
   * val status = handle.query(MyWorkflow::getStatus)
   * ```
   *
   * @param query the query method reference
   * @return the query result
   */
  public suspend fun <R> query(query: KFunction1<T, R>): R

  /**
   * Queries the workflow with one argument.
   */
  public suspend fun <A, R> query(query: KFunction2<T, A, R>, arg: A): R

  /**
   * Queries the workflow by name.
   *
   * @param queryName the query name
   * @param resultClass the expected result type
   * @param args the query arguments
   * @return the query result
   */
  public suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R

  /**
   * Executes an update on the workflow and waits for the result.
   *
   * @param update the update method reference
   * @return the update result
   * @throws WorkflowUpdateException if the update fails
   */
  public suspend fun <R> executeUpdate(update: KFunction1<T, R>): R

  /**
   * Executes an update with one argument and waits for the result.
   */
  public suspend fun <A, R> executeUpdate(update: KFunction2<T, A, R>, arg: A): R

  /**
   * Executes an update by name and waits for the result.
   *
   * @param updateName the update name
   * @param resultClass the expected result type
   * @param args the update arguments
   * @return the update result
   */
  public suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R

  /**
   * Starts an update on the workflow and returns a handle for async result retrieval.
   *
   * @param update the update method reference
   * @return handle for retrieving the update result
   */
  public suspend fun <R> startUpdate(update: KFunction1<T, R>): KUpdateHandle<R>

  /**
   * Starts an update with one argument and returns a handle.
   */
  public suspend fun <A, R> startUpdate(update: KFunction2<T, A, R>, arg: A): KUpdateHandle<R>

  /**
   * Starts an update by name and returns a handle.
   *
   * @param updateName the update name
   * @param resultClass the expected result type
   * @param args the update arguments
   * @return handle for retrieving the update result
   */
  public suspend fun <R> startUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): KUpdateHandle<R>

  /**
   * Gets a handle for an existing update by ID.
   *
   * @param updateId the update ID
   * @param resultClass the expected result type
   * @return handle for retrieving the update result
   */
  public fun <R> getUpdateHandle(updateId: String, resultClass: Class<R>): KUpdateHandle<R>

  /**
   * Requests cancellation of the workflow.
   *
   * This is a request; the workflow may choose to ignore it or perform cleanup.
   */
  public suspend fun cancel()

  /**
   * Terminates the workflow immediately.
   *
   * Unlike cancellation, termination is forceful and immediate.
   *
   * @param reason optional reason for termination
   */
  public suspend fun terminate(reason: String? = null)

  /**
   * Describes the workflow execution.
   *
   * @return detailed information about the workflow execution
   */
  public suspend fun describe(): WorkflowExecutionDescription

  /**
   * Waits for the workflow to complete and returns its result.
   *
   * Use this when the result type is not known at compile time.
   *
   * @param R the expected result type
   * @param resultClass the class of the expected result
   * @return the workflow result
   */
  public suspend fun <R> result(resultClass: Class<R>): R

  /**
   * Returns the underlying WorkflowStub for advanced operations.
   */
  public fun toStub(): WorkflowStub
}

/**
 * Handle for a workflow with a known result type.
 *
 * Extends [KWorkflowHandle] with the ability to await the workflow result.
 *
 * @param T the workflow interface type
 * @param R the result type
 */
public interface KTypedWorkflowHandle<T, R> : KWorkflowHandle<T> {

  /**
   * Waits for the workflow to complete and returns its result.
   *
   * @return the workflow result
   * @throws WorkflowException if the workflow fails
   */
  public suspend fun result(): R

  /**
   * Waits for the workflow to complete with a timeout.
   *
   * @param timeout maximum time to wait
   * @return the workflow result
   * @throws WorkflowException if the workflow fails
   * @throws TimeoutException if the wait times out
   */
  public suspend fun result(timeout: java.time.Duration): R
}

/**
 * Handle for an update operation in progress.
 *
 * @param R the result type of the update
 */
public interface KUpdateHandle<R> {

  /**
   * The update ID.
   */
  public val updateId: String

  /**
   * The workflow execution this update was sent to.
   */
  public val execution: WorkflowExecution

  /**
   * Waits for the update to complete and returns its result.
   *
   * @return the update result
   * @throws WorkflowUpdateException if the update fails
   */
  public suspend fun result(): R

  /**
   * Waits for the update to complete with a timeout.
   *
   * @param timeout maximum time to wait
   * @return the update result
   */
  public suspend fun result(timeout: java.time.Duration): R
}

/**
 * Options for starting a workflow.
 */
@TemporalDsl
public class WorkflowStartOptions private constructor(
  public val workflowId: String,
  public val taskQueue: String,
  public val workflowExecutionTimeout: java.time.Duration?,
  public val workflowRunTimeout: java.time.Duration?,
  public val workflowTaskTimeout: java.time.Duration?,
  public val memo: Map<String, Any>?,
  public val searchAttributes: Map<String, Any>?
) {
  public class Builder {
    public var workflowId: String = ""
    public var taskQueue: String = ""
    public var workflowExecutionTimeout: java.time.Duration? = null
    public var workflowRunTimeout: java.time.Duration? = null
    public var workflowTaskTimeout: java.time.Duration? = null
    public var memo: Map<String, Any>? = null
    public var searchAttributes: Map<String, Any>? = null

    public fun build(): WorkflowStartOptions = WorkflowStartOptions(
      workflowId = workflowId,
      taskQueue = taskQueue,
      workflowExecutionTimeout = workflowExecutionTimeout,
      workflowRunTimeout = workflowRunTimeout,
      workflowTaskTimeout = workflowTaskTimeout,
      memo = memo,
      searchAttributes = searchAttributes
    )
  }

  public companion object {
    public inline operator fun invoke(block: Builder.() -> Unit): WorkflowStartOptions =
      Builder().apply(block).build()
  }
}

/**
 * Untyped handle for interacting with a workflow execution.
 *
 * Use this when you don't know the workflow type at compile time.
 * Operations use string names instead of method references.
 *
 * Example:
 * ```kotlin
 * val handle = client.getUntypedWorkflowHandle("order-123")
 * handle.signal("updatePriority", Priority.HIGH)
 * val status = handle.query<OrderStatus>("status")
 * ```
 */
public interface WorkflowHandle {

  /**
   * The workflow ID.
   */
  public val workflowId: String

  /**
   * The run ID of the current execution, if known.
   */
  public val runId: String?

  /**
   * The workflow execution containing workflowId and runId.
   */
  public val execution: WorkflowExecution

  /**
   * Waits for the workflow to complete and returns its result.
   *
   * @param R the expected result type
   * @param resultClass the class of the expected result
   * @return the workflow result
   */
  public suspend fun <R> result(resultClass: Class<R>): R

  /**
   * Sends a signal to the workflow by name.
   *
   * @param signalName the signal name
   * @param args the signal arguments
   */
  public suspend fun signal(signalName: String, vararg args: Any?)

  /**
   * Queries the workflow by name.
   *
   * @param queryName the query name
   * @param resultClass the expected result type
   * @param args the query arguments
   * @return the query result
   */
  public suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R

  /**
   * Executes an update by name and waits for the result.
   *
   * @param updateName the update name
   * @param resultClass the expected result type
   * @param args the update arguments
   * @return the update result
   */
  public suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R

  /**
   * Requests cancellation of the workflow.
   */
  public suspend fun cancel()

  /**
   * Terminates the workflow immediately.
   *
   * @param reason optional reason for termination
   */
  public suspend fun terminate(reason: String? = null)

  /**
   * Describes the workflow execution.
   *
   * @return detailed information about the workflow execution
   */
  public suspend fun describe(): WorkflowExecutionDescription

  /**
   * Returns the underlying WorkflowStub for advanced operations.
   */
  public fun toStub(): WorkflowStub
}

/**
 * Reified extension for getting the workflow result with type inference.
 */
public suspend inline fun <reified R> WorkflowHandle.result(): R = result(R::class.java)

/**
 * Reified extension for querying the workflow with type inference.
 */
public suspend inline fun <reified R> WorkflowHandle.query(queryName: String, vararg args: Any?): R =
  query(queryName, R::class.java, *args)

/**
 * Reified extension for getting the workflow result with type inference.
 */
public suspend inline fun <reified R> KWorkflowHandle<*>.result(): R = result(R::class.java)

/**
 * Reified extension for querying the workflow with type inference.
 */
public suspend inline fun <reified R, T> KWorkflowHandle<T>.query(queryName: String, vararg args: Any?): R =
  query(queryName, R::class.java, *args)
