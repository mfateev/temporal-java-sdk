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
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateHandle
import io.temporal.kotlin.TemporalDsl
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.UpdateMethod
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.jvm.javaMethod

// TODO: Switch from Dispatchers.IO + blocking Java SDK calls to fully async implementation
//  using gRPC async client. This will eliminate thread pool overhead and provide true
//  non-blocking suspension.

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
public open class WorkflowHandle(
  @PublishedApi internal val stub: WorkflowStub
) {

  /**
   * The workflow ID.
   */
  public val workflowId: String
    get() = stub.execution?.workflowId ?: throw IllegalStateException("Workflow not yet started")

  /**
   * The run ID of the current execution, if known.
   */
  public val runId: String?
    get() = stub.execution?.runId?.takeIf { it.isNotEmpty() }

  /**
   * The workflow execution containing workflowId and runId.
   */
  public val execution: WorkflowExecution
    get() = stub.execution ?: throw IllegalStateException("Workflow not yet started")

  /**
   * Sends a signal to the workflow by name.
   *
   * @param signalName the signal name
   * @param args the signal arguments
   */
  public suspend fun signal(signalName: String, vararg args: Any?) {
    withContext(Dispatchers.IO) {
      stub.signal(signalName, *args)
    }
  }

  /**
   * Queries the workflow by name with reified type inference.
   *
   * @param queryName the query name
   * @param args the query arguments
   * @return the query result
   */
  public suspend inline fun <reified R> query(queryName: String, vararg args: Any?): R =
    query(queryName, R::class.java, *args)

  /**
   * Queries the workflow by name.
   *
   * @param queryName the query name
   * @param resultClass the expected result type
   * @param args the query arguments
   * @return the query result
   */
  public suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.query(queryName, resultClass, *args)
    }
  }

  /**
   * Executes an update by name with reified type inference and waits for the result.
   *
   * @param updateName the update name
   * @param args the update arguments
   * @return the update result
   */
  public suspend inline fun <reified R> executeUpdate(updateName: String, vararg args: Any?): R =
    executeUpdate(updateName, R::class.java, *args)

  /**
   * Executes an update by name and waits for the result.
   *
   * @param updateName the update name
   * @param resultClass the expected result type
   * @param args the update arguments
   * @return the update result
   */
  public suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.update(updateName, resultClass, *args)
    }
  }

  /**
   * Requests cancellation of the workflow.
   */
  public suspend fun cancel() {
    withContext(Dispatchers.IO) {
      stub.cancel()
    }
  }

  /**
   * Terminates the workflow immediately.
   *
   * @param reason optional reason for termination
   */
  public suspend fun terminate(reason: String? = null) {
    withContext(Dispatchers.IO) {
      stub.terminate(reason)
    }
  }

  /**
   * Describes the workflow execution.
   *
   * @return detailed information about the workflow execution
   */
  public suspend fun describe(): KWorkflowExecutionDescription {
    return withContext(Dispatchers.IO) {
      KWorkflowExecutionDescription(stub.describe())
    }
  }

  /**
   * Waits for the workflow to complete and returns its result with reified type inference.
   *
   * Use this when the result type is known at compile time but you're using an untyped handle.
   * For typed handles ([KTypedWorkflowHandle]), use [KTypedWorkflowHandle.result] instead.
   *
   * @return the workflow result
   */
  public suspend inline fun <reified R> getResult(): R = getResult(R::class.java)

  /**
   * Waits for the workflow to complete and returns its result.
   *
   * Use this when the result type is known at compile time but you're using an untyped handle.
   * For typed handles ([KTypedWorkflowHandle]), use [KTypedWorkflowHandle.result] instead.
   *
   * @param R the expected result type
   * @param resultClass the class of the expected result
   * @return the workflow result
   */
  public suspend fun <R> getResult(resultClass: Class<R>): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(resultClass)
    }
  }

  /**
   * Gets a handle for an existing update by ID with reified type inference.
   *
   * @param updateId the update ID
   * @return handle for retrieving the update result
   */
  public inline fun <reified R> getUpdateHandle(updateId: String): KUpdateHandle<R> =
    getUpdateHandle(updateId, R::class.java)

  /**
   * Gets a handle for an existing update by ID.
   *
   * @param updateId the update ID
   * @param resultClass the expected result type
   * @return handle for retrieving the update result
   */
  public fun <R> getUpdateHandle(updateId: String, resultClass: Class<R>): KUpdateHandle<R> {
    val handle = stub.getUpdateHandle(updateId, resultClass)
    return KUpdateHandle(handle)
  }

  /**
   * Returns the underlying WorkflowStub for advanced operations.
   */
  public fun toStub(): WorkflowStub = stub
}

/**
 * Handle for interacting with a workflow execution.
 *
 * Provides methods to signal, query, update, and cancel workflows
 * using type-safe method references.
 *
 * @param T the workflow interface type
 */
public open class KWorkflowHandle<T>(
  stub: WorkflowStub,
  @PublishedApi internal val workflowInterface: Class<T>
) : WorkflowHandle(stub) {

  /**
   * Sends a signal to the workflow using a method reference.
   *
   * Example:
   * ```kotlin
   * handle.signal(MyWorkflow::updateStatus)
   * ```
   *
   * @param signal the signal method reference
   */
  public suspend fun signal(signal: KFunction1<T, Unit>) {
    val signalName = extractSignalName(signal)
    signal(signalName)
  }

  /**
   * Sends a signal with one argument to the workflow.
   */
  public suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A) {
    val signalName = extractSignalName(signal)
    signal(signalName, arg)
  }

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
  public suspend fun <R> query(query: KFunction1<T, R>): R {
    val (queryName, resultClass) = extractQueryMetadata(query)
    @Suppress("UNCHECKED_CAST")
    return query(queryName, resultClass as Class<R>)
  }

  /**
   * Queries the workflow with one argument.
   */
  public suspend fun <A, R> query(query: KFunction2<T, A, R>, arg: A): R {
    val (queryName, resultClass) = extractQueryMetadata(query)
    @Suppress("UNCHECKED_CAST")
    return query(queryName, resultClass as Class<R>, arg)
  }

  /**
   * Executes an update on the workflow and waits for the result.
   *
   * @param update the update method reference
   * @return the update result
   */
  public suspend fun <R> executeUpdate(update: KFunction1<T, R>): R {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdate(updateName, resultClass as Class<R>)
  }

  /**
   * Executes an update with one argument and waits for the result.
   */
  public suspend fun <A, R> executeUpdate(update: KFunction2<T, A, R>, arg: A): R {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdate(updateName, resultClass as Class<R>, arg)
  }

  private fun extractSignalName(signal: KFunction<*>): String {
    val javaMethod = signal.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve signal method reference")

    val signalMethod = javaMethod.getAnnotation(SignalMethod::class.java)
    return if (signalMethod != null && signalMethod.name.isNotEmpty()) {
      signalMethod.name
    } else {
      javaMethod.name
    }
  }

  private fun extractQueryMetadata(query: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = query.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve query method reference")

    val queryMethod = javaMethod.getAnnotation(QueryMethod::class.java)
    val queryName = if (queryMethod != null && queryMethod.name.isNotEmpty()) {
      queryMethod.name
    } else {
      javaMethod.name
    }

    return Pair(queryName, javaMethod.returnType)
  }

  private fun extractUpdateMetadata(update: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = update.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve update method reference")

    val updateMethod = javaMethod.getAnnotation(UpdateMethod::class.java)
    val updateName = if (updateMethod != null && updateMethod.name.isNotEmpty()) {
      updateMethod.name
    } else {
      javaMethod.name
    }

    return Pair(updateName, javaMethod.returnType)
  }
}

/**
 * Handle for a workflow with a known result type.
 *
 * Extends [KWorkflowHandle] with the ability to await the workflow result.
 *
 * @param T the workflow interface type
 * @param R the result type
 */
public class KTypedWorkflowHandle<T, R>(
  stub: WorkflowStub,
  workflowInterface: Class<T>,
  @PublishedApi internal val resultClass: Class<R>
) : KWorkflowHandle<T>(stub, workflowInterface) {

  /**
   * Waits for the workflow to complete and returns its result.
   *
   * @return the workflow result
   */
  public suspend fun result(): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(resultClass)
    }
  }

  /**
   * Waits for the workflow to complete with a timeout.
   *
   * @param timeout maximum time to wait
   * @return the workflow result
   */
  public suspend fun result(timeout: Duration): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(timeout.toMillis(), TimeUnit.MILLISECONDS, resultClass)
    }
  }
}

/**
 * Handle for an update operation in progress.
 *
 * @param R the result type of the update
 */
public class KUpdateHandle<R>(
  @PublishedApi internal val delegate: WorkflowUpdateHandle<R>
) {

  /**
   * The update ID.
   */
  public val updateId: String get() = delegate.id

  /**
   * The workflow execution this update was sent to.
   */
  public val execution: WorkflowExecution get() = delegate.execution

  /**
   * Waits for the update to complete and returns its result.
   *
   * @return the update result
   */
  public suspend fun result(): R {
    return delegate.resultAsync.await()
  }

  /**
   * Waits for the update to complete with a timeout.
   *
   * @param timeout maximum time to wait
   * @return the update result
   */
  public suspend fun result(timeout: Duration): R {
    return kotlinx.coroutines.withTimeout(timeout.toMillis()) {
      delegate.resultAsync.await()
    }
  }
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
