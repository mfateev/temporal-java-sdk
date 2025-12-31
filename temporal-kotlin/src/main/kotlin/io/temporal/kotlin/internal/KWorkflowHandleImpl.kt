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

package io.temporal.kotlin.internal

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.client.UpdateOptions
import io.temporal.client.WorkflowExecutionDescription
import io.temporal.client.WorkflowStub
import io.temporal.kotlin.client.KTypedWorkflowHandle
import io.temporal.kotlin.client.KUpdateHandle
import io.temporal.kotlin.client.KWorkflowHandle
import io.temporal.kotlin.client.WorkflowHandle
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

/**
 * Internal implementation of [KWorkflowHandle].
 */
@InternalTemporalApi
internal class KWorkflowHandleImpl<T>(
  private val stub: WorkflowStub,
  private val workflowClass: Class<T>
) : KWorkflowHandle<T> {

  override val workflowId: String
    get() = stub.execution?.workflowId ?: throw IllegalStateException("Workflow not yet started")

  override val runId: String?
    get() = stub.execution?.runId?.takeIf { it.isNotEmpty() }

  override val execution: WorkflowExecution
    get() = stub.execution ?: throw IllegalStateException("Workflow not yet started")

  override suspend fun signal(signal: KFunction1<T, Unit>) {
    val signalName = extractSignalName(signal)
    signal(signalName)
  }

  override suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A) {
    val signalName = extractSignalName(signal)
    signal(signalName, arg)
  }

  override suspend fun signal(signalName: String, vararg args: Any?) {
    withContext(Dispatchers.IO) {
      stub.signal(signalName, *args)
    }
  }

  override suspend fun <R> query(query: KFunction1<T, R>): R {
    val (queryName, resultClass) = extractQueryMetadata(query)
    @Suppress("UNCHECKED_CAST")
    return query(queryName, resultClass as Class<R>)
  }

  override suspend fun <A, R> query(query: KFunction2<T, A, R>, arg: A): R {
    val (queryName, resultClass) = extractQueryMetadata(query)
    @Suppress("UNCHECKED_CAST")
    return query(queryName, resultClass as Class<R>, arg)
  }

  override suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.query(queryName, resultClass, *args)
    }
  }

  override suspend fun <R> executeUpdate(update: KFunction1<T, R>): R {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdate(updateName, resultClass as Class<R>)
  }

  override suspend fun <A, R> executeUpdate(update: KFunction2<T, A, R>, arg: A): R {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdate(updateName, resultClass as Class<R>, arg)
  }

  override suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.update(updateName, resultClass, *args)
    }
  }

  override suspend fun <R> startUpdate(update: KFunction1<T, R>): KUpdateHandle<R> {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdate(updateName, resultClass as Class<R>)
  }

  override suspend fun <A, R> startUpdate(update: KFunction2<T, A, R>, arg: A): KUpdateHandle<R> {
    val (updateName, resultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdate(updateName, resultClass as Class<R>, arg)
  }

  override suspend fun <R> startUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): KUpdateHandle<R> {
    return withContext(Dispatchers.IO) {
      val options = UpdateOptions.newBuilder(resultClass)
        .setUpdateName(updateName)
        .build()
      val handle = stub.startUpdate(options, *args)
      KUpdateHandleImpl(handle.execution, handle.id, resultClass) {
        handle.resultAsync.await()
      }
    }
  }

  override fun <R> getUpdateHandle(updateId: String, resultClass: Class<R>): KUpdateHandle<R> {
    val handle = stub.getUpdateHandle(updateId, resultClass)
    return KUpdateHandleImpl(handle.execution, handle.id, resultClass) {
      handle.resultAsync.await()
    }
  }

  override suspend fun cancel() {
    withContext(Dispatchers.IO) {
      stub.cancel()
    }
  }

  override suspend fun terminate(reason: String?) {
    withContext(Dispatchers.IO) {
      stub.terminate(reason)
    }
  }

  override suspend fun describe(): WorkflowExecutionDescription {
    return withContext(Dispatchers.IO) {
      stub.describe()
    }
  }

  override suspend fun <R> result(resultClass: Class<R>): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(resultClass)
    }
  }

  override fun toStub(): WorkflowStub = stub

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
 * Internal implementation of [KTypedWorkflowHandle].
 */
@InternalTemporalApi
internal class KTypedWorkflowHandleImpl<T, R>(
  private val stub: WorkflowStub,
  private val resultClass: Class<R>
) : KTypedWorkflowHandle<T, R>, KWorkflowHandle<T> by KWorkflowHandleImpl(stub, Any::class.java as Class<T>) {

  override suspend fun result(): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(resultClass)
    }
  }

  override suspend fun result(timeout: Duration): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(timeout.toMillis(), TimeUnit.MILLISECONDS, resultClass)
    }
  }

  override fun toStub(): WorkflowStub = stub
}

/**
 * Internal implementation of [WorkflowHandle] (untyped).
 */
@InternalTemporalApi
internal class WorkflowHandleImpl(
  private val stub: WorkflowStub
) : WorkflowHandle {

  override val workflowId: String
    get() = stub.execution?.workflowId ?: throw IllegalStateException("Workflow not yet started")

  override val runId: String?
    get() = stub.execution?.runId?.takeIf { it.isNotEmpty() }

  override val execution: WorkflowExecution
    get() = stub.execution ?: throw IllegalStateException("Workflow not yet started")

  override suspend fun <R> result(resultClass: Class<R>): R {
    return withContext(Dispatchers.IO) {
      stub.getResult(resultClass)
    }
  }

  override suspend fun signal(signalName: String, vararg args: Any?) {
    withContext(Dispatchers.IO) {
      stub.signal(signalName, *args)
    }
  }

  override suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.query(queryName, resultClass, *args)
    }
  }

  override suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R {
    return withContext(Dispatchers.IO) {
      stub.update(updateName, resultClass, *args)
    }
  }

  override suspend fun cancel() {
    withContext(Dispatchers.IO) {
      stub.cancel()
    }
  }

  override suspend fun terminate(reason: String?) {
    withContext(Dispatchers.IO) {
      stub.terminate(reason)
    }
  }

  override suspend fun describe(): WorkflowExecutionDescription {
    return withContext(Dispatchers.IO) {
      stub.describe()
    }
  }

  override fun toStub(): WorkflowStub = stub
}

/**
 * Internal implementation of [KUpdateHandle].
 */
@InternalTemporalApi
internal class KUpdateHandleImpl<R>(
  override val execution: WorkflowExecution,
  override val updateId: String,
  private val resultClass: Class<R>,
  private val resultProvider: suspend () -> R
) : KUpdateHandle<R> {

  override suspend fun result(): R = resultProvider()

  override suspend fun result(timeout: Duration): R {
    // Note: The timeout is handled at the provider level for now
    // A more sophisticated implementation would use withTimeout
    return kotlinx.coroutines.withTimeout(timeout.toMillis()) {
      resultProvider()
    }
  }
}
