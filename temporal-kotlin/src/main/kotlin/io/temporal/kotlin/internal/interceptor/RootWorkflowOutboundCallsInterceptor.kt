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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.internal.interceptor

import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes
import io.temporal.api.common.v1.Payloads
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.interceptor.KActivityInvocationInput
import io.temporal.kotlin.interceptor.KCancelWorkflowInput
import io.temporal.kotlin.interceptor.KChildWorkflowInvocationInput
import io.temporal.kotlin.interceptor.KContinueAsNewInput
import io.temporal.kotlin.interceptor.KLocalActivityInvocationInput
import io.temporal.kotlin.interceptor.KSignalExternalInput
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.kotlin.workflow.KChildWorkflowHandle
import io.temporal.kotlin.workflow.KChildWorkflowOptions
import io.temporal.kotlin.workflow.KContinueAsNewOptions
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.ContinueAsNewOptions
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.Optional
import java.util.Random
import java.util.UUID
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Root outbound calls interceptor that performs the actual workflow operations.
 *
 * This is the final interceptor in the outbound chain - it doesn't delegate to a next interceptor
 * but instead executes the actual operations through the KotlinWorkflowContext.
 */
@InternalTemporalApi
internal class RootWorkflowOutboundCallsInterceptor(
  private val context: KotlinWorkflowContext,
  private val dataConverter: DataConverter
) : KWorkflowOutboundCallsInterceptor {

  // ==================== Activities ====================

  override suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R {
    val javaOptions = input.options.toJavaOptions()
    return context.executeActivityByName(
      activityName = input.activityName,
      options = javaOptions,
      resultClass = input.resultClass,
      args = input.arguments
    )
  }

  override suspend fun <R> executeLocalActivity(input: KLocalActivityInvocationInput<R>): R {
    val javaOptions = input.options.toJavaOptions()
    return context.executeLocalActivityByName(
      activityName = input.activityName,
      options = javaOptions,
      resultClass = input.resultClass,
      args = input.arguments
    )
  }

  // ==================== Child Workflows ====================

  override suspend fun <T, R> startChildWorkflow(
    input: KChildWorkflowInvocationInput<R>
  ): KChildWorkflowHandle<T, R> {
    val javaOptions = input.options.toJavaOptions(input.workflowId)
    return context.startChildWorkflowWithHandle(
      workflowType = input.workflowType,
      options = javaOptions,
      resultClass = input.resultClass,
      args = input.arguments
    )
  }

  // ==================== Timers ====================

  override suspend fun delay(duration: Duration) {
    context.createTimer(duration.toJavaDuration())
  }

  // ==================== Await Conditions ====================

  override suspend fun awaitCondition(timeout: Duration, reason: String, condition: () -> Boolean): Boolean {
    return context.awaitCondition(timeout.toJavaDuration(), condition)
  }

  override suspend fun awaitCondition(reason: String, condition: () -> Boolean) {
    context.awaitCondition(condition)
  }

  // ==================== Side Effects ====================

  override fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
    // For sideEffect, we need to wrap the function to serialize/deserialize
    var unserializedResult: R? = null

    // The context's sideEffect expects a suspend function, but we need synchronous behavior
    // We'll use a simplified approach that works with the replay context directly
    val resultPayloads = kotlinx.coroutines.runBlocking {
      context.sideEffect {
        val result = func()
        unserializedResult = result
        if (result != null && result != Unit) {
          dataConverter.toPayloads(result)
        } else {
          Optional.empty()
        }
      }
    }

    // Return the unserialized result if we have it
    unserializedResult?.let { return it }

    // Otherwise deserialize from the result
    @Suppress("UNCHECKED_CAST")
    return if (resultPayloads.isPresent && resultClass != Unit::class.java && resultClass != Void.TYPE) {
      dataConverter.fromPayload(resultPayloads.get().getPayloads(0), resultClass, resultClass)
    } else {
      null as R
    }
  }

  override fun <R> mutableSideEffect(
    id: String,
    resultClass: Class<R>,
    updated: (R?, R?) -> Boolean,
    func: () -> R
  ): R {
    return context.mutableSideEffect(id, resultClass) { previousValue ->
      val newValue = func()
      // The context's mutableSideEffect only takes the new value computation
      // The 'updated' predicate logic needs to be handled at a higher level if needed
      newValue
    }
  }

  // ==================== Versioning ====================

  override fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    return kotlinx.coroutines.runBlocking {
      context.getVersion(changeId, minSupported, maxSupported)
    }
  }

  // ==================== Continue As New ====================

  override fun continueAsNew(input: KContinueAsNewInput): Nothing {
    val javaOptions = input.options?.toJavaOptions()
    context.continueAsNew(input.workflowType, javaOptions, *input.arguments)
  }

  // ==================== External Workflow Communication ====================

  override suspend fun signalExternalWorkflow(input: KSignalExternalInput) {
    // Serialize the arguments
    val payloads = if (input.arguments.isNotEmpty()) {
      dataConverter.toPayloads(*input.arguments)
    } else {
      Optional.empty<Payloads>()
    }

    // Build signal attributes
    val attributes = SignalExternalWorkflowExecutionCommandAttributes.newBuilder()
      .setExecution(input.execution)
      .setSignalName(input.signalName)
    payloads.ifPresent { attributes.setInput(it) }

    // Use the replay context to signal external workflow
    kotlinx.coroutines.suspendCancellableCoroutine<Unit> { cont ->
      context.replayContext.signalExternalWorkflowExecution(
        attributes
      ) { _, failure ->
        if (failure != null) {
          cont.resumeWithException(RuntimeException(failure.message))
        } else {
          cont.resume(Unit) {}
        }
      }
    }
  }

  override suspend fun cancelWorkflow(input: KCancelWorkflowInput) {
    // Use the replay context to cancel external workflow
    kotlinx.coroutines.suspendCancellableCoroutine<Unit> { cont ->
      context.replayContext.requestCancelExternalWorkflowExecution(
        input.execution,
        input.reason
      ) { _, exception ->
        if (exception != null) {
          cont.resumeWithException(exception)
        } else {
          cont.resume(Unit) {}
        }
      }
    }
  }

  // ==================== Search Attributes and Memo ====================

  override fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>) {
    context.upsertTypedSearchAttributes(*updates)
  }

  override fun upsertMemo(memo: Map<String, Any>) {
    context.upsertMemo(memo)
  }

  // ==================== Utilities ====================

  override fun newRandom(): Random {
    return context.newRandom()
  }

  override fun randomUUID(): UUID {
    return context.randomUUID()
  }

  override fun currentTimeMillis(): Long {
    return context.currentTimeMillis
  }

  // ==================== Conversion Helpers ====================

  private fun KActivityOptions.toJavaOptions(): ActivityOptions {
    val builder = ActivityOptions.newBuilder()
    taskQueue?.let { builder.setTaskQueue(it) }
    scheduleToStartTimeout?.let { builder.setScheduleToStartTimeout(it.toJavaDuration()) }
    startToCloseTimeout?.let { builder.setStartToCloseTimeout(it.toJavaDuration()) }
    scheduleToCloseTimeout?.let { builder.setScheduleToCloseTimeout(it.toJavaDuration()) }
    heartbeatTimeout?.let { builder.setHeartbeatTimeout(it.toJavaDuration()) }
    retryOptions?.let { builder.setRetryOptions(it.toJavaOptions()) }
    cancellationType?.let { builder.setCancellationType(it) }
    return builder.build()
  }

  private fun KLocalActivityOptions.toJavaOptions(): LocalActivityOptions {
    val builder = LocalActivityOptions.newBuilder()
    scheduleToCloseTimeout?.let { builder.setScheduleToCloseTimeout(it.toJavaDuration()) }
    startToCloseTimeout?.let { builder.setStartToCloseTimeout(it.toJavaDuration()) }
    localRetryThreshold?.let { builder.setLocalRetryThreshold(it.toJavaDuration()) }
    retryOptions?.let { builder.setRetryOptions(it.toJavaOptions()) }
    return builder.build()
  }

  private fun KChildWorkflowOptions.toJavaOptions(workflowId: String): ChildWorkflowOptions {
    val builder = ChildWorkflowOptions.newBuilder()
    builder.setWorkflowId(workflowId)
    taskQueue?.let { builder.setTaskQueue(it) }
    workflowExecutionTimeout?.let { builder.setWorkflowExecutionTimeout(it.toJavaDuration()) }
    workflowRunTimeout?.let { builder.setWorkflowRunTimeout(it.toJavaDuration()) }
    workflowTaskTimeout?.let { builder.setWorkflowTaskTimeout(it.toJavaDuration()) }
    retryOptions?.let { builder.setRetryOptions(it.toJavaOptions()) }
    cancellationType?.let { builder.setCancellationType(it) }
    parentClosePolicy?.let { builder.setParentClosePolicy(it) }
    memo?.let { builder.setMemo(it) }
    typedSearchAttributes?.let { builder.setTypedSearchAttributes(it) }
    return builder.build()
  }

  private fun KContinueAsNewOptions.toJavaOptions(): ContinueAsNewOptions {
    val builder = ContinueAsNewOptions.newBuilder()
    taskQueue?.let { builder.setTaskQueue(it) }
    workflowRunTimeout?.let { builder.setWorkflowRunTimeout(it.toJavaDuration()) }
    workflowTaskTimeout?.let { builder.setWorkflowTaskTimeout(it.toJavaDuration()) }
    retryOptions?.let { builder.setRetryOptions(it.toJavaOptions()) }
    memo?.let { builder.setMemo(it) }
    typedSearchAttributes?.let { builder.setTypedSearchAttributes(it) }
    return builder.build()
  }

  private fun io.temporal.kotlin.common.KRetryOptions.toJavaOptions(): io.temporal.common.RetryOptions {
    val builder = io.temporal.common.RetryOptions.newBuilder()
    initialInterval?.let { builder.setInitialInterval(it.toJavaDuration()) }
    maximumInterval?.let { builder.setMaximumInterval(it.toJavaDuration()) }
    backoffCoefficient?.let { builder.setBackoffCoefficient(it) }
    maximumAttempts?.let { builder.setMaximumAttempts(it) }
    doNotRetry?.let { builder.setDoNotRetry(*it.toTypedArray()) }
    return builder.build()
  }
}
