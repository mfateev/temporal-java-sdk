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

package io.temporal.kotlin.interceptor

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.interceptors.Header
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.workflow.KChildWorkflowHandle
import io.temporal.kotlin.workflow.KChildWorkflowOptions
import io.temporal.kotlin.workflow.KContinueAsNewOptions
import java.lang.reflect.Type
import java.util.Random
import java.util.UUID
import kotlin.time.Duration

/**
 * Input for activity invocation.
 *
 * @property activityName the name of the activity type
 * @property resultClass the expected result class
 * @property resultType the expected result type (may include generics)
 * @property arguments the arguments to pass to the activity
 * @property options the activity options
 * @property header the header containing metadata
 */
public data class KActivityInvocationInput<R>(
  val activityName: String,
  val resultClass: Class<R>,
  val resultType: Type,
  val arguments: Array<Any?>,
  val options: KActivityOptions,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KActivityInvocationInput<*>
    if (activityName != other.activityName) return false
    if (resultClass != other.resultClass) return false
    if (resultType != other.resultType) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (options != other.options) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = activityName.hashCode()
    result = 31 * result + resultClass.hashCode()
    result = 31 * result + resultType.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + options.hashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for local activity invocation.
 *
 * @property activityName the name of the activity type
 * @property resultClass the expected result class
 * @property resultType the expected result type (may include generics)
 * @property arguments the arguments to pass to the activity
 * @property options the local activity options
 * @property header the header containing metadata
 */
public data class KLocalActivityInvocationInput<R>(
  val activityName: String,
  val resultClass: Class<R>,
  val resultType: Type,
  val arguments: Array<Any?>,
  val options: KLocalActivityOptions,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KLocalActivityInvocationInput<*>
    if (activityName != other.activityName) return false
    if (resultClass != other.resultClass) return false
    if (resultType != other.resultType) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (options != other.options) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = activityName.hashCode()
    result = 31 * result + resultClass.hashCode()
    result = 31 * result + resultType.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + options.hashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for child workflow invocation.
 *
 * @property workflowId the workflow ID for the child
 * @property workflowType the workflow type name
 * @property resultClass the expected result class
 * @property resultType the expected result type (may include generics)
 * @property arguments the arguments to pass to the child workflow
 * @property options the child workflow options
 * @property header the header containing metadata
 */
public data class KChildWorkflowInvocationInput<R>(
  val workflowId: String,
  val workflowType: String,
  val resultClass: Class<R>,
  val resultType: Type,
  val arguments: Array<Any?>,
  val options: KChildWorkflowOptions,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KChildWorkflowInvocationInput<*>
    if (workflowId != other.workflowId) return false
    if (workflowType != other.workflowType) return false
    if (resultClass != other.resultClass) return false
    if (resultType != other.resultType) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (options != other.options) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = workflowId.hashCode()
    result = 31 * result + workflowType.hashCode()
    result = 31 * result + resultClass.hashCode()
    result = 31 * result + resultType.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + options.hashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for continue-as-new.
 *
 * @property workflowType the workflow type for the new execution (null to inherit)
 * @property options the continue-as-new options (null to inherit)
 * @property arguments the arguments for the new execution
 * @property header the header containing metadata
 */
public data class KContinueAsNewInput(
  val workflowType: String?,
  val options: KContinueAsNewOptions?,
  val arguments: Array<Any?>,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KContinueAsNewInput
    if (workflowType != other.workflowType) return false
    if (options != other.options) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = workflowType?.hashCode() ?: 0
    result = 31 * result + (options?.hashCode() ?: 0)
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for signaling an external workflow.
 *
 * @property execution the target workflow execution
 * @property signalName the name of the signal
 * @property arguments the arguments for the signal
 * @property header the header containing metadata
 */
public data class KSignalExternalInput(
  val execution: WorkflowExecution,
  val signalName: String,
  val arguments: Array<Any?>,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KSignalExternalInput
    if (execution != other.execution) return false
    if (signalName != other.signalName) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = execution.hashCode()
    result = 31 * result + signalName.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for canceling an external workflow.
 *
 * @property execution the target workflow execution
 * @property reason optional reason for the cancellation
 */
public data class KCancelWorkflowInput(
  val execution: WorkflowExecution,
  val reason: String?
)

/**
 * Intercepts outbound calls from workflow code to Temporal APIs (activities, child workflows, timers, etc.).
 *
 * The calls to the interceptor are executed in the context of a workflow and must follow
 * the same rules all the other workflow code follows.
 *
 * All async operations are `suspend` functions. For parallel execution, use standard `async { }` pattern.
 *
 * Prefer extending [KWorkflowOutboundCallsInterceptorBase] and overriding only the methods
 * you need instead of implementing this interface directly.
 *
 * An instance may be created in [KWorkflowInboundCallsInterceptor.init] and set by passing it
 * into `init` method of the `next` [KWorkflowInboundCallsInterceptor].
 *
 * Example:
 * ```kotlin
 * class TracingOutboundInterceptor(
 *     next: KWorkflowOutboundCallsInterceptor
 * ) : KWorkflowOutboundCallsInterceptorBase(next) {
 *
 *     override suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R {
 *         val span = tracer.startSpan("activity:${input.activityName}")
 *         return try {
 *             next.executeActivity(input)
 *         } finally {
 *             span.end()
 *         }
 *     }
 * }
 * ```
 *
 * @see KWorkflowInboundCallsInterceptor.init
 * @see KWorkflowOutboundCallsInterceptorBase
 */
public interface KWorkflowOutboundCallsInterceptor {

  // ==================== Activities ====================

  /**
   * Executes an activity.
   *
   * Use `async { }` for parallel execution.
   *
   * @param input the activity invocation input
   * @return the activity result
   */
  public suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R

  /**
   * Executes a local activity.
   *
   * Use `async { }` for parallel execution.
   *
   * @param input the local activity invocation input
   * @return the activity result
   */
  public suspend fun <R> executeLocalActivity(input: KLocalActivityInvocationInput<R>): R

  // ==================== Child Workflows ====================

  /**
   * Starts a child workflow and returns a handle with Deferred result.
   *
   * @param input the child workflow invocation input
   * @return a handle for interacting with the child workflow
   */
  public suspend fun <T, R> startChildWorkflow(input: KChildWorkflowInvocationInput<R>): KChildWorkflowHandle<T, R>

  // ==================== Timers ====================

  /**
   * Suspends for the specified duration.
   *
   * @param duration the duration to wait
   */
  public suspend fun delay(duration: Duration)

  // ==================== Await Conditions ====================

  /**
   * Awaits until the condition returns true or timeout expires.
   *
   * @param timeout maximum time to wait
   * @param reason description of what is being awaited (for debugging)
   * @param condition the condition to evaluate
   * @return true if condition was satisfied, false if timeout expired
   */
  public suspend fun awaitCondition(timeout: Duration, reason: String, condition: () -> Boolean): Boolean

  /**
   * Awaits until the condition returns true.
   *
   * @param reason description of what is being awaited (for debugging)
   * @param condition the condition to evaluate
   */
  public suspend fun awaitCondition(reason: String, condition: () -> Boolean)

  // ==================== Side Effects ====================

  /**
   * Executes a non-deterministic function and records its result.
   *
   * @param resultClass the expected result class
   * @param func the function to execute
   * @return the result (or recorded value during replay)
   */
  public fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R

  /**
   * Executes a mutable side effect.
   *
   * Only records a new marker if the value has changed.
   *
   * @param id unique identifier for this mutable side effect
   * @param resultClass the expected result class
   * @param updated predicate to determine if value changed
   * @param func function to compute the new value
   * @return the result
   */
  public fun <R> mutableSideEffect(id: String, resultClass: Class<R>, updated: (R?, R?) -> Boolean, func: () -> R): R

  // ==================== Versioning ====================

  /**
   * Gets a version for a particular change.
   *
   * @param changeId identifier of a particular change
   * @param minSupported minimum supported version
   * @param maxSupported maximum supported version
   * @return the version to use for the change
   */
  public fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int

  // ==================== Continue As New ====================

  /**
   * Continues the workflow as a new execution.
   *
   * This method never returns normally.
   *
   * @param input the continue-as-new input
   */
  public fun continueAsNew(input: KContinueAsNewInput): Nothing

  // ==================== External Workflow Communication ====================

  /**
   * Signals an external workflow.
   *
   * @param input the signal input
   */
  public suspend fun signalExternalWorkflow(input: KSignalExternalInput)

  /**
   * Cancels an external workflow.
   *
   * @param input the cancel input
   */
  public suspend fun cancelWorkflow(input: KCancelWorkflowInput)

  // ==================== Search Attributes and Memo ====================

  /**
   * Updates search attributes.
   *
   * @param updates the search attribute updates to apply
   */
  public fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>)

  /**
   * Updates workflow memo.
   *
   * @param memo map of memo key-value pairs to upsert
   */
  public fun upsertMemo(memo: Map<String, Any>)

  // ==================== Utilities ====================

  /**
   * Returns a deterministic random number generator.
   *
   * @return a deterministic random generator
   */
  public fun newRandom(): Random

  /**
   * Generates a deterministic UUID.
   *
   * @return a deterministic UUID
   */
  public fun randomUUID(): UUID

  /**
   * Returns the current workflow time in milliseconds.
   *
   * @return current time in milliseconds
   */
  public fun currentTimeMillis(): Long
}

/**
 * Base implementation that forwards all calls to the next interceptor.
 *
 * Extend this class and override only the methods you need.
 *
 * @param next the next interceptor in the chain
 */
public open class KWorkflowOutboundCallsInterceptorBase(
  protected val next: KWorkflowOutboundCallsInterceptor
) : KWorkflowOutboundCallsInterceptor {

  override suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R {
    return next.executeActivity(input)
  }

  override suspend fun <R> executeLocalActivity(input: KLocalActivityInvocationInput<R>): R {
    return next.executeLocalActivity(input)
  }

  override suspend fun <T, R> startChildWorkflow(input: KChildWorkflowInvocationInput<R>): KChildWorkflowHandle<T, R> {
    return next.startChildWorkflow(input)
  }

  override suspend fun delay(duration: Duration) {
    next.delay(duration)
  }

  override suspend fun awaitCondition(timeout: Duration, reason: String, condition: () -> Boolean): Boolean {
    return next.awaitCondition(timeout, reason, condition)
  }

  override suspend fun awaitCondition(reason: String, condition: () -> Boolean) {
    next.awaitCondition(reason, condition)
  }

  override fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
    return next.sideEffect(resultClass, func)
  }

  override fun <R> mutableSideEffect(id: String, resultClass: Class<R>, updated: (R?, R?) -> Boolean, func: () -> R): R {
    return next.mutableSideEffect(id, resultClass, updated, func)
  }

  override fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    return next.getVersion(changeId, minSupported, maxSupported)
  }

  override fun continueAsNew(input: KContinueAsNewInput): Nothing {
    next.continueAsNew(input)
  }

  override suspend fun signalExternalWorkflow(input: KSignalExternalInput) {
    next.signalExternalWorkflow(input)
  }

  override suspend fun cancelWorkflow(input: KCancelWorkflowInput) {
    next.cancelWorkflow(input)
  }

  override fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>) {
    next.upsertTypedSearchAttributes(*updates)
  }

  override fun upsertMemo(memo: Map<String, Any>) {
    next.upsertMemo(memo)
  }

  override fun newRandom(): Random {
    return next.newRandom()
  }

  override fun randomUUID(): UUID {
    return next.randomUUID()
  }

  override fun currentTimeMillis(): Long {
    return next.currentTimeMillis()
  }
}
