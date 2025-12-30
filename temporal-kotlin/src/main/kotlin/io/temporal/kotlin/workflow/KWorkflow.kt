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

@file:OptIn(InternalTemporalApi::class, kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.workflow

import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.kotlin.toJava
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInfo
import kotlinx.coroutines.suspendCancellableCoroutine
import java.time.Instant
import java.util.Random
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration

/**
 * Provides access to Temporal workflow APIs from within Kotlin workflow code.
 *
 * This object provides Kotlin-friendly wrappers for Temporal workflow operations
 * including time, random number generation, versioning, and side effects.
 *
 * All methods must be called from within workflow code only.
 *
 * Example:
 * ```kotlin
 * @WorkflowInterface
 * interface MyWorkflow {
 *   @WorkflowMethod
 *   suspend fun execute(): String
 * }
 *
 * class MyWorkflowImpl : MyWorkflow {
 *   override suspend fun execute(): String {
 *     val info = KWorkflow.getInfo()
 *     val currentTime = KWorkflow.currentTime()
 *     return "Workflow ${info.workflowId} at $currentTime"
 *   }
 * }
 * ```
 */
public object KWorkflow {

  /**
   * Thread-local storage for the current Kotlin workflow context.
   * This is set by the workflow runner when executing workflow code.
   */
  @InternalTemporalApi
  internal val currentContext = ThreadLocal<KotlinWorkflowContext?>()

  /**
   * Returns information about the current workflow execution.
   *
   * @return the workflow information with Kotlin-friendly nullable types
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun getInfo(): KWorkflowInfo {
    // For now, delegate to the Java SDK's Workflow.getInfo()
    // Once we have full Kotlin context integration, we can use our own context
    val javaInfo: WorkflowInfo = Workflow.getInfo()
    return KWorkflowInfoImpl(javaInfo)
  }

  /**
   * Returns the current workflow time as an [Instant].
   *
   * This is deterministic and returns the same value during replay.
   * Must be used instead of [System.currentTimeMillis] or [Instant.now]
   * to ensure deterministic workflow execution.
   *
   * @return the current workflow time
   */
  public fun currentTime(): Instant {
    return Instant.ofEpochMilli(currentTimeMillis())
  }

  /**
   * Returns the current workflow time in milliseconds since epoch.
   *
   * This is deterministic and returns the same value during replay.
   *
   * @return current time in milliseconds
   */
  public fun currentTimeMillis(): Long {
    val context = currentContext.get()
      ?: throw IllegalStateException("Called outside of workflow context")
    return context.currentTimeMillis
  }

  /**
   * Generates a deterministic UUID.
   *
   * Must be used instead of [UUID.randomUUID] to ensure deterministic
   * workflow execution during replay.
   *
   * @return a deterministic UUID
   */
  public fun randomUUID(): UUID {
    val context = currentContext.get()
      ?: throw IllegalStateException("Called outside of workflow context")
    return context.randomUUID()
  }

  /**
   * Returns a deterministic random number generator.
   *
   * Must be used instead of [java.util.Random] or other random generators
   * to ensure deterministic workflow execution during replay.
   *
   * @return a deterministic random generator
   */
  public fun newRandom(): Random {
    val context = currentContext.get()
      ?: throw IllegalStateException("Called outside of workflow context")
    return context.newRandom()
  }

  /**
   * Gets a version for a particular change.
   *
   * Used to safely perform backwards incompatible changes to workflow
   * definitions. Not supported for the replaying code that has already
   * recorded activity calls for the change-id.
   *
   * @param changeId identifier of a particular change
   * @param minSupported minimum supported version
   * @param maxSupported maximum supported version
   * @return the version to use for the change
   */
  public fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    return Workflow.getVersion(changeId, minSupported, maxSupported)
  }

  /**
   * Executes a non-deterministic function and records its result.
   *
   * Use this for operations like generating random values from external
   * sources or getting the current date/time from system clock.
   *
   * The function is executed only during the first workflow execution.
   * During replay, the recorded value is returned without re-executing
   * the function.
   *
   * Warning: Do not use sideEffect to modify workflow state.
   * Only use the returned value.
   *
   * Example:
   * ```kotlin
   * val randomValue = KWorkflow.sideEffect {
   *   SecureRandom().nextInt(100)
   * }
   * ```
   *
   * @param R the result type
   * @param resultClass the class of the result type
   * @param func the function to execute
   * @return the result of the function (or recorded value during replay)
   */
  public fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
    return Workflow.sideEffect(resultClass) { func() }
  }

  /**
   * Reified version of [sideEffect] for easier Kotlin usage.
   *
   * Example:
   * ```kotlin
   * val randomValue: Int = KWorkflow.sideEffect {
   *   SecureRandom().nextInt(100)
   * }
   * ```
   *
   * @param R the result type
   * @param func the function to execute
   * @return the result of the function (or recorded value during replay)
   */
  public inline fun <reified R> sideEffect(noinline func: () -> R): R {
    return sideEffect(R::class.java, func)
  }

  /**
   * Checks if cancellation has been requested for this workflow.
   *
   * @return true if cancellation has been requested
   */
  public fun isCancelRequested(): Boolean {
    return try {
      val context = currentContext.get()
      context?.isCancelRequested ?: false
    } catch (e: Exception) {
      false
    }
  }

  /**
   * Default version constant for workflow versioning.
   *
   * Use this as minSupported when introducing a new version
   * to allow workflows that haven't recorded any version yet.
   */
  public const val DEFAULT_VERSION: Int = Workflow.DEFAULT_VERSION

  /**
   * Executes an activity by name and waits for the result.
   *
   * This is a suspend function that will suspend the coroutine until
   * the activity completes.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeActivity(
   *   "myActivity",
   *   options = ActivityOptions {
   *     setStartToCloseTimeout(Duration.ofMinutes(5))
   *   },
   *   "arg1", 42
   * )
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param options the activity options
   * @param args arguments to pass to the activity
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend inline fun <reified R> executeActivity(
    activityName: String,
    options: ActivityOptions,
    vararg args: Any?
  ): R {
    return executeActivity(activityName, R::class.java, options, *args)
  }

  /**
   * Executes an activity by name and waits for the result.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param resultClass the class of the expected result type
   * @param options the activity options
   * @param args arguments to pass to the activity
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend fun <R> executeActivity(
    activityName: String,
    resultClass: Class<R>,
    options: ActivityOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeActivity must be called from within workflow code")
    return context.executeActivityByName(activityName, options, resultClass, *args)
  }

  /**
   * Starts an activity asynchronously and returns a handle to await or cancel it.
   *
   * Use this for parallel activity execution patterns where you want to
   * start multiple activities and await them later.
   *
   * Example:
   * ```kotlin
   * val handle1 = KWorkflow.startActivity<String>(
   *   "activity1",
   *   options = activityOptions,
   *   "arg1"
   * )
   * val handle2 = KWorkflow.startActivity<Int>(
   *   "activity2",
   *   options = activityOptions,
   *   42
   * )
   *
   * // Activities run in parallel
   * val result1 = handle1.await()
   * val result2 = handle2.await()
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param options the activity options
   * @param args arguments to pass to the activity
   * @return a handle that can be used to await or cancel the activity
   */
  public inline fun <reified R> startActivity(
    activityName: String,
    options: ActivityOptions,
    vararg args: Any?
  ): KActivityHandle<R> {
    return startActivity(activityName, R::class.java, options, *args)
  }

  /**
   * Starts an activity asynchronously and returns a handle to await or cancel it.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param resultClass the class of the expected result type
   * @param options the activity options
   * @param args arguments to pass to the activity
   * @return a handle that can be used to await or cancel the activity
   */
  public fun <R> startActivity(
    activityName: String,
    resultClass: Class<R>,
    options: ActivityOptions,
    vararg args: Any?
  ): KActivityHandle<R> {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startActivity must be called from within workflow code")
    // For async activity execution, we wrap the suspend function in a handle
    return DeferredActivityHandle(context, activityName, options, resultClass, args)
  }

  /**
   * Suspends until the given condition evaluates to true.
   *
   * The condition is evaluated whenever the workflow receives a new event
   * such as a signal, timer firing, or activity completion.
   *
   * Example:
   * ```kotlin
   * var approved = false
   *
   * // In signal handler:
   * approved = true
   *
   * // In workflow:
   * KWorkflow.condition { approved }
   * // Continues after approved becomes true
   * ```
   *
   * @param condition the condition to wait for
   */
  public fun condition(condition: () -> Boolean) {
    Workflow.await { condition() }
  }

  /**
   * Suspends until the given condition evaluates to true or the timeout expires.
   *
   * @param timeout maximum time to wait for the condition
   * @param condition the condition to wait for
   * @return true if condition was satisfied, false if timeout expired
   */
  public fun condition(timeout: Duration, condition: () -> Boolean): Boolean {
    return Workflow.await(timeout.toJava()) { condition() }
  }

  /**
   * Suspends until the given condition evaluates to true or the timeout expires.
   *
   * @param timeout maximum time to wait for the condition (Java Duration)
   * @param condition the condition to wait for
   * @return true if condition was satisfied, false if timeout expired
   */
  public fun condition(timeout: java.time.Duration, condition: () -> Boolean): Boolean {
    return Workflow.await(timeout) { condition() }
  }

  // ==================== Local Activity Methods ====================

  /**
   * Executes a local activity by name and waits for the result.
   *
   * Local activities are short-lived activities that execute in the same
   * worker process as the workflow. They are optimized for low-latency
   * operations and don't require a separate activity task queue.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeLocalActivity(
   *   "validateInput",
   *   options = LocalActivityOptions {
   *     setStartToCloseTimeout(Duration.ofSeconds(5))
   *   },
   *   inputData
   * )
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param options the local activity options
   * @param args arguments to pass to the activity
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend inline fun <reified R> executeLocalActivity(
    activityName: String,
    options: LocalActivityOptions,
    vararg args: Any?
  ): R {
    return executeLocalActivity(activityName, R::class.java, options, *args)
  }

  /**
   * Executes a local activity by name and waits for the result.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param resultClass the class of the expected result type
   * @param options the local activity options
   * @param args arguments to pass to the activity
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend fun <R> executeLocalActivity(
    activityName: String,
    resultClass: Class<R>,
    options: LocalActivityOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeLocalActivity must be called from within workflow code")
    return context.executeLocalActivityByName(activityName, options, resultClass, *args)
  }

  /**
   * Starts a local activity asynchronously and returns a handle to await or cancel it.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param options the local activity options
   * @param args arguments to pass to the activity
   * @return a handle that can be used to await or cancel the activity
   */
  public inline fun <reified R> startLocalActivity(
    activityName: String,
    options: LocalActivityOptions,
    vararg args: Any?
  ): KActivityHandle<R> {
    return startLocalActivity(activityName, R::class.java, options, *args)
  }

  /**
   * Starts a local activity asynchronously and returns a handle to await or cancel it.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param resultClass the class of the expected result type
   * @param options the local activity options
   * @param args arguments to pass to the activity
   * @return a handle that can be used to await or cancel the activity
   */
  public fun <R> startLocalActivity(
    activityName: String,
    resultClass: Class<R>,
    options: LocalActivityOptions,
    vararg args: Any?
  ): KActivityHandle<R> {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startLocalActivity must be called from within workflow code")
    return DeferredLocalActivityHandle(context, activityName, options, resultClass, args)
  }

  // ==================== Child Workflow Methods ====================

  /**
   * Executes a child workflow by type name and waits for the result.
   *
   * This is a suspend function that will suspend the coroutine until
   * the child workflow completes.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeChildWorkflow(
   *   "ChildWorkflow",
   *   options = ChildWorkflowOptions {
   *     setWorkflowId("child-workflow-id")
   *   },
   *   "arg1", 42
   * )
   * ```
   *
   * @param R the expected return type of the child workflow
   * @param workflowType the type name of the child workflow
   * @param options the child workflow options
   * @param args arguments to pass to the child workflow
   * @return the child workflow result
   * @throws ChildWorkflowException if the child workflow fails
   */
  public suspend inline fun <reified R> executeChildWorkflow(
    workflowType: String,
    options: ChildWorkflowOptions,
    vararg args: Any?
  ): R {
    return executeChildWorkflow(workflowType, R::class.java, options, *args)
  }

  /**
   * Executes a child workflow by type name and waits for the result.
   *
   * @param R the expected return type of the child workflow
   * @param workflowType the type name of the child workflow
   * @param resultClass the class of the expected result type
   * @param options the child workflow options
   * @param args arguments to pass to the child workflow
   * @return the child workflow result
   * @throws ChildWorkflowException if the child workflow fails
   */
  public suspend fun <R> executeChildWorkflow(
    workflowType: String,
    resultClass: Class<R>,
    options: ChildWorkflowOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeChildWorkflow must be called from within workflow code")
    return context.executeChildWorkflowByName(workflowType, options, resultClass, *args)
  }

  /**
   * Starts a child workflow asynchronously and returns a handle to await or cancel it.
   *
   * Use this for parallel child workflow execution patterns where you want to
   * start multiple child workflows and await them later.
   *
   * Example:
   * ```kotlin
   * val handle1 = KWorkflow.startChildWorkflow<String>(
   *   "ChildWorkflow1",
   *   options = childOptions,
   *   "arg1"
   * )
   * val handle2 = KWorkflow.startChildWorkflow<Int>(
   *   "ChildWorkflow2",
   *   options = childOptions,
   *   42
   * )
   *
   * // Child workflows run in parallel
   * val result1 = handle1.await()
   * val result2 = handle2.await()
   * ```
   *
   * @param R the expected return type of the child workflow
   * @param workflowType the type name of the child workflow
   * @param options the child workflow options
   * @param args arguments to pass to the child workflow
   * @return a handle that can be used to await, cancel, or signal the child workflow
   */
  public inline fun <reified R> startChildWorkflow(
    workflowType: String,
    options: ChildWorkflowOptions,
    vararg args: Any?
  ): KChildWorkflowHandle<R> {
    return startChildWorkflow(workflowType, R::class.java, options, *args)
  }

  /**
   * Starts a child workflow asynchronously and returns a handle to await or cancel it.
   *
   * @param R the expected return type of the child workflow
   * @param workflowType the type name of the child workflow
   * @param resultClass the class of the expected result type
   * @param options the child workflow options
   * @param args arguments to pass to the child workflow
   * @return a handle that can be used to await, cancel, or signal the child workflow
   */
  public fun <R> startChildWorkflow(
    workflowType: String,
    resultClass: Class<R>,
    options: ChildWorkflowOptions,
    vararg args: Any?
  ): KChildWorkflowHandle<R> {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    return DeferredChildWorkflowHandle(context, workflowType, options, resultClass, args)
  }

  // ==================== Timer/Delay Methods ====================

  /**
   * Suspends the workflow for the specified duration.
   *
   * This is deterministic and will resume at the same point during replay.
   * Must be used instead of [Thread.sleep] or [kotlinx.coroutines.delay]
   * to ensure deterministic workflow execution.
   *
   * Example:
   * ```kotlin
   * // Wait for 5 minutes
   * KWorkflow.delay(5.minutes)
   *
   * // Or with Java duration
   * KWorkflow.delay(java.time.Duration.ofMinutes(5))
   * ```
   *
   * @param duration the duration to sleep (Kotlin Duration)
   */
  public suspend fun delay(duration: Duration) {
    delay(duration.toJava())
  }

  /**
   * Suspends the workflow for the specified duration.
   *
   * @param duration the duration to sleep (Java Duration)
   */
  public suspend fun delay(duration: java.time.Duration) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.delay must be called from within workflow code")
    context.createTimer(duration)
  }

  /**
   * Suspends the workflow for the specified number of milliseconds.
   *
   * @param millis the number of milliseconds to sleep
   */
  public suspend fun delay(millis: Long) {
    delay(java.time.Duration.ofMillis(millis))
  }

  /**
   * Returns the current workflow time in milliseconds.
   * Uses the context if available, otherwise falls back to Workflow API.
   */
  internal fun currentTimeMillisInternal(): Long {
    val context = currentContext.get()
    return context?.currentTimeMillis ?: Workflow.currentTimeMillis()
  }
}

/**
 * Internal implementation of [KActivityHandle] that wraps a [Promise].
 */
internal class PromiseActivityHandle<R>(
  private val promise: Promise<R>
) : KActivityHandle<R> {

  override val isCompleted: Boolean
    get() = promise.isCompleted

  override suspend fun await(): R = promise.await()

  override fun cancel(reason: String?) {
    // Note: Promise cancellation in Temporal is handled through CancellationScope
    // This is a best-effort cancel - the activity may have already completed
    // Full cancellation support requires wrapping in a CancellationScope
  }
}

/**
 * Suspends until this [Promise] completes and returns the result.
 *
 * This extension function converts the blocking Promise.get() call
 * to a coroutine-friendly suspend function.
 *
 * Note: This is designed to work within Temporal workflow context
 * where the workflow thread handles deterministic execution.
 *
 * @return the promise result
 * @throws Exception if the promise completed exceptionally
 */
public suspend fun <R> Promise<R>.await(): R = suspendCancellableCoroutine { cont ->
  // Use handle to get both success and failure cases
  this.handle { result, exception ->
    if (exception != null) {
      cont.resumeWithException(exception)
    } else {
      cont.resume(result)
    }
    null // Return value required by handle but not used
  }
}

/**
 * Deferred activity handle that executes the activity when await() is called.
 */
internal class DeferredActivityHandle<R>(
  private val context: KotlinWorkflowContext,
  private val activityName: String,
  private val options: ActivityOptions,
  private val resultClass: Class<R>,
  private val args: Array<out Any?>
) : KActivityHandle<R> {

  @Volatile
  private var completed = false

  @Volatile
  private var result: R? = null

  override val isCompleted: Boolean
    get() = completed

  override suspend fun await(): R {
    if (completed) {
      @Suppress("UNCHECKED_CAST")
      return result as R
    }
    val r = context.executeActivityByName(activityName, options, resultClass, *args)
    result = r
    completed = true
    return r
  }

  override fun cancel(reason: String?) {
    // Cancellation not supported for deferred activities
  }
}

/**
 * Deferred local activity handle that executes the activity when await() is called.
 */
internal class DeferredLocalActivityHandle<R>(
  private val context: KotlinWorkflowContext,
  private val activityName: String,
  private val options: LocalActivityOptions,
  private val resultClass: Class<R>,
  private val args: Array<out Any?>
) : KActivityHandle<R> {

  @Volatile
  private var completed = false

  @Volatile
  private var result: R? = null

  override val isCompleted: Boolean
    get() = completed

  override suspend fun await(): R {
    if (completed) {
      @Suppress("UNCHECKED_CAST")
      return result as R
    }
    val r = context.executeLocalActivityByName(activityName, options, resultClass, *args)
    result = r
    completed = true
    return r
  }

  override fun cancel(reason: String?) {
    // Cancellation not supported for deferred activities
  }
}

/**
 * Deferred child workflow handle that executes the child workflow when await() is called.
 */
internal class DeferredChildWorkflowHandle<R>(
  private val context: KotlinWorkflowContext,
  private val workflowType: String,
  private val options: ChildWorkflowOptions,
  private val resultClass: Class<R>,
  private val args: Array<out Any?>
) : KChildWorkflowHandle<R> {

  @Volatile
  private var completed = false

  @Volatile
  private var result: R? = null

  override val isCompleted: Boolean
    get() = completed

  override suspend fun await(): R {
    if (completed) {
      @Suppress("UNCHECKED_CAST")
      return result as R
    }
    val r = context.executeChildWorkflowByName(workflowType, options, resultClass, *args)
    result = r
    completed = true
    return r
  }

  override suspend fun getExecution(): io.temporal.api.common.v1.WorkflowExecution {
    // For deferred handles, the workflow starts when await() is called
    // This is a simplified implementation
    throw UnsupportedOperationException("getExecution is not supported for deferred child workflows")
  }

  override fun signal(signalName: String, vararg args: Any?) {
    // Signaling not supported for deferred child workflows
    throw UnsupportedOperationException("signal is not supported for deferred child workflows")
  }
}
