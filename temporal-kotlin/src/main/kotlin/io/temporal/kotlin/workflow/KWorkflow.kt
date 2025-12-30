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
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.kotlin.toJava
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
    return Instant.ofEpochMilli(Workflow.currentTimeMillis())
  }

  /**
   * Returns the current workflow time in milliseconds since epoch.
   *
   * This is deterministic and returns the same value during replay.
   *
   * @return current time in milliseconds
   */
  public fun currentTimeMillis(): Long {
    return Workflow.currentTimeMillis()
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
    return Workflow.randomUUID()
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
    return Workflow.newRandom()
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
    val stub = Workflow.newUntypedActivityStub(options)
    val promise: Promise<R> = stub.executeAsync(activityName, resultClass, *args)
    return promise.await()
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
    val stub = Workflow.newUntypedActivityStub(options)
    val promise: Promise<R> = stub.executeAsync(activityName, resultClass, *args)
    return PromiseActivityHandle(promise)
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
