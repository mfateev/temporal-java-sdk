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

import com.uber.m3.tally.Scope
import io.temporal.activity.ActivityMethod
import io.temporal.common.SearchAttributeKey
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.SearchAttributes
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.common.KArgs2
import io.temporal.kotlin.common.KArgs3
import io.temporal.kotlin.common.KArgs4
import io.temporal.kotlin.common.KArgs5
import io.temporal.kotlin.common.KArgs6
import io.temporal.kotlin.common.KEncodedValues
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.converters.KOptionsConverters
import io.temporal.kotlin.internal.workflow.KotlinWorkflowContext
import io.temporal.kotlin.toJava
import io.temporal.workflow.Promise
import io.temporal.workflow.UpdateInfo
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInfo
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.delay
import org.slf4j.Logger
import java.time.Instant
import java.util.Random
import java.util.UUID
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3
import kotlin.reflect.KFunction4
import kotlin.reflect.KFunction5
import kotlin.reflect.KFunction6
import kotlin.reflect.KFunction7
import kotlin.reflect.KSuspendFunction1
import kotlin.reflect.KSuspendFunction2
import kotlin.reflect.KSuspendFunction3
import kotlin.reflect.KSuspendFunction4
import kotlin.reflect.KSuspendFunction5
import kotlin.reflect.KSuspendFunction6
import kotlin.reflect.KSuspendFunction7
import kotlin.reflect.jvm.javaMethod
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
 *     val now = KWorkflow.now()
 *     return "Workflow ${info.workflowId} at $now"
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
  @PublishedApi
  internal val currentContext = ThreadLocal<KotlinWorkflowContext?>()

  /**
   * Information about the current workflow execution.
   *
   * Example:
   * ```kotlin
   * if (KWorkflow.info.isContinueAsNewSuggested) {
   *     KWorkflow.continueAsNew(state)
   * }
   * ```
   *
   * @throws IllegalStateException if accessed outside of workflow code
   */
  public val info: KWorkflowInfo
    @JvmName("info")
    get() {
      // First try the Kotlin coroutine context (for Kotlin workflows)
      val context = currentContext.get()
      if (context != null) {
        return KWorkflowInfoFromContext(context)
      }
      // Fall back to Java SDK context (for Java workflows calling Kotlin code)
      val javaInfo: WorkflowInfo = Workflow.getInfo()
      return KWorkflowInfoImpl(javaInfo)
    }

  /**
   * Returns a logger for the current workflow.
   *
   * Uses the workflow type as the logger name.
   *
   * @return SLF4J logger for workflow logging
   */
  public fun logger(): Logger {
    return Workflow.getLogger(Workflow.getInfo().workflowType)
  }

  /**
   * Returns a logger with the specified name.
   *
   * @param name the logger name
   * @return SLF4J logger for workflow logging
   */
  public fun logger(name: String): Logger {
    return Workflow.getLogger(name)
  }

  /**
   * Returns a logger for the specified class.
   *
   * @param clazz the class to use as the logger name
   * @return SLF4J logger for workflow logging
   */
  public fun logger(clazz: Class<*>): Logger {
    return Workflow.getLogger(clazz)
  }

  /**
   * Returns the current workflow time as an [Instant].
   *
   * This is deterministic and returns the same value during replay.
   * Must be used instead of [System.currentTimeMillis] or [java.time.Instant.now]
   * to ensure deterministic workflow execution.
   *
   * Matches Kotlin's idiomatic `Clock.System.now()` naming.
   *
   * @return the current workflow time
   */
  public fun now(): Instant {
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
    val interceptor = context.outboundInterceptor
      ?: return context.currentTimeMillis // Fallback if interceptor not yet initialized
    return interceptor.currentTimeMillis()
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
    val interceptor = context.outboundInterceptor
      ?: return context.randomUUID() // Fallback if interceptor not yet initialized
    return interceptor.randomUUID()
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
    val interceptor = context.outboundInterceptor
      ?: return context.newRandom() // Fallback if interceptor not yet initialized
    return interceptor.newRandom()
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

  // ==================== Search Attributes ====================

  /**
   * The current search attributes as a typed [SearchAttributes] object.
   *
   * Example:
   * ```kotlin
   * val attrs = KWorkflow.typedSearchAttributes
   * val status = attrs.get(SearchAttributeKey.forKeyword("Status"))
   * ```
   */
  public val typedSearchAttributes: SearchAttributes
    @JvmName("typedSearchAttributes")
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.typedSearchAttributes must be accessed from within workflow code")
      return context.getTypedSearchAttributes()
    }

  /**
   * Gets a single search attribute value by key.
   *
   * @param key the search attribute key
   * @return the search attribute value, or null if not found
   */
  public fun <T> getSearchAttribute(key: SearchAttributeKey<T>): T? {
    return typedSearchAttributes.get(key)
  }

  /**
   * Updates search attributes by applying the given updates.
   *
   * Example:
   * ```kotlin
   * KWorkflow.upsertTypedSearchAttributes(
   *     SearchAttributeKey.forKeyword("Status").valueSet("Processing"),
   *     SearchAttributeKey.forLong("Count").valueSet(42L)
   * )
   * ```
   *
   * @param updates the search attribute updates to apply
   */
  public fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.upsertTypedSearchAttributes must be called from within workflow code")
    context.upsertTypedSearchAttributes(*updates)
  }

  // ==================== Memo ====================

  /**
   * Gets a memo value by key.
   *
   * @param key the memo key
   * @param valueClass the expected value class
   * @return the memo value, or null if not found
   */
  public fun <T> getMemo(key: String, valueClass: Class<T>): T? {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.getMemo must be called from within workflow code")
    return context.getMemo(key, valueClass)
  }

  /**
   * Gets a memo value by key using reified type.
   *
   * @param key the memo key
   * @return the memo value, or null if not found
   */
  public inline fun <reified T> getMemo(key: String): T? {
    return getMemo(key, T::class.java)
  }

  /**
   * Updates workflow memo with the given key-value pairs.
   *
   * Example:
   * ```kotlin
   * KWorkflow.upsertMemo(mapOf(
   *     "status" to "processing",
   *     "count" to 42
   * ))
   * ```
   *
   * @param memo map of memo key-value pairs to upsert
   */
  public fun upsertMemo(memo: Map<String, Any?>) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.upsertMemo must be called from within workflow code")
    context.upsertMemo(memo)
  }

  // ==================== Cron/Continue-As-New Support ====================

  /**
   * Gets the result from the last successful run of this workflow.
   * Useful for cron workflows or continue-as-new chains.
   *
   * @param resultClass the expected result class
   * @return the last completion result, or null if none
   */
  public fun <R> getLastCompletionResult(resultClass: Class<R>): R? {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.getLastCompletionResult must be called from within workflow code")
    return context.getLastCompletionResult(resultClass)
  }

  /**
   * Gets the result from the last successful run of this workflow using reified type.
   *
   * @return the last completion result, or null if none
   */
  public inline fun <reified R> getLastCompletionResult(): R? {
    return getLastCompletionResult(R::class.java)
  }

  /**
   * The failure from the previous run of this workflow, if any.
   * Useful for cron workflows or continue-as-new chains.
   *
   * Example:
   * ```kotlin
   * val failure = KWorkflow.previousRunFailure
   * if (failure != null) {
   *     logger.warn("Previous run failed: ${failure.message}")
   * }
   * ```
   */
  public val previousRunFailure: Exception?
    @JvmName("previousRunFailure")
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.previousRunFailure must be accessed from within workflow code")
      return context.getPreviousRunFailure()
    }

  /**
   * Gets the failure from the previous run of this workflow, if any.
   *
   * @return the previous run failure, or null if the previous run succeeded
   */

  // ==================== Replay Detection ====================

  /**
   * Whether the workflow code is currently being replayed.
   *
   * Use this to conditionally skip operations that shouldn't be repeated during replay,
   * such as logging or external notifications.
   *
   * Example:
   * ```kotlin
   * if (!KWorkflow.isReplaying) {
   *     logger.info("Processing order: $orderId")
   * }
   * ```
   */
  public val isReplaying: Boolean
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.isReplaying must be accessed from within workflow code")
      return context.isReplaying
    }

  // ==================== Metrics ====================

  /**
   * The metrics scope for this workflow.
   *
   * Use this to emit custom metrics from workflow code.
   *
   * Example:
   * ```kotlin
   * KWorkflow.metricsScope.counter("orders_processed").inc(1)
   * ```
   */
  public val metricsScope: Scope
    @JvmName("metricsScope")
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.metricsScope must be accessed from within workflow code")
      return context.getMetricsScope()
    }

  // ==================== Mutable Side Effect ====================

  /**
   * Executes a mutable side effect.
   *
   * Similar to [sideEffect], but only records a new marker if the value has changed.
   * The function receives the previous value (if any) and returns the new value.
   * This is useful for accessing dynamically changing configuration.
   *
   * Example:
   * ```kotlin
   * val config = KWorkflow.mutableSideEffect("config", Config::class.java) { previous ->
   *     loadConfigFromDatabase() // Only recorded if different from previous
   * }
   * ```
   *
   * @param id unique identifier for this mutable side effect
   * @param resultClass the expected result class
   * @param func function that takes the previous value and returns the new value
   * @return the result of the function
   */
  public fun <R> mutableSideEffect(
    id: String,
    resultClass: Class<R>,
    func: (R?) -> R
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.mutableSideEffect must be called from within workflow code")
    return context.mutableSideEffect(id, resultClass, func)
  }

  /**
   * Executes a mutable side effect using reified type.
   *
   * @param id unique identifier for this mutable side effect
   * @param func function that takes the previous value and returns the new value
   * @return the result of the function
   */
  public inline fun <reified R> mutableSideEffect(
    id: String,
    noinline func: (R?) -> R
  ): R {
    return mutableSideEffect(id, R::class.java, func)
  }

  // ==================== Retry ====================

  /**
   * Executes a block with retry logic according to the specified options.
   *
   * This is useful for retrying operations that may fail transiently.
   * The block is executed repeatedly until it succeeds, the maximum
   * attempts are exhausted, or the expiration timeout is reached.
   *
   * Note: Activities already have built-in retry options via [KActivityOptions].
   * This function is useful for:
   * - Retrying a sequence of operations as a unit
   * - Custom retry logic around non-activity operations
   * - Retrying local computations that may fail transiently
   *
   * Example:
   * ```kotlin
   * val result = KWorkflow.retry(
   *   KRetryOptions(
   *     initialInterval = 1.seconds,
   *     maximumInterval = 30.seconds,
   *     backoffCoefficient = 2.0,
   *     maximumAttempts = 5
   *   ),
   *   expiration = 5.minutes
   * ) {
   *   riskyOperation()
   * }
   * ```
   *
   * @param R the return type of the block
   * @param options the retry options specifying retry policy
   * @param expiration optional maximum time to retry (null means no limit)
   * @param block the suspend block to execute
   * @return the result of the block when it succeeds
   * @throws Exception the last exception if all retries are exhausted
   */
  public suspend fun <R> retry(
    options: KRetryOptions,
    expiration: Duration? = null,
    block: suspend () -> R
  ): R {
    val startTime = currentTimeMillis()
    var attempt = 0
    var lastException: Throwable
    var nextDelayMs = options.initialInterval.inWholeMilliseconds
    val maxIntervalMs = (options.maximumInterval ?: (options.initialInterval * 100)).inWholeMilliseconds

    while (true) {
      attempt++
      try {
        return block()
      } catch (e: Throwable) {
        // Check if this exception type should not be retried
        if (shouldNotRetry(e, options.doNotRetry)) {
          throw e
        }

        lastException = e

        // Check if we've exceeded maximum attempts
        if (options.maximumAttempts > 0 && attempt >= options.maximumAttempts) {
          throw lastException
        }

        // Check if we've exceeded expiration time
        if (expiration != null) {
          val elapsed = currentTimeMillis() - startTime
          if (elapsed >= expiration.inWholeMilliseconds) {
            throw lastException
          }
        }

        // Workflow-safe delay (intercepted by dispatcher to become Temporal timer)
        delay(nextDelayMs)

        // Calculate next delay with exponential backoff
        nextDelayMs = (nextDelayMs * options.backoffCoefficient).toLong()
          .coerceAtMost(maxIntervalMs)
      }
    }
  }

  /**
   * Checks if the exception matches any of the "do not retry" exception types.
   */
  private fun shouldNotRetry(exception: Throwable, doNotRetry: List<String>): Boolean {
    if (doNotRetry.isEmpty()) return false

    var current: Throwable? = exception
    while (current != null) {
      val className = current::class.java.name
      if (doNotRetry.any { className == it || className.endsWith(".$it") }) {
        return true
      }
      current = current.cause
    }
    return false
  }

  // ==================== Update Info ====================

  /**
   * Information about the currently executing update, if any.
   *
   * This is only available when called from within an update handler.
   * Returns null if called from the main workflow method or a signal handler.
   *
   * Example:
   * ```kotlin
   * @UpdateMethod
   * suspend fun processUpdate(data: String): String {
   *     val updateInfo = KWorkflow.currentUpdateInfo
   *     if (updateInfo != null) {
   *         logger.info("Processing update: ${updateInfo.updateId}")
   *     }
   *     return "processed"
   * }
   * ```
   */
  public val currentUpdateInfo: UpdateInfo?
    @JvmName("currentUpdateInfo")
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.currentUpdateInfo must be accessed from within workflow code")
      return context.getCurrentUpdateInfo()
    }

  /**
   * Returns information about the currently executing update, if any.
   *
   * @return the current update info, or null if not in an update handler
   */

  // ==================== Handler Completion Check ====================

  /**
   * Whether all signal and update handlers have completed.
   *
   * This is useful for ensuring graceful completion before continuing-as-new
   * or completing the workflow. You can use this with [awaitCondition] to
   * wait for all handlers to finish.
   *
   * Example:
   * ```kotlin
   * // Before continuing as new, wait for handlers to complete
   * KWorkflow.awaitCondition { KWorkflow.isEveryHandlerFinished }
   * KWorkflow.continueAsNew(newState)
   * ```
   */
  public val isEveryHandlerFinished: Boolean
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.isEveryHandlerFinished must be accessed from within workflow code")
      return context.isEveryHandlerFinished()
    }

  // ==================== Workflow Details ====================

  /**
   * The current workflow details.
   *
   * Details are user-defined strings that provide additional context about
   * the workflow's current state. They are visible in the Temporal UI and
   * can be retrieved via the describe workflow API.
   *
   * Example:
   * ```kotlin
   * KWorkflow.currentDetails = "Processing batch 5 of 10"
   * // ... do work ...
   * KWorkflow.currentDetails = "Waiting for approval"
   * ```
   */
  public var currentDetails: String?
    @JvmName("currentDetails")
    get() {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.currentDetails must be accessed from within workflow code")
      return context.getCurrentDetails()
    }

    @JvmName("currentDetails")
    set(value) {
      val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.currentDetails must be accessed from within workflow code")
      context.setCurrentDetails(value)
    }

  /**
   * Default version constant for workflow versioning.
   *
   * Use this as minSupported when introducing a new version
   * to allow workflows that haven't recorded any version yet.
   */
  public const val DEFAULT_VERSION: Int = Workflow.DEFAULT_VERSION

  /**
   * Executes an activity by name with no arguments and waits for the result.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeActivity(
   *   "myActivity",
   *   KActivityOptions(startToCloseTimeout = 5.minutes)
   * )
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param options the activity options
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend inline fun <reified R> executeActivity(
    activityName: String,
    options: KActivityOptions
  ): R {
    return executeActivityByName(activityName, R::class.java, options)
  }

  /**
   * Executes an activity by name with one argument and waits for the result.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeActivity(
   *   "myActivity",
   *   "inputArg",
   *   KActivityOptions(startToCloseTimeout = 5.minutes)
   * )
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param arg the argument to pass to the activity
   * @param options the activity options
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend inline fun <reified R> executeActivity(
    activityName: String,
    arg: Any?,
    options: KActivityOptions
  ): R {
    return executeActivityByName(activityName, R::class.java, options, arg)
  }

  /**
   * Executes an activity by name with multiple arguments and waits for the result.
   *
   * Example:
   * ```kotlin
   * val result: String = KWorkflow.executeActivity(
   *   "myActivity",
   *   listOf("arg1", 42),
   *   KActivityOptions(startToCloseTimeout = 5.minutes)
   * )
   * ```
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param args the arguments to pass to the activity
   * @param options the activity options
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  public suspend inline fun <reified R> executeActivity(
    activityName: String,
    args: List<Any?>,
    options: KActivityOptions
  ): R {
    return executeActivityByName(activityName, R::class.java, options, *args.toTypedArray())
  }

  /**
   * Executes an activity by name and waits for the result.
   *
   * Internal implementation that takes the result class explicitly.
   *
   * @param R the expected return type of the activity
   * @param activityName the name of the activity to execute
   * @param resultClass the class of the expected result type
   * @param options the activity options
   * @param args arguments to pass to the activity
   * @return the activity result
   * @throws ActivityException if the activity fails
   */
  @PublishedApi
  internal suspend fun <R> executeActivityByName(
    activityName: String,
    resultClass: Class<R>,
    options: KActivityOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeActivity must be called from within workflow code")
    return context.executeActivityByName(activityName, KOptionsConverters.toJava(options), resultClass, *args)
  }

  // ==================== Typed Activity Execution (Method Reference) ====================

  /**
   * Executes an activity using a method reference and waits for the result.
   *
   * This provides compile-time type safety for activity arguments and return types.
   *
   * Example:
   * ```kotlin
   * val result = KWorkflow.executeActivity(
   *   GreetingActivities::composeGreeting,
   *   KActivityOptions(startToCloseTimeout = 30.seconds),
   *   "Hello", "World"
   * )
   * ```
   *
   * @param T the activity interface type
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param options the activity options
   * @return the activity result
   */
  public suspend fun <T, R> executeActivity(
    activity: KFunction1<T, R>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options)
  }

  /**
   * Executes an activity using a method reference with 1 argument.
   *
   * @param T the activity interface type
   * @param A1 the type of the first argument
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param arg1 the first argument
   * @param options the activity options
   * @return the activity result
   */
  public suspend fun <T, A1, R> executeActivity(
    activity: KFunction2<T, A1, R>,
    arg1: A1,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes an activity using a method reference with 2 arguments.
   *
   * @param T the activity interface type
   * @param A1 the type of the first argument
   * @param A2 the type of the second argument
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param args the arguments wrapped in KArgs2
   * @param options the activity options
   * @return the activity result
   */
  public suspend fun <T, A1, A2, R> executeActivity(
    activity: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes an activity using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeActivity(
    activity: KFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes an activity using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeActivity(
    activity: KFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes an activity using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeActivity(
    activity: KFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes an activity using a method reference with 6 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeActivity(
    activity: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  // ==================== Typed Suspend Activity Execution (Method Reference) ====================
  // These overloads support suspend activity methods (suspend fun in interfaces)

  /**
   * Executes a suspend activity using a method reference and waits for the result.
   *
   * Use this overload when the activity method is declared as a suspend function.
   *
   * @param T the activity interface type
   * @param R the return type of the activity
   * @param activity the suspend activity method reference
   * @param options the activity options
   * @return the activity result
   */
  @JvmName("executeSuspendActivity0")
  public suspend fun <T, R> executeActivity(
    activity: KSuspendFunction1<T, R>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options)
  }

  /**
   * Executes a suspend activity using a method reference with 1 argument.
   */
  @JvmName("executeSuspendActivity1")
  public suspend fun <T, A1, R> executeActivity(
    activity: KSuspendFunction2<T, A1, R>,
    arg1: A1,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes a suspend activity using a method reference with 2 arguments.
   */
  @JvmName("executeSuspendActivity2")
  public suspend fun <T, A1, A2, R> executeActivity(
    activity: KSuspendFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend activity using a method reference with 3 arguments.
   */
  @JvmName("executeSuspendActivity3")
  public suspend fun <T, A1, A2, A3, R> executeActivity(
    activity: KSuspendFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend activity using a method reference with 4 arguments.
   */
  @JvmName("executeSuspendActivity4")
  public suspend fun <T, A1, A2, A3, A4, R> executeActivity(
    activity: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend activity using a method reference with 5 arguments.
   */
  @JvmName("executeSuspendActivity5")
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeActivity(
    activity: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend activity using a method reference with 6 arguments.
   */
  @JvmName("executeSuspendActivity6")
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeActivity(
    activity: KSuspendFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>,
    options: KActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivityByName(activityName, resultClass as Class<R>, options, *args.toArray())
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
   * KWorkflow.awaitCondition { approved }
   * // Continues after approved becomes true
   * ```
   *
   * @param condition the condition to wait for
   */
  public suspend fun awaitCondition(condition: () -> Boolean) {
    val context = currentContext.get()
    if (context != null) {
      // Use our Kotlin-native condition waiting
      context.awaitCondition(condition)
    } else {
      // Fallback to Java SDK (for non-suspend workflows)
      Workflow.await { condition() }
    }
  }

  /**
   * Suspends until the given condition evaluates to true or the timeout expires.
   *
   * @param timeout maximum time to wait for the condition
   * @param condition the condition to wait for
   * @return true if condition was satisfied, false if timeout expired
   */
  public suspend fun awaitCondition(timeout: Duration, condition: () -> Boolean): Boolean {
    return awaitCondition(timeout.toJava(), condition)
  }

  /**
   * Suspends until the given condition evaluates to true or the timeout expires.
   *
   * @param timeout maximum time to wait for the condition (Java Duration)
   * @param condition the condition to wait for
   * @return true if condition was satisfied, false if timeout expired
   */
  public suspend fun awaitCondition(timeout: java.time.Duration, condition: () -> Boolean): Boolean {
    val context = currentContext.get()
    return if (context != null) {
      // Use our Kotlin-native condition waiting with timeout
      context.awaitCondition(timeout, condition)
    } else {
      // Fallback to Java SDK (for non-suspend workflows)
      Workflow.await(timeout) { condition() }
    }
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
   *   options = KLocalActivityOptions(startToCloseTimeout = 5.seconds),
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
    options: KLocalActivityOptions,
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
    options: KLocalActivityOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeLocalActivity must be called from within workflow code")
    return context.executeLocalActivityByName(activityName, KOptionsConverters.toJava(options), resultClass, *args)
  }

  // ==================== Typed Local Activity Execution (Method Reference) ====================

  /**
   * Executes a local activity using a method reference and waits for the result.
   *
   * @param T the activity interface type
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param options the local activity options
   * @return the activity result
   */
  public suspend fun <T, R> executeLocalActivity(
    activity: KFunction1<T, R>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options)
  }

  /**
   * Executes a local activity using a method reference with 1 argument.
   */
  public suspend fun <T, A1, R> executeLocalActivity(
    activity: KFunction2<T, A1, R>,
    arg1: A1,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes a local activity using a method reference with 2 arguments.
   */
  public suspend fun <T, A1, A2, R> executeLocalActivity(
    activity: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a local activity using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeLocalActivity(
    activity: KFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a local activity using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeLocalActivity(
    activity: KFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a local activity using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeLocalActivity(
    activity: KFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a local activity using a method reference with 6 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeLocalActivity(
    activity: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  // ==================== Typed Suspend Local Activity Execution (Method Reference) ====================
  // These overloads support suspend local activity methods (suspend fun in interfaces)

  /**
   * Executes a suspend local activity using a method reference and waits for the result.
   *
   * Use this overload when the activity method is declared as a suspend function.
   *
   * @param T the activity interface type
   * @param R the return type of the activity
   * @param activity the suspend activity method reference
   * @param options the local activity options
   * @return the activity result
   */
  @JvmName("executeSuspendLocalActivity0")
  public suspend fun <T, R> executeLocalActivity(
    activity: KSuspendFunction1<T, R>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options)
  }

  /**
   * Executes a suspend local activity using a method reference with 1 argument.
   */
  @JvmName("executeSuspendLocalActivity1")
  public suspend fun <T, A1, R> executeLocalActivity(
    activity: KSuspendFunction2<T, A1, R>,
    arg1: A1,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes a suspend local activity using a method reference with 2 arguments.
   */
  @JvmName("executeSuspendLocalActivity2")
  public suspend fun <T, A1, A2, R> executeLocalActivity(
    activity: KSuspendFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend local activity using a method reference with 3 arguments.
   */
  @JvmName("executeSuspendLocalActivity3")
  public suspend fun <T, A1, A2, A3, R> executeLocalActivity(
    activity: KSuspendFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend local activity using a method reference with 4 arguments.
   */
  @JvmName("executeSuspendLocalActivity4")
  public suspend fun <T, A1, A2, A3, A4, R> executeLocalActivity(
    activity: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend local activity using a method reference with 5 arguments.
   */
  @JvmName("executeSuspendLocalActivity5")
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeLocalActivity(
    activity: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend local activity using a method reference with 6 arguments.
   */
  @JvmName("executeSuspendLocalActivity6")
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeLocalActivity(
    activity: KSuspendFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>,
    options: KLocalActivityOptions
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, *args.toArray())
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
   *   options = KChildWorkflowOptions(workflowId = "child-workflow-id"),
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
    options: KChildWorkflowOptions,
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
    options: KChildWorkflowOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeChildWorkflow must be called from within workflow code")
    return context.executeChildWorkflowByName(workflowType, KOptionsConverters.toJava(options), resultClass, *args)
  }

  // ==================== Typed Child Workflow Execution (Method Reference) ====================

  /**
   * Executes a child workflow using a method reference and waits for the result.
   *
   * Example:
   * ```kotlin
   * val result = KWorkflow.executeChildWorkflow(
   *   ChildWorkflow::processOrder,
   *   KChildWorkflowOptions(workflowId = "child-123"),
   *   order
   * )
   * ```
   *
   * @param T the workflow interface type
   * @param R the return type of the workflow
   * @param workflow the workflow method reference
   * @param options the child workflow options
   * @return the child workflow result
   */
  public suspend fun <T, R> executeChildWorkflow(
    workflow: KFunction1<T, R>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options)
  }

  /**
   * Executes a child workflow using a method reference with 1 argument.
   */
  public suspend fun <T, A1, R> executeChildWorkflow(
    workflow: KFunction2<T, A1, R>,
    arg1: A1,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes a child workflow using a method reference with 2 arguments.
   */
  public suspend fun <T, A1, A2, R> executeChildWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a child workflow using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeChildWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a child workflow using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeChildWorkflow(
    workflow: KFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a child workflow using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeChildWorkflow(
    workflow: KFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  // ==================== Typed Suspend Child Workflow Execution (Method Reference) ====================
  // These overloads support suspend child workflow methods (suspend fun in interfaces)

  /**
   * Executes a suspend child workflow using a method reference and waits for the result.
   *
   * Use this overload when the child workflow method is declared as a suspend function.
   *
   * @param T the workflow interface type
   * @param R the return type of the workflow
   * @param workflow the suspend workflow method reference
   * @param options the child workflow options
   * @return the child workflow result
   */
  @JvmName("executeSuspendChildWorkflow0")
  public suspend fun <T, R> executeChildWorkflow(
    workflow: KSuspendFunction1<T, R>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options)
  }

  /**
   * Executes a suspend child workflow using a method reference with 1 argument.
   */
  @JvmName("executeSuspendChildWorkflow1")
  public suspend fun <T, A1, R> executeChildWorkflow(
    workflow: KSuspendFunction2<T, A1, R>,
    arg1: A1,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes a suspend child workflow using a method reference with 2 arguments.
   */
  @JvmName("executeSuspendChildWorkflow2")
  public suspend fun <T, A1, A2, R> executeChildWorkflow(
    workflow: KSuspendFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend child workflow using a method reference with 3 arguments.
   */
  @JvmName("executeSuspendChildWorkflow3")
  public suspend fun <T, A1, A2, A3, R> executeChildWorkflow(
    workflow: KSuspendFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend child workflow using a method reference with 4 arguments.
   */
  @JvmName("executeSuspendChildWorkflow4")
  public suspend fun <T, A1, A2, A3, A4, R> executeChildWorkflow(
    workflow: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    args: KArgs4<A1, A2, A3, A4>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  /**
   * Executes a suspend child workflow using a method reference with 5 arguments.
   */
  @JvmName("executeSuspendChildWorkflow5")
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeChildWorkflow(
    workflow: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    args: KArgs5<A1, A2, A3, A4, A5>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, *args.toArray())
  }

  // ==================== Child Workflow Handle Methods ====================

  /**
   * Starts a child workflow and returns a handle for interaction.
   *
   * Use this when you need to signal, query, or cancel the child workflow
   * while it's running. For simple fire-and-wait cases, prefer
   * [executeChildWorkflow] instead.
   *
   * Example:
   * ```kotlin
   * val handle = KWorkflow.startChildWorkflow(
   *   ChildWorkflow::processOrder,
   *   order,
   *   KChildWorkflowOptions(workflowId = "child-123")
   * )
   * handle.signal(ChildWorkflow::updatePriority, Priority.HIGH)
   * val result = handle.result()
   * ```
   *
   * @param T the workflow interface type
   * @param R the return type of the workflow
   * @param workflow the workflow method reference
   * @param options the child workflow options
   * @return a handle for interacting with the child workflow
   */
  public suspend fun <T, R> startChildWorkflow(
    workflow: KFunction1<T, R>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, KOptionsConverters.toJava(options), resultClass as Class<R>)
  }

  /**
   * Starts a child workflow with 1 argument and returns a handle.
   */
  public suspend fun <T, A1, R> startChildWorkflow(
    workflow: KFunction2<T, A1, R>,
    arg1: A1,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, KOptionsConverters.toJava(options), resultClass as Class<R>, arg1)
  }

  /**
   * Starts a child workflow with 2 arguments and returns a handle.
   */
  public suspend fun <T, A1, A2, R> startChildWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, KOptionsConverters.toJava(options), resultClass as Class<R>, *args.toArray())
  }

  /**
   * Starts a child workflow with 3 arguments and returns a handle.
   */
  public suspend fun <T, A1, A2, A3, R> startChildWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    args: KArgs3<A1, A2, A3>,
    options: KChildWorkflowOptions = KChildWorkflowOptions()
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, KOptionsConverters.toJava(options), resultClass as Class<R>, *args.toArray())
  }

  /**
   * Gets a handle to an existing child workflow by workflow ID.
   *
   * Use this to interact with a child workflow started earlier in the
   * same workflow execution.
   *
   * @param T the child workflow interface type
   * @param R the expected result type
   * @param workflowId the child workflow's workflow ID
   * @return a handle for interacting with the child workflow
   */
  public inline fun <reified T, reified R> getChildWorkflowHandle(
    workflowId: String
  ): KChildWorkflowHandle<T, R> {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.getChildWorkflowHandle must be called from within workflow code")
    return context.getChildWorkflowHandle(workflowId, R::class.java)
  }

  // ==================== Signal Handler Registration ====================

  /**
   * Registers a signal handler for a specific signal name.
   *
   * Signal handlers are invoked when the workflow receives a signal with the
   * matching name. The handler receives the signal arguments as [KEncodedValues]
   * which can be decoded to the expected types.
   *
   * Example:
   * ```kotlin
   * class MyWorkflowImpl : MyWorkflow {
   *   private var approved = false
   *
   *   override suspend fun execute(): String {
   *     // Register signal handler
   *     KWorkflow.registerSignalHandler("approve") { args ->
   *       approved = args.get<Boolean>(0)
   *     }
   *
   *     // Wait for approval
   *     KWorkflow.awaitCondition { approved }
   *     return "Approved!"
   *   }
   * }
   * ```
   *
   * @param signalName the name of the signal to handle
   * @param handler the suspend function to invoke when the signal is received
   * @throws IllegalArgumentException if a handler is already registered for this signal
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun registerSignalHandler(
    signalName: String,
    handler: suspend (KEncodedValues) -> Unit
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerSignalHandler must be called from within workflow code")
    context.registerSignalHandler(signalName, handler)
  }

  /**
   * Registers a signal handler with no arguments for a specific signal name.
   *
   * This is a convenience method for signals that don't require arguments.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerSignalHandler("cancel") {
   *   shouldCancel = true
   * }
   * ```
   *
   * @param signalName the name of the signal to handle
   * @param handler the suspend function to invoke when the signal is received
   */
  public fun registerSignalHandler(
    signalName: String,
    handler: suspend () -> Unit
  ) {
    registerSignalHandler(signalName) { _ -> handler() }
  }

  /**
   * Registers a dynamic signal handler for all unhandled signals.
   *
   * The dynamic handler is invoked for any signal that doesn't have a specific
   * handler registered. Only one dynamic handler can be registered per workflow.
   *
   * Any signals that were buffered before this handler was registered (e.g., from
   * signalWithStart) will be replayed to the handler.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerDynamicSignalHandler { signalName, args ->
   *   println("Received signal: $signalName with ${args.size} arguments")
   * }
   * ```
   *
   * @param handler the suspend function to invoke for unhandled signals
   * @throws IllegalArgumentException if a dynamic handler is already registered
   * @throws IllegalStateException if called outside of workflow code
   */
  public suspend fun registerDynamicSignalHandler(
    handler: suspend (signalName: String, args: KEncodedValues) -> Unit
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicSignalHandler must be called from within workflow code")
    context.registerDynamicSignalHandler(handler)
  }

  // ==================== Query Handler Registration ====================

  /**
   * Registers a query handler for a specific query name.
   *
   * Query handlers are invoked synchronously when the workflow is queried.
   * They must NOT be suspend functions and should return quickly without
   * blocking or performing side effects.
   *
   * Example:
   * ```kotlin
   * class MyWorkflowImpl : MyWorkflow {
   *   private var status = "pending"
   *
   *   override suspend fun execute(): String {
   *     // Register query handler
   *     KWorkflow.registerQueryHandler<String>("getStatus") { args ->
   *       status
   *     }
   *
   *     // ... workflow logic ...
   *     return "done"
   *   }
   * }
   * ```
   *
   * @param R the return type of the query
   * @param queryName the name of the query to handle
   * @param handler the function to invoke when the query is received
   * @throws IllegalArgumentException if a handler is already registered for this query
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun <R> registerQueryHandler(
    queryName: String,
    handler: (KEncodedValues) -> R
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerQueryHandler must be called from within workflow code")
    context.registerQueryHandler(queryName, handler)
  }

  /**
   * Registers a query handler with no arguments for a specific query name.
   *
   * This is a convenience method for queries that don't require arguments.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerQueryHandler<String>("getStatus") {
   *   currentStatus
   * }
   * ```
   *
   * @param R the return type of the query
   * @param queryName the name of the query to handle
   * @param handler the function to invoke when the query is received
   */
  public fun <R> registerQueryHandler(
    queryName: String,
    handler: () -> R
  ) {
    registerQueryHandler<R>(queryName) { _ -> handler() }
  }

  /**
   * Registers a dynamic query handler for all unhandled queries.
   *
   * The dynamic handler is invoked for any query that doesn't have a specific
   * handler registered. Only one dynamic handler can be registered per workflow.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerDynamicQueryHandler { queryName, args ->
   *   when (queryName) {
   *     "status" -> currentStatus
   *     "count" -> itemCount
   *     else -> "Unknown query: $queryName"
   *   }
   * }
   * ```
   *
   * @param handler the function to invoke for unhandled queries
   * @throws IllegalArgumentException if a dynamic handler is already registered
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun registerDynamicQueryHandler(
    handler: (queryName: String, args: KEncodedValues) -> Any?
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicQueryHandler must be called from within workflow code")
    context.registerDynamicQueryHandler(handler)
  }

  // ==================== Update Handler Registration ====================

  // Update handlers in Kotlin workflows should be suspend functions.
  // This allows them to use KWorkflow.executeActivity, KWorkflow.delay,
  // and other suspend-based workflow APIs.
  //
  // Example using @UpdateMethod annotation:
  // ```kotlin
  // @WorkflowInterface
  // interface MyWorkflow {
  //   @UpdateMethod
  //   suspend fun processUpdate(data: String): String
  //
  //   @UpdateValidatorMethod(updateName = "processUpdate")
  //   fun validateUpdate(data: String) // Validators are NOT suspend
  // }
  //
  // class MyWorkflowImpl : MyWorkflow {
  //   override suspend fun processUpdate(data: String): String {
  //     // Can call activities, delay, etc. because this is a suspend function
  //     return KWorkflow.executeActivity(
  //       Activities::process,
  //       KActivityOptions(startToCloseTimeout = 30.seconds),
  //       data
  //     )
  //   }
  //
  //   override fun validateUpdate(data: String) {
  //     require(data.isNotEmpty()) { "Data cannot be empty" }
  //   }
  // }
  // ```

  /**
   * Registers a dynamic update handler for all unhandled updates.
   *
   * The dynamic handler is invoked for any update that doesn't have a specific
   * handler registered via @UpdateMethod annotation. Only one dynamic handler
   * can be registered per workflow.
   *
   * Update handlers should be suspend functions to allow calling activities,
   * child workflows, and other workflow operations.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerDynamicUpdateHandler { updateName, args ->
   *   when (updateName) {
   *     "setConfig" -> {
   *       val newConfig = args.get<Config>(0)
   *       // Can call activities since this is a suspend function
   *       KWorkflow.executeActivity(
   *         ConfigActivities::validateAndApply,
   *         KActivityOptions(startToCloseTimeout = 30.seconds),
   *         newConfig
   *       )
   *       config = newConfig
   *       "Config updated"
   *     }
   *     else -> throw IllegalArgumentException("Unknown update: $updateName")
   *   }
   * }
   * ```
   *
   * @param handler the suspend function to invoke for unhandled updates
   * @throws IllegalArgumentException if a dynamic handler is already registered
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun registerDynamicUpdateHandler(
    handler: suspend (updateName: String, args: KEncodedValues) -> Any?
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicUpdateHandler must be called from within workflow code")
    context.registerDynamicUpdateHandler(handler)
  }

  /**
   * Registers a dynamic update validator for all unhandled updates.
   *
   * The validator is invoked before the update handler to validate inputs.
   * If the validator throws an exception, the update is rejected.
   *
   * @param validator the function to validate update inputs
   */
  public fun registerDynamicUpdateValidator(
    validator: (updateName: String, args: KEncodedValues) -> Unit
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicUpdateValidator must be called from within workflow code")
    context.registerDynamicUpdateValidator(validator)
  }

  /**
   * Registers an update handler for a specific update name.
   *
   * Update handlers are invoked when the workflow receives an update with the
   * matching name. The handler receives the update arguments as [KEncodedValues]
   * which can be decoded to the expected types, and returns a result.
   *
   * Update handlers should be suspend functions to allow calling activities,
   * child workflows, and other workflow operations.
   *
   * Example:
   * ```kotlin
   * class MyWorkflowImpl : MyWorkflow {
   *   private var config: Config = Config.default()
   *
   *   override suspend fun execute(): String {
   *     // Register update handler
   *     KWorkflow.registerUpdateHandler("updateConfig") { args ->
   *       val newConfig = args.get<Config>(0)
   *       // Can call activities since this is a suspend function
   *       KWorkflow.executeActivity(
   *         ConfigActivities::validateConfig,
   *         KActivityOptions(startToCloseTimeout = 30.seconds),
   *         newConfig
   *       )
   *       config = newConfig
   *       "Config updated successfully"
   *     }
   *
   *     // ... workflow logic ...
   *     return "done"
   *   }
   * }
   * ```
   *
   * @param updateName the name of the update to handle
   * @param handler the suspend function to invoke when the update is received
   * @throws IllegalArgumentException if a handler is already registered for this update
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun registerUpdateHandler(
    updateName: String,
    handler: suspend (KEncodedValues) -> Any?
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerUpdateHandler must be called from within workflow code")
    context.registerUpdateHandler(updateName, handler)
  }

  /**
   * Registers an update handler with no arguments for a specific update name.
   *
   * This is a convenience method for updates that don't require arguments.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerUpdateHandler("reset") {
   *   state = initialState
   *   "State reset"
   * }
   * ```
   *
   * @param updateName the name of the update to handle
   * @param handler the suspend function to invoke when the update is received
   */
  public fun registerUpdateHandler(
    updateName: String,
    handler: suspend () -> Any?
  ) {
    registerUpdateHandler(updateName) { _ -> handler() }
  }

  /**
   * Registers an update handler with a validator for a specific update name.
   *
   * The validator is invoked before the update handler to validate inputs.
   * If the validator throws an exception, the update is rejected without
   * executing the handler.
   *
   * Example:
   * ```kotlin
   * KWorkflow.registerUpdateHandler(
   *   "updateConfig",
   *   validator = { args ->
   *     val config = args.get<Config>(0)
   *     require(config.isValid) { "Invalid config" }
   *   },
   *   handler = { args ->
   *     val config = args.get<Config>(0)
   *     currentConfig = config
   *     "Config updated"
   *   }
   * )
   * ```
   *
   * @param updateName the name of the update to handle
   * @param validator the function to validate update inputs (NOT suspend, must return quickly)
   * @param handler the suspend function to invoke when the update is received
   * @throws IllegalArgumentException if a handler is already registered for this update
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun registerUpdateHandler(
    updateName: String,
    validator: (KEncodedValues) -> Unit,
    handler: suspend (KEncodedValues) -> Any?
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerUpdateHandler must be called from within workflow code")
    context.registerUpdateValidator(updateName, validator)
    context.registerUpdateHandler(updateName, handler)
  }

  // ==================== Continue-As-New ====================

  /**
   * Continues the workflow execution as a new run with the same workflow type.
   *
   * Continue-as-new completes the current workflow execution and immediately starts
   * a new execution with fresh event history. This is useful for:
   *
   * - **Preventing history growth**: Long-running workflows accumulate event history.
   *   Continue-as-new resets the history, preventing performance degradation.
   * - **Periodic processing**: Workflows that process batches can continue-as-new
   *   after each batch to maintain a clean state.
   * - **Implementing loops**: Instead of infinite loops, use continue-as-new to
   *   implement recurring behavior without history buildup.
   *
   * **Important**: This function never returns normally. It terminates the current
   * workflow execution and signals the Temporal runtime to start a new execution.
   *
   * Example - Batch processing workflow:
   * ```kotlin
   * @WorkflowInterface
   * interface BatchProcessor {
   *   @WorkflowMethod
   *   suspend fun processBatches(startOffset: Int)
   * }
   *
   * class BatchProcessorImpl : BatchProcessor {
   *   override suspend fun processBatches(startOffset: Int) {
   *     val batchSize = 100
   *     val items = KWorkflow.executeActivity(
   *       DataActivities::fetchBatch,
   *       KActivityOptions(startToCloseTimeout = 1.minutes),
   *       startOffset, batchSize
   *     )
   *
   *     if (items.isEmpty()) {
   *       return // All done, workflow completes normally
   *     }
   *
   *     // Process items...
   *     for (item in items) {
   *       KWorkflow.executeActivity(
   *         DataActivities::processItem,
   *         KActivityOptions(startToCloseTimeout = 30.seconds),
   *         item
   *       )
   *     }
   *
   *     // Continue with the next batch
   *     KWorkflow.continueAsNew(startOffset + batchSize)
   *   }
   * }
   * ```
   *
   * Example - Long-running workflow with history check:
   * ```kotlin
   * override suspend fun execute(state: WorkflowState) {
   *   while (true) {
   *     // Check if history is getting too large
   *     if (KWorkflow.getInfo().isContinueAsNewSuggested) {
   *       KWorkflow.continueAsNew(state)
   *     }
   *
   *     // Wait for signals and process...
   *     KWorkflow.awaitCondition { hasNewWork }
   *     processWork()
   *   }
   * }
   * ```
   *
   * @param args Arguments to pass to the new workflow execution
   */
  public fun continueAsNew(vararg args: Any?): Nothing {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(null, null, *args)
  }

  /**
   * Continues the workflow execution as a new run with the same workflow type
   * but with modified options.
   *
   * This variant allows you to change execution parameters like task queue,
   * timeouts, or retry options for the new execution.
   *
   * **Important**: This function never returns normally. It terminates the current
   * workflow execution and signals the Temporal runtime to start a new execution.
   *
   * Example - Changing task queue:
   * ```kotlin
   * // Move to a different task queue for the next execution
   * KWorkflow.continueAsNew(
   *   KContinueAsNewOptions(taskQueue = "high-priority-queue"),
   *   nextBatchId
   * )
   * ```
   *
   * Example - Adjusting timeout:
   * ```kotlin
   * // Give more time for larger batches
   * KWorkflow.continueAsNew(
   *   KContinueAsNewOptions(workflowRunTimeout = 2.hours),
   *   largeBatchData
   * )
   * ```
   *
   * @param options Options to override for the new execution. Null values inherit
   *   from the current execution.
   * @param args Arguments to pass to the new workflow execution
   */
  public fun continueAsNew(options: KContinueAsNewOptions, vararg args: Any?): Nothing {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(null, KOptionsConverters.toJava(options), *args)
  }

  /**
   * Continues as a different workflow type with specified options.
   *
   * This variant allows you to continue as a completely different workflow type,
   * which is useful for:
   * - **Workflow versioning**: Migrating to a new workflow implementation
   * - **Workflow chaining**: Transitioning to a different processing phase
   *
   * **Important**: This function never returns normally. It terminates the current
   * workflow execution and signals the Temporal runtime to start a new execution.
   *
   * Example - Version migration:
   * ```kotlin
   * // Continue as new version of the workflow
   * KWorkflow.continueAsNew(
   *   "OrderProcessorV2",
   *   KContinueAsNewOptions(taskQueue = "orders-v2"),
   *   orderId, migratedState
   * )
   * ```
   *
   * @param workflowType The workflow type name for the new execution
   * @param options Options to override for the new execution. Null values inherit
   *   from the current execution.
   * @param args Arguments to pass to the new workflow execution
   */
  public fun continueAsNew(
    workflowType: String,
    options: KContinueAsNewOptions,
    vararg args: Any?
  ): Nothing {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(workflowType, KOptionsConverters.toJava(options), *args)
  }

  /**
   * Continues as a different workflow type using a method reference.
   *
   * This provides compile-time type safety when continuing as a different
   * workflow type.
   *
   * **Important**: This function never returns normally. It terminates the current
   * workflow execution and signals the Temporal runtime to start a new execution.
   *
   * Example:
   * ```kotlin
   * // Type-safe continue as different workflow
   * KWorkflow.continueAsNew(
   *   OrderProcessorV2::process,
   *   KContinueAsNewOptions(),
   *   orderId, updatedState
   * )
   * ```
   *
   * @param T the workflow interface type
   * @param workflow the workflow method reference
   * @param options Options to override for the new execution
   * @param args Arguments to pass to the new workflow execution
   */
  public fun <T> continueAsNew(
    workflow: KFunction<*>,
    options: KContinueAsNewOptions,
    vararg args: Any?
  ): Nothing {
    val (workflowType, _) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(workflowType, KOptionsConverters.toJava(options), *args)
  }

  // ==================== External Workflow Handles ====================

  /**
   * Gets a typed handle for an external workflow by workflow ID.
   *
   * External workflows can be signaled and cancelled, but their results cannot
   * be awaited from within a workflow (use the client API for that).
   *
   * Example:
   * ```kotlin
   * val handle = KWorkflow.getExternalWorkflowHandle<OrderWorkflow>("order-123")
   * handle.signal(OrderWorkflow::updatePriority, Priority.HIGH)
   * handle.cancel()
   * ```
   *
   * @param T the external workflow interface type
   * @param workflowId the ID of the external workflow
   * @return a typed handle for interacting with the external workflow
   */
  public inline fun <reified T : Any> getExternalWorkflowHandle(workflowId: String): KExternalWorkflowHandle<T> {
    return getExternalWorkflowHandle(T::class.java, workflowId)
  }

  /**
   * Gets a typed handle for an external workflow by workflow ID and run ID.
   *
   * @param T the external workflow interface type
   * @param workflowId the ID of the external workflow
   * @param runId the run ID of the specific execution
   * @return a typed handle for interacting with the external workflow
   */
  public inline fun <reified T : Any> getExternalWorkflowHandle(workflowId: String, runId: String): KExternalWorkflowHandle<T> {
    return getExternalWorkflowHandle(T::class.java, workflowId, runId)
  }

  /**
   * Gets a typed handle for an external workflow by workflow ID.
   *
   * @param workflowInterface the external workflow interface class
   * @param workflowId the ID of the external workflow
   * @return a typed handle for interacting with the external workflow
   */
  @PublishedApi
  internal fun <T : Any> getExternalWorkflowHandle(
    workflowInterface: Class<T>,
    workflowId: String
  ): KExternalWorkflowHandle<T> {
    val typedStub = Workflow.newExternalWorkflowStub(workflowInterface, workflowId)
    val untypedStub = io.temporal.workflow.ExternalWorkflowStub.fromTyped(typedStub)
    return KExternalWorkflowHandle(untypedStub, workflowInterface)
  }

  /**
   * Gets a typed handle for an external workflow by workflow ID and run ID.
   *
   * @param workflowInterface the external workflow interface class
   * @param workflowId the ID of the external workflow
   * @param runId the run ID of the specific execution
   * @return a typed handle for interacting with the external workflow
   */
  @PublishedApi
  internal fun <T : Any> getExternalWorkflowHandle(
    workflowInterface: Class<T>,
    workflowId: String,
    runId: String
  ): KExternalWorkflowHandle<T> {
    val execution = io.temporal.api.common.v1.WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .setRunId(runId)
      .build()
    val typedStub = Workflow.newExternalWorkflowStub(workflowInterface, execution)
    val untypedStub = io.temporal.workflow.ExternalWorkflowStub.fromTyped(typedStub)
    return KExternalWorkflowHandle(untypedStub, workflowInterface)
  }

  /**
   * Gets an untyped handle for an external workflow by workflow ID.
   *
   * Use this when the workflow type is not known at compile time.
   *
   * Example:
   * ```kotlin
   * val handle = KWorkflow.getExternalWorkflowHandle("order-123")
   * handle.signal("updatePriority", Priority.HIGH)
   * handle.cancel()
   * ```
   *
   * @param workflowId the ID of the external workflow
   * @return an untyped handle for interacting with the external workflow
   */
  public fun getExternalWorkflowHandle(workflowId: String): KUntypedExternalWorkflowHandle {
    val stub = Workflow.newUntypedExternalWorkflowStub(workflowId)
    return KUntypedExternalWorkflowHandle(stub)
  }

  /**
   * Gets an untyped handle for an external workflow by workflow ID and run ID.
   *
   * @param workflowId the ID of the external workflow
   * @param runId the run ID of the specific execution
   * @return an untyped handle for interacting with the external workflow
   */
  public fun getExternalWorkflowHandle(workflowId: String, runId: String): KUntypedExternalWorkflowHandle {
    val execution = io.temporal.api.common.v1.WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .setRunId(runId)
      .build()
    val stub = Workflow.newUntypedExternalWorkflowStub(execution)
    return KUntypedExternalWorkflowHandle(stub)
  }

  // ==================== Internal Helper Functions ====================

  /**
   * Extracts activity name and return type from a KFunction reference.
   */
  private fun extractActivityMetadata(activity: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = activity.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve activity method reference")

    // Check for @ActivityMethod annotation for custom name
    val activityMethod = javaMethod.getAnnotation(ActivityMethod::class.java)
    val activityName = if (activityMethod != null && activityMethod.name.isNotEmpty()) {
      activityMethod.name
    } else {
      // Default to method name with first letter capitalized (Temporal convention)
      javaMethod.name.replaceFirstChar { it.uppercase() }
    }

    val returnType = javaMethod.returnType
    return Pair(activityName, returnType)
  }

  /**
   * Extracts workflow type name and return type from a KFunction reference.
   */
  private fun extractWorkflowMetadata(workflow: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = workflow.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve workflow method reference")

    // Check for @WorkflowMethod annotation for custom name
    val workflowMethod = javaMethod.getAnnotation(WorkflowMethod::class.java)
    val workflowType = if (workflowMethod != null && workflowMethod.name.isNotEmpty()) {
      workflowMethod.name
    } else {
      // Default to declaring class simple name (Temporal convention)
      javaMethod.declaringClass.simpleName
    }

    val returnType = javaMethod.returnType
    return Pair(workflowType, returnType)
  }
}

/**
 * Converts this [Promise] to a standard [Deferred].
 *
 * This extension is marked as internal API since it extends the Java SDK Promise type.
 *
 * This allows Temporal promises to work with all standard kotlinx.coroutines
 * utilities like [awaitAll], structured concurrency, etc.
 *
 * Example:
 * ```kotlin
 * coroutineScope {
 *   val d1 = async { executeActivity<Int>("Op1", options) }
 *   val d2 = someJavaApi().toDeferred()
 *   awaitAll(d1, d2)
 * }
 * ```
 *
 * @return a [Deferred] that completes when this promise completes
 */
@InternalTemporalApi
public fun <R> Promise<R>.toDeferred(): Deferred<R> {
  val deferred = CompletableDeferred<R>()
  this.handle { result, exception ->
    if (exception != null) {
      deferred.completeExceptionally(exception)
    } else {
      deferred.complete(result)
    }
    null // Return value required by handle but not used
  }
  return deferred
}

/**
 * Suspends until this [Promise] completes and returns the result.
 *
 * This extension is marked as internal API since it extends the Java SDK Promise type.
 *
 * This is a convenience extension that converts the Promise to a Deferred
 * and awaits it.
 *
 * @return the promise result
 * @throws Exception if the promise completed exceptionally
 */
@InternalTemporalApi
public suspend fun <R> Promise<R>.await(): R = toDeferred().await()
