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
import io.temporal.common.converter.EncodedValues
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.kotlin.toJava
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInfo
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import java.time.Instant
import java.util.Random
import java.util.UUID
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

  // ==================== Signal Handler Registration ====================

  /**
   * Registers a signal handler for a specific signal name.
   *
   * Signal handlers are invoked when the workflow receives a signal with the
   * matching name. The handler receives the signal arguments as [EncodedValues]
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
   *       approved = args.get(0, Boolean::class.java)
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
    handler: suspend (EncodedValues) -> Unit
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
  public fun registerDynamicSignalHandler(
    handler: suspend (signalName: String, args: EncodedValues) -> Unit
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
    handler: (EncodedValues) -> R
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
    handler: (queryName: String, args: EncodedValues) -> Any?
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicQueryHandler must be called from within workflow code")
    context.registerDynamicQueryHandler(handler)
  }
}

/**
 * Converts this [Promise] to a standard [Deferred].
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
 * This is a convenience extension that converts the Promise to a Deferred
 * and awaits it.
 *
 * @return the promise result
 * @throws Exception if the promise completed exceptionally
 */
public suspend fun <R> Promise<R>.await(): R = toDeferred().await()
