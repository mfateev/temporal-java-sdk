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

import io.temporal.activity.ActivityMethod
import io.temporal.common.converter.EncodedValues
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.kotlin.toJava
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInfo
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
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
  @PublishedApi
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
   *   options = KActivityOptions(startToCloseTimeout = 5.minutes),
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
    options: KActivityOptions,
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
    options: KActivityOptions,
    vararg args: Any?
  ): R {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.executeActivity must be called from within workflow code")
    return context.executeActivityByName(activityName, options.toJavaOptions(), resultClass, *args)
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
    return executeActivity(activityName, resultClass as Class<R>, options)
  }

  /**
   * Executes an activity using a method reference with 1 argument.
   *
   * @param T the activity interface type
   * @param A1 the type of the first argument
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param options the activity options
   * @param arg1 the first argument
   * @return the activity result
   */
  public suspend fun <T, A1, R> executeActivity(
    activity: KFunction2<T, A1, R>,
    options: KActivityOptions,
    arg1: A1
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1)
  }

  /**
   * Executes an activity using a method reference with 2 arguments.
   *
   * @param T the activity interface type
   * @param A1 the type of the first argument
   * @param A2 the type of the second argument
   * @param R the return type of the activity
   * @param activity the activity method reference
   * @param options the activity options
   * @param arg1 the first argument
   * @param arg2 the second argument
   * @return the activity result
   */
  public suspend fun <T, A1, A2, R> executeActivity(
    activity: KFunction3<T, A1, A2, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1, arg2)
  }

  /**
   * Executes an activity using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeActivity(
    activity: KFunction4<T, A1, A2, A3, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3)
  }

  /**
   * Executes an activity using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeActivity(
    activity: KFunction5<T, A1, A2, A3, A4, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4)
  }

  /**
   * Executes an activity using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeActivity(
    activity: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4, arg5)
  }

  /**
   * Executes an activity using a method reference with 6 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeActivity(
    activity: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4, arg5, arg6)
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
    return context.executeLocalActivityByName(activityName, options.toJavaOptions(), resultClass, *args)
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
    options: KLocalActivityOptions,
    arg1: A1
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
    options: KLocalActivityOptions,
    arg1: A1,
    arg2: A2
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1, arg2)
  }

  /**
   * Executes a local activity using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeLocalActivity(
    activity: KFunction4<T, A1, A2, A3, R>,
    options: KLocalActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3)
  }

  /**
   * Executes a local activity using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeLocalActivity(
    activity: KFunction5<T, A1, A2, A3, A4, R>,
    options: KLocalActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4)
  }

  /**
   * Executes a local activity using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeLocalActivity(
    activity: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KLocalActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4, arg5)
  }

  /**
   * Executes a local activity using a method reference with 6 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeLocalActivity(
    activity: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KLocalActivityOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): R {
    val (activityName, resultClass) = extractActivityMetadata(activity)
    @Suppress("UNCHECKED_CAST")
    return executeLocalActivity(activityName, resultClass as Class<R>, options, arg1, arg2, arg3, arg4, arg5, arg6)
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
    return context.executeChildWorkflowByName(workflowType, options.toJavaOptions(), resultClass, *args)
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
    options: KChildWorkflowOptions
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
    options: KChildWorkflowOptions,
    arg1: A1
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
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1, arg2)
  }

  /**
   * Executes a child workflow using a method reference with 3 arguments.
   */
  public suspend fun <T, A1, A2, A3, R> executeChildWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1, arg2, arg3)
  }

  /**
   * Executes a child workflow using a method reference with 4 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeChildWorkflow(
    workflow: KFunction5<T, A1, A2, A3, A4, R>,
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1, arg2, arg3, arg4)
  }

  /**
   * Executes a child workflow using a method reference with 5 arguments.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeChildWorkflow(
    workflow: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return executeChildWorkflow(workflowType, resultClass as Class<R>, options, arg1, arg2, arg3, arg4, arg5)
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
   *   KChildWorkflowOptions(workflowId = "child-123"),
   *   order
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
    options: KChildWorkflowOptions
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, options.toJavaOptions(), resultClass as Class<R>)
  }

  /**
   * Starts a child workflow with 1 argument and returns a handle.
   */
  public suspend fun <T, A1, R> startChildWorkflow(
    workflow: KFunction2<T, A1, R>,
    options: KChildWorkflowOptions,
    arg1: A1
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, options.toJavaOptions(), resultClass as Class<R>, arg1)
  }

  /**
   * Starts a child workflow with 2 arguments and returns a handle.
   */
  public suspend fun <T, A1, A2, R> startChildWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, options.toJavaOptions(), resultClass as Class<R>, arg1, arg2)
  }

  /**
   * Starts a child workflow with 3 arguments and returns a handle.
   */
  public suspend fun <T, A1, A2, A3, R> startChildWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    options: KChildWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): KChildWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.startChildWorkflow must be called from within workflow code")
    @Suppress("UNCHECKED_CAST")
    return context.startChildWorkflowWithHandle(workflowType, options.toJavaOptions(), resultClass as Class<R>, arg1, arg2, arg3)
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
   *       val newConfig = args.get(0, Config::class.java)
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
    handler: suspend (updateName: String, args: EncodedValues) -> Any?
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
    validator: (updateName: String, args: EncodedValues) -> Unit
  ) {
    val context = currentContext.get()
      ?: throw IllegalStateException("KWorkflow.registerDynamicUpdateValidator must be called from within workflow code")
    context.registerDynamicUpdateValidator(validator)
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
    Workflow.continueAsNew(*args)
    // The above call always throws, but Kotlin needs this for Nothing return type
    throw IllegalStateException("continueAsNew should have thrown")
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
    Workflow.continueAsNew(options.toJavaOptions(), *args)
    // The above call always throws, but Kotlin needs this for Nothing return type
    throw IllegalStateException("continueAsNew should have thrown")
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
    Workflow.continueAsNew(workflowType, options.toJavaOptions(), *args)
    // The above call always throws, but Kotlin needs this for Nothing return type
    throw IllegalStateException("continueAsNew should have thrown")
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
    Workflow.continueAsNew(workflowType, options.toJavaOptions(), *args)
    // The above call always throws, but Kotlin needs this for Nothing return type
    throw IllegalStateException("continueAsNew should have thrown")
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
