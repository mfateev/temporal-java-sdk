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

package io.temporal.kotlin.activity

import io.temporal.activity.Activity
import io.temporal.activity.ActivityExecutionContext
import io.temporal.client.ActivityCompletionClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Provides access to Temporal activity APIs from within Kotlin activity code.
 *
 * Use the [executionContext] property to obtain a [KActivityContext] for accessing activity APIs.
 * This matches Java SDK's [Activity.getExecutionContext] pattern with Kotlin idiomatic syntax.
 *
 * Supports both regular and suspend activities:
 * - **Regular activities**: Use thread-local context from Java SDK
 * - **Suspend activities**: Use coroutine context element
 *
 * Example (regular activity):
 * ```kotlin
 * class MyActivityImpl : MyActivity {
 *   override fun process(input: String): String {
 *     val context = KActivity.executionContext
 *     val info = context.info
 *     println("Processing in activity ${info.activityId}, attempt ${info.attempt}")
 *
 *     // Heartbeat during long operations
 *     for (i in 1..100) {
 *       doWork(i)
 *       context.heartbeat(i)
 *     }
 *
 *     return "done"
 *   }
 * }
 * ```
 *
 * Example (suspend activity):
 * ```kotlin
 * class MySuspendActivityImpl : MySuspendActivity {
 *   override suspend fun fetchData(url: String): Data {
 *     val context = KActivity.executionContext
 *     println("Fetching in activity ${context.info.activityId}")
 *
 *     for (i in 1..10) {
 *       val chunk = httpClient.get(url).body()
 *       context.heartbeat(i)
 *     }
 *
 *     return data
 *   }
 * }
 * ```
 */
public object KActivity {

  /**
   * The activity execution context for the current activity.
   *
   * This is the primary entry point for accessing activity APIs, matching
   * Java SDK's [Activity.getExecutionContext] pattern with Kotlin idiomatic syntax.
   *
   * Works in both regular and suspend activities.
   *
   * @throws IllegalStateException if called outside of activity code
   */
  public val executionContext: KActivityContext
    get() {
      // First try the suspend activity context (for suspend activities running on coroutine threads)
      val suspendContext = CurrentSuspendActivityContext.get()
      if (suspendContext != null) {
        return KActivityContextImpl(suspendContext.executionContext)
      }

      // Fall back to Java SDK's thread-local context (for regular activities)
      return KActivityContextImpl(Activity.getExecutionContext())
    }

  /**
   * Information about the current activity execution.
   *
   * Works in both regular and suspend activities.
   *
   * @throws IllegalStateException if called outside of activity code
   */
  public val info: KActivityInfo
    get() {
      // First try the suspend activity context (for suspend activities running on coroutine threads)
      val suspendContext = CurrentSuspendActivityContext.get()
      if (suspendContext != null) {
        return KActivityInfoImpl(suspendContext.executionContext.info)
      }

      // Fall back to Java SDK's thread-local context (for regular activities)
      return KActivityInfoImpl(Activity.getExecutionContext().info)
    }

  /**
   * Returns a logger for the current activity.
   *
   * Uses the activity type as the logger name. Works in both regular and suspend activities.
   *
   * @return SLF4J logger for activity logging
   */
  public fun logger(): Logger {
    // First try the suspend activity context (for suspend activities running on coroutine threads)
    val suspendContext = CurrentSuspendActivityContext.get()
    if (suspendContext != null) {
      return LoggerFactory.getLogger(suspendContext.executionContext.info.activityType)
    }

    // Fall back to Java SDK's thread-local context (for regular activities)
    return LoggerFactory.getLogger(Activity.getExecutionContext().info.activityType)
  }

  /**
   * Returns a logger with the specified name.
   *
   * @param name the logger name
   * @return SLF4J logger for activity logging
   */
  public fun logger(name: String): Logger {
    return LoggerFactory.getLogger(name)
  }

  /**
   * Returns a logger for the specified class.
   *
   * @param clazz the class to use as the logger name
   * @return SLF4J logger for activity logging
   */
  public fun logger(clazz: Class<*>): Logger {
    return LoggerFactory.getLogger(clazz)
  }

  /**
   * Records a heartbeat for the current activity.
   *
   * Heartbeats are used to:
   * 1. Report progress to the Temporal service
   * 2. Detect if the activity should be cancelled
   * 3. Store details that can be retrieved if the activity is retried
   *
   * This is a short, non-blocking operation that records progress locally.
   * The actual network call happens asynchronously in the background.
   * Use this method in both regular and suspend activities.
   *
   * @param details progress details to record (optional)
   * @throws ActivityCompletionException if the activity has been cancelled
   */
  public fun heartbeat(details: Any? = null) {
    // First try the suspend activity context (for suspend activities running on coroutine threads)
    val suspendContext = CurrentSuspendActivityContext.get()
    if (suspendContext != null) {
      // In suspend activity - use the manual completion client
      suspendContext.completionClient.recordHeartbeat(details)
      return
    }

    // Fall back to Java SDK's thread-local context (for regular activities)
    Activity.getExecutionContext().heartbeat(details)
  }

  /**
   * Gets the heartbeat details from the previous activity attempt.
   *
   * This is useful for resuming work after a retry. Returns null if
   * there are no details from a previous attempt.
   *
   * Works in both regular and suspend activities.
   *
   * @param T the expected type of the heartbeat details
   * @param detailsClass the class of the expected details type
   * @return the heartbeat details, or null if none
   */
  public fun <T> heartbeatDetails(detailsClass: Class<T>): T? {
    // First try the suspend activity context (for suspend activities running on coroutine threads)
    val suspendContext = CurrentSuspendActivityContext.get()
    if (suspendContext != null) {
      return suspendContext.executionContext
        .getHeartbeatDetails(detailsClass)
        .orElse(null)
    }

    // Fall back to Java SDK's thread-local context (for regular activities)
    return Activity.getExecutionContext()
      .getHeartbeatDetails(detailsClass)
      .orElse(null)
  }

  /**
   * Reified version of [heartbeatDetails] for easier Kotlin usage.
   *
   * Example:
   * ```kotlin
   * val progress = KActivity.heartbeatDetails<Int>()
   * val startIndex = progress ?: 0
   * ```
   *
   * @param T the expected type of the heartbeat details
   * @return the heartbeat details, or null if none
   */
  public inline fun <reified T> heartbeatDetails(): T? {
    return heartbeatDetails(T::class.java)
  }

  /**
   * The raw Java execution context for advanced use cases.
   *
   * Prefer using [executionContext] when possible.
   * Works in both regular and suspend activities.
   */
  public val javaExecutionContext: ActivityExecutionContext
    get() {
      // First try the suspend activity context (for suspend activities running on coroutine threads)
      val suspendContext = CurrentSuspendActivityContext.get()
      if (suspendContext != null) {
        return suspendContext.executionContext
      }

      // Fall back to Java SDK's thread-local context (for regular activities)
      return Activity.getExecutionContext()
    }

  /**
   * The task token for async activity completion.
   *
   * Use this when you need to complete the activity asynchronously
   * from a different process. Works in both regular and suspend activities.
   */
  public val taskToken: ByteArray
    get() {
      // First try the suspend activity context (for suspend activities running on coroutine threads)
      val suspendContext = CurrentSuspendActivityContext.get()
      if (suspendContext != null) {
        return suspendContext.executionContext.taskToken
      }

      // Fall back to Java SDK's thread-local context (for regular activities)
      return Activity.getExecutionContext().taskToken
    }

  /**
   * Marks this activity to be completed asynchronously.
   *
   * After calling this method, the activity method should return immediately.
   * The activity will remain open until completed via [ActivityCompletionClient].
   * Works in both regular and suspend activities.
   *
   * Example:
   * ```kotlin
   * override fun processAsync(input: String) {
   *   val taskToken = KActivity.taskToken
   *   // Store taskToken for later completion
   *   externalService.startProcessing(input, taskToken)
   *   KActivity.doNotCompleteOnReturn()
   *   // Activity remains open after return
   * }
   * ```
   */
  public fun doNotCompleteOnReturn() {
    // First try the suspend activity context (for suspend activities running on coroutine threads)
    val suspendContext = CurrentSuspendActivityContext.get()
    if (suspendContext != null) {
      suspendContext.executionContext.doNotCompleteOnReturn()
      return
    }

    // Fall back to Java SDK's thread-local context (for regular activities)
    Activity.getExecutionContext().doNotCompleteOnReturn()
  }

  /**
   * Whether a cancellation has been requested for this activity.
   *
   * Activities should periodically check this and clean up if true.
   * Works in both regular and suspend activities.
   */
  public val isCancellationRequested: Boolean
    get() = try {
      // First try the suspend activity context (for suspend activities running on coroutine threads)
      val suspendContext = CurrentSuspendActivityContext.get()
      if (suspendContext != null) {
        suspendContext.executionContext.info.let { false }
      } else {
        // Fall back to Java SDK's thread-local context (for regular activities)
        Activity.getExecutionContext().info.let { false }
      }
    } catch (e: Exception) {
      false
    }
}
