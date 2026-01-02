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
import kotlinx.coroutines.CancellationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.coroutines.coroutineContext

/**
 * Provides access to Temporal activity APIs from within Kotlin activity code.
 *
 * This object provides Kotlin-friendly wrappers for Temporal activity operations
 * including heartbeating, getting activity info, and accessing the execution context.
 *
 * Supports both regular and suspend activities:
 * - **Regular activities**: Use thread-local context from Java SDK
 * - **Suspend activities**: Use coroutine context element
 *
 * Example (regular activity):
 * ```kotlin
 * class MyActivityImpl : MyActivity {
 *   override fun process(input: String): String {
 *     val info = KActivity.getInfo()
 *     println("Processing in activity ${info.activityId}, attempt ${info.attempt}")
 *
 *     // Heartbeat during long operations
 *     for (i in 1..100) {
 *       doWork(i)
 *       KActivity.heartbeat(i)
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
 *     val info = KActivity.getInfo()
 *     println("Fetching in activity ${info.activityId}")
 *
 *     // Non-blocking heartbeat in suspend activity
 *     for (i in 1..10) {
 *       val chunk = httpClient.get(url).body()
 *       KActivity.suspendHeartbeat(i)  // Non-blocking
 *     }
 *
 *     return data
 *   }
 * }
 * ```
 */
public object KActivity {

  /**
   * Returns information about the current activity execution.
   *
   * Works in both regular and suspend activities.
   *
   * @return the activity information with Kotlin-friendly types
   * @throws IllegalStateException if called outside of activity code
   */
  public fun getInfo(): KActivityInfo {
    // Try thread-local context (regular activities)
    val context = Activity.getExecutionContext()
    return KActivityInfoImpl(context.info)
  }

  /**
   * Returns a logger for the current activity.
   *
   * Uses the activity type as the logger name.
   *
   * @return SLF4J logger for activity logging
   */
  public fun logger(): Logger {
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
   * Records a heartbeat for the current activity (blocking version).
   *
   * Use this in regular (non-suspend) activities. For suspend activities,
   * use [suspendHeartbeat] instead for non-blocking operation.
   *
   * Heartbeats are used to:
   * 1. Report progress to the Temporal service
   * 2. Detect if the activity should be cancelled
   * 3. Store details that can be retrieved if the activity is retried
   *
   * @param details progress details to record (optional)
   * @throws ActivityCompletionException if the activity has been cancelled
   */
  public fun heartbeat(details: Any?) {
    Activity.getExecutionContext().heartbeat(details)
  }

  /**
   * Records a heartbeat for the current suspend activity (non-blocking version).
   *
   * This suspend function performs the heartbeat on a background dispatcher
   * to avoid blocking the coroutine. Use this in suspend activities instead
   * of the regular [heartbeat] function.
   *
   * If the activity has been cancelled, this function throws [CancellationException]
   * which will cancel the coroutine and properly report cancellation to Temporal.
   *
   * Example:
   * ```kotlin
   * override suspend fun processItems(items: List<Item>): Result {
   *     for ((index, item) in items.withIndex()) {
   *         process(item)
   *         KActivity.suspendHeartbeat(Progress(index, items.size))
   *     }
   *     return Result.success()
   * }
   * ```
   *
   * @param details progress details to record (optional)
   * @throws CancellationException if the activity has been cancelled
   * @throws IllegalStateException if called outside of a suspend activity
   */
  public suspend fun suspendHeartbeat(details: Any? = null) {
    val suspendContext = coroutineContext[SuspendActivityContextElement]?.context
      ?: throw IllegalStateException(
        "suspendHeartbeat() must be called from within a suspend activity. " +
          "For regular activities, use heartbeat() instead."
      )
    suspendContext.heartbeat(details)
  }

  /**
   * Gets the heartbeat details from the previous activity attempt.
   *
   * This is useful for resuming work after a retry. Returns null if
   * there are no details from a previous attempt.
   *
   * @param T the expected type of the heartbeat details
   * @param detailsClass the class of the expected details type
   * @return the heartbeat details, or null if none
   */
  public fun <T> getHeartbeatDetails(detailsClass: Class<T>): T? {
    return Activity.getExecutionContext()
      .getHeartbeatDetails(detailsClass)
      .orElse(null)
  }

  /**
   * Reified version of [getHeartbeatDetails] for easier Kotlin usage.
   *
   * Example:
   * ```kotlin
   * val progress = KActivity.getHeartbeatDetails<Int>()
   * val startIndex = progress ?: 0
   * ```
   *
   * @param T the expected type of the heartbeat details
   * @return the heartbeat details, or null if none
   */
  public inline fun <reified T> getHeartbeatDetails(): T? {
    return getHeartbeatDetails(T::class.java)
  }

  /**
   * Returns the raw execution context for advanced use cases.
   *
   * Prefer using the other methods on this object when possible.
   *
   * @return the underlying activity execution context
   */
  public fun getExecutionContext(): ActivityExecutionContext {
    return Activity.getExecutionContext()
  }

  /**
   * Returns the task token for async activity completion.
   *
   * Use this when you need to complete the activity asynchronously
   * from a different process.
   *
   * @return the task token bytes
   */
  public fun getTaskToken(): ByteArray {
    return Activity.getExecutionContext().taskToken
  }

  /**
   * Marks this activity to be completed asynchronously.
   *
   * After calling this method, the activity method should return immediately.
   * The activity will remain open until completed via [ActivityCompletionClient].
   *
   * Example:
   * ```kotlin
   * override fun processAsync(input: String) {
   *   val taskToken = KActivity.getTaskToken()
   *   // Store taskToken for later completion
   *   externalService.startProcessing(input, taskToken)
   *   KActivity.doNotCompleteOnReturn()
   *   // Activity remains open after return
   * }
   * ```
   */
  public fun doNotCompleteOnReturn() {
    Activity.getExecutionContext().doNotCompleteOnReturn()
  }

  /**
   * Checks if a cancellation has been requested for this activity.
   *
   * Activities should periodically check this and clean up if true.
   *
   * @return true if cancellation was requested
   */
  public fun isCancellationRequested(): Boolean {
    return try {
      Activity.getExecutionContext().info.let { false }
    } catch (e: Exception) {
      false
    }
  }
}
