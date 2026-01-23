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
import org.slf4j.Logger

/**
 * Context object passed to an Activity implementation.
 *
 * Use [KActivityContext.current] from an activity implementation to access.
 * This matches Java SDK's [io.temporal.activity.ActivityExecutionContext] pattern.
 *
 * Example (regular activity):
 * ```kotlin
 * class MyActivityImpl : MyActivity {
 *   override fun process(input: String): String {
 *     val context = KActivityContext.current
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
 *     val context = KActivityContext.current
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
public interface KActivityContext {

  public companion object {
    /**
     * Returns the current activity context.
     *
     * This is the primary entry point for accessing activity APIs from within
     * activity code. Works in both regular and suspend activities.
     *
     * @throws IllegalStateException if called outside of activity code
     */
    @JvmStatic
    public val current: KActivityContext
      get() {
        // First try the suspend activity context (for suspend activities running on coroutine threads)
        val suspendContext = CurrentSuspendActivityContext.get()
        if (suspendContext != null) {
          return SuspendActivityContextWrapper(suspendContext)
        }

        // Fall back to Java SDK's thread-local context (for regular activities)
        return KActivityContextImpl(Activity.getExecutionContext())
      }
  }

  /**
   * Information about the Activity Execution and the Workflow Execution that invoked it.
   */
  public val info: KActivityInfo

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
   * @throws io.temporal.client.ActivityCompletionException if the activity has been cancelled
   */
  public fun heartbeat(details: Any? = null)

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
  public fun <T> heartbeatDetails(detailsClass: Class<T>): T?

  /**
   * Gets a correlation token that can be used to complete the Activity Execution
   * asynchronously through [io.temporal.client.ActivityCompletionClient].
   */
  public val taskToken: ByteArray

  /**
   * If this method is called during an Activity Execution then the Activity Execution
   * is not going to complete when its method returns. It is expected to be completed
   * asynchronously using [io.temporal.client.ActivityCompletionClient].
   */
  public fun doNotCompleteOnReturn()

  /**
   * Returns true if [doNotCompleteOnReturn] was called.
   */
  public val isDoNotCompleteOnReturn: Boolean

  /**
   * Returns a logger for this activity.
   *
   * Uses the activity type as the logger name.
   *
   * @return SLF4J logger for activity logging
   */
  public fun logger(): Logger

  /**
   * Returns a logger with the specified name.
   *
   * @param name the logger name
   * @return SLF4J logger for activity logging
   */
  public fun logger(name: String): Logger

  /**
   * Returns a logger for the specified class.
   *
   * @param clazz the class to use as the logger name
   * @return SLF4J logger for activity logging
   */
  public fun logger(clazz: Class<*>): Logger
}

/**
 * Reified version of [KActivityContext.heartbeatDetails] for easier Kotlin usage.
 *
 * Example:
 * ```kotlin
 * val progress = context.heartbeatDetails<Int>()
 * val startIndex = progress ?: 0
 * ```
 *
 * @param T the expected type of the heartbeat details
 * @return the heartbeat details, or null if none
 */
public inline fun <reified T> KActivityContext.heartbeatDetails(): T? {
  return heartbeatDetails(T::class.java)
}
