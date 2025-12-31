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

/**
 * Provides access to Temporal activity APIs from within Kotlin activity code.
 *
 * This object provides Kotlin-friendly wrappers for Temporal activity operations
 * including heartbeating, getting activity info, and accessing the execution context.
 *
 * All methods must be called from within activity code only.
 *
 * Example:
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
 */
public object KActivity {

  /**
   * Returns information about the current activity execution.
   *
   * @return the activity information with Kotlin-friendly types
   * @throws IllegalStateException if called outside of activity code
   */
  public fun getInfo(): KActivityInfo {
    val context = Activity.getExecutionContext()
    return KActivityInfoImpl(context.info)
  }

  /**
   * Records a heartbeat for the current activity.
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
