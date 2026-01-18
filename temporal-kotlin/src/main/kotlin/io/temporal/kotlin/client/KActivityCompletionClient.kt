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

package io.temporal.kotlin.client

import io.temporal.client.ActivityCompletionClient

/**
 * Client for completing activities asynchronously from outside the activity execution.
 *
 * Use this when an activity calls `KActivityContext.current().doNotCompleteOnReturn()`
 * to signal that the activity will be completed externally.
 *
 * Obtain an instance via [KClient.newActivityCompletionClient].
 *
 * Example:
 * ```kotlin
 * val completionClient = client.newActivityCompletionClient()
 *
 * // Complete by task token (most common)
 * completionClient.complete(taskToken, "result")
 *
 * // Or get a handle for repeated operations
 * val handle = completionClient.forTaskToken(taskToken)
 * handle.heartbeat("progress update")
 * handle.complete("final result")
 * ```
 *
 * @see KActivityCompletionHandle
 */
public class KActivityCompletionClient internal constructor(
  internal val javaClient: ActivityCompletionClient
) {

  /**
   * Creates a handle for completing an activity by task token.
   *
   * This is the most common way to complete async activities. The task token
   * is obtained from [io.temporal.kotlin.activity.KActivityContext.taskToken].
   *
   * @param taskToken The task token from the activity execution context
   * @return A handle for completing the activity
   */
  public fun forTaskToken(taskToken: ByteArray): KActivityCompletionHandle =
    KActivityCompletionHandle.ByTaskToken(javaClient, taskToken)

  /**
   * Creates a handle for completing an activity by workflow and activity identifiers.
   *
   * Use this when the task token is not available but you know the workflow
   * and activity IDs.
   *
   * @param workflowId The ID of the workflow that started the activity
   * @param activityId The ID of the activity
   * @param runId Optional run ID of the workflow (for disambiguation)
   * @return A handle for completing the activity
   */
  public fun forActivity(
    workflowId: String,
    activityId: String,
    runId: String? = null
  ): KActivityCompletionHandle =
    KActivityCompletionHandle.ByActivityId(javaClient, workflowId, runId, activityId)

  // ==================== Convenience Methods ====================
  // For one-off completions without creating a handle

  /**
   * Completes the activity successfully by task token.
   *
   * @param taskToken The task token from the activity execution context
   * @param result The result to return from the activity
   */
  public suspend fun <R> complete(taskToken: ByteArray, result: R) {
    forTaskToken(taskToken).complete(result)
  }

  /**
   * Completes the activity with failure by task token.
   *
   * @param taskToken The task token from the activity execution context
   * @param exception The exception to use as the failure details
   */
  public suspend fun completeExceptionally(taskToken: ByteArray, exception: Exception) {
    forTaskToken(taskToken).completeExceptionally(exception)
  }

  /**
   * Records a heartbeat by task token.
   *
   * Use this to keep the activity alive and report progress for long-running
   * external operations.
   *
   * @param taskToken The task token from the activity execution context
   * @param details Optional progress details to record with the heartbeat
   */
  public suspend fun <V> heartbeat(taskToken: ByteArray, details: V) {
    forTaskToken(taskToken).heartbeat(details)
  }

  /**
   * Reports cancellation by task token.
   *
   * Use this to confirm successful cancellation to the server.
   *
   * @param taskToken The task token from the activity execution context
   * @param details Optional details to record with the cancellation
   */
  public suspend fun <V> reportCancellation(taskToken: ByteArray, details: V) {
    forTaskToken(taskToken).reportCancellation(details)
  }
}
