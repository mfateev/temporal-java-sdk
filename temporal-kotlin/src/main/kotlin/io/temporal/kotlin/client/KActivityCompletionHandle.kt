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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.Optional

/**
 * Handle for completing a specific activity asynchronously.
 *
 * Obtain via [KActivityCompletionClient.forTaskToken] or [KActivityCompletionClient.forActivity].
 *
 * Example:
 * ```kotlin
 * val handle = completionClient.forTaskToken(taskToken)
 * handle.heartbeat("50% complete")
 * handle.complete("result")
 * ```
 */
public sealed class KActivityCompletionHandle {

  /**
   * Completes the activity successfully with the given result.
   *
   * @param result The result to return from the activity
   */
  public abstract suspend fun <R> complete(result: R)

  /**
   * Completes the activity with a failure.
   *
   * @param exception The exception to use as the failure details
   */
  public abstract suspend fun completeExceptionally(exception: Exception)

  /**
   * Records a heartbeat for the activity.
   *
   * Use this to keep the activity alive and report progress for long-running
   * external operations.
   *
   * @param details Optional progress details to record with the heartbeat
   */
  public abstract suspend fun <V> heartbeat(details: V)

  /**
   * Reports that the activity was cancelled.
   *
   * Use this to confirm successful cancellation to the server.
   *
   * @param details Optional details to record with the cancellation
   */
  public abstract suspend fun <V> reportCancellation(details: V)

  /**
   * Handle for completing an activity identified by task token.
   */
  internal class ByTaskToken(
    private val client: ActivityCompletionClient,
    private val taskToken: ByteArray
  ) : KActivityCompletionHandle() {

    override suspend fun <R> complete(result: R): Unit = withContext(Dispatchers.IO) {
      client.complete(taskToken, result)
    }

    override suspend fun completeExceptionally(exception: Exception): Unit = withContext(Dispatchers.IO) {
      client.completeExceptionally(taskToken, exception)
    }

    override suspend fun <V> heartbeat(details: V): Unit = withContext(Dispatchers.IO) {
      client.heartbeat(taskToken, details)
    }

    override suspend fun <V> reportCancellation(details: V): Unit = withContext(Dispatchers.IO) {
      client.reportCancellation(taskToken, details)
    }
  }

  /**
   * Handle for completing an activity identified by workflow ID and activity ID.
   */
  internal class ByActivityId(
    private val client: ActivityCompletionClient,
    private val workflowId: String,
    private val runId: String?,
    private val activityId: String
  ) : KActivityCompletionHandle() {

    override suspend fun <R> complete(result: R): Unit = withContext(Dispatchers.IO) {
      client.complete(workflowId, Optional.ofNullable(runId), activityId, result)
    }

    override suspend fun completeExceptionally(exception: Exception): Unit = withContext(Dispatchers.IO) {
      client.completeExceptionally(workflowId, Optional.ofNullable(runId), activityId, exception)
    }

    override suspend fun <V> heartbeat(details: V): Unit = withContext(Dispatchers.IO) {
      client.heartbeat(workflowId, Optional.ofNullable(runId), activityId, details)
    }

    override suspend fun <V> reportCancellation(details: V): Unit = withContext(Dispatchers.IO) {
      client.reportCancellation(workflowId, Optional.ofNullable(runId), activityId, details)
    }
  }
}
