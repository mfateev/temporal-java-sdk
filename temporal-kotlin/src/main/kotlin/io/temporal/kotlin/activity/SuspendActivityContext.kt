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

import io.temporal.activity.ActivityExecutionContext
import io.temporal.activity.ActivityInfo
import io.temporal.activity.ManualActivityCompletionClient
import io.temporal.failure.CanceledFailure
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

// TODO: Switch from Dispatchers.IO + blocking Java SDK calls to fully async implementation
//  using gRPC async client. This will eliminate thread pool overhead and provide true
//  non-blocking suspension.

/**
 * Coroutine context element that holds the suspend activity context.
 *
 * This element is added to the coroutine context when executing a suspend activity,
 * allowing access to activity APIs from within the coroutine.
 */
internal class SuspendActivityContextElement(
  val context: SuspendActivityContext
) : AbstractCoroutineContextElement(Key) {
  companion object Key : CoroutineContext.Key<SuspendActivityContextElement>
}

/**
 * Activity context for suspend activities.
 *
 * Provides access to activity APIs from within a coroutine, including:
 * - Activity info
 * - Heartbeating (non-blocking via coroutines)
 * - Task token for external completion
 * - Heartbeat details from previous attempts
 *
 * Cancellation handling: When a heartbeat detects that the activity has been cancelled
 * (server returns `CanceledFailure`), this context cancels the parent coroutine job,
 * propagating cancellation through the coroutine hierarchy.
 */
internal class SuspendActivityContext(
  private val javaContext: ActivityExecutionContext,
  private val completionClient: ManualActivityCompletionClient,
  private val parentJob: Job
) {
  /**
   * Activity information for the current execution.
   */
  val info: ActivityInfo
    get() = javaContext.info

  /**
   * Task token for external activity completion scenarios.
   */
  val taskToken: ByteArray
    get() = javaContext.taskToken

  /**
   * Sends a heartbeat with optional details.
   *
   * This is a suspend function that performs the heartbeat on [Dispatchers.IO]
   * to avoid blocking the coroutine dispatcher.
   *
   * @param details Optional progress details to record with the heartbeat
   * @throws CancellationException if the activity has been cancelled
   */
  suspend fun heartbeat(details: Any? = null) {
    try {
      withContext(Dispatchers.IO) {
        completionClient.recordHeartbeat(details)
      }
    } catch (e: CanceledFailure) {
      // Activity was cancelled by server - cancel the coroutine
      val cancellation = CancellationException("Activity cancelled by server", e)
      parentJob.cancel(cancellation)
      throw cancellation
    }
  }

  /**
   * Gets heartbeat details from a previous activity attempt.
   *
   * @param T the expected type of the heartbeat details
   * @param detailsClass the class of the expected details type
   * @return the heartbeat details, or null if none
   */
  fun <T> heartbeatDetails(detailsClass: Class<T>): T? {
    return javaContext.getHeartbeatDetails(detailsClass).orElse(null)
  }

  /**
   * Gets heartbeat details from a previous activity attempt using reified type.
   *
   * @param T the expected type of the heartbeat details
   * @return the heartbeat details, or null if none
   */
  inline fun <reified T> heartbeatDetails(): T? {
    return heartbeatDetails(T::class.java)
  }
}
