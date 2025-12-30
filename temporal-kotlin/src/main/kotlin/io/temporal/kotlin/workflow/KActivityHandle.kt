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

package io.temporal.kotlin.workflow

import kotlinx.coroutines.CompletableDeferred
import java.util.function.Consumer

/**
 * Handle for an asynchronously executing activity.
 *
 * This handle is returned when starting an activity without waiting for completion,
 * allowing parallel activity execution patterns.
 *
 * Example:
 * ```kotlin
 * val handle1 = KWorkflow.startActivity<String>("activity1", "arg1")
 * val handle2 = KWorkflow.startActivity<Int>("activity2", 42)
 *
 * // Activities run in parallel
 * val result1 = handle1.await()
 * val result2 = handle2.await()
 * ```
 *
 * @param R the result type of the activity
 */
public interface KActivityHandle<R> {

  /**
   * Returns `true` if the activity has completed.
   *
   * Completion may be due to normal termination, an exception, or cancellation.
   */
  public val isCompleted: Boolean

  /**
   * Waits for the activity to complete and returns its result.
   *
   * @return the activity result
   * @throws Exception if the activity failed
   */
  public suspend fun await(): R

  /**
   * Requests cancellation of this activity.
   *
   * Note: Cancellation is cooperative. The activity must handle the cancellation
   * request to actually stop. If the activity has already completed, this has no effect.
   *
   * @param reason optional reason for the cancellation
   */
  public fun cancel(reason: String? = null)
}

/**
 * Internal implementation of [KActivityHandle].
 *
 * @param R the result type
 * @param deferred the CompletableDeferred that will hold the result
 * @param cancellationCallback callback to invoke when cancel() is called
 */
internal class KActivityHandleImpl<R>(
  private val deferred: CompletableDeferred<R>,
  private val cancellationCallback: Consumer<Exception?>?
) : KActivityHandle<R> {

  override val isCompleted: Boolean
    get() = deferred.isCompleted

  override suspend fun await(): R = deferred.await()

  override fun cancel(reason: String?) {
    if (!isCompleted) {
      cancellationCallback?.accept(
        RuntimeException(reason ?: "Activity cancelled")
      )
    }
  }

  /**
   * Completes this handle with a successful result.
   * For internal use only.
   */
  internal fun complete(value: R) {
    deferred.complete(value)
  }

  /**
   * Completes this handle exceptionally.
   * For internal use only.
   */
  internal fun completeExceptionally(exception: Throwable) {
    deferred.completeExceptionally(exception)
  }
}
