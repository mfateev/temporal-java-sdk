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

package io.temporal.kotlin.internal

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Handles delay operations for Kotlin coroutines in Temporal workflows.
 *
 * This object maps Kotlin coroutine `delay()` calls to Temporal timer operations,
 * ensuring that delays are deterministic and replay-safe.
 */
@InternalTemporalApi
@OptIn(InternalCoroutinesApi::class)
internal object KotlinDelay {

  /**
   * Schedules a coroutine to resume after a delay using Temporal timers.
   *
   * @param timeMillis the delay in milliseconds
   * @param continuation the continuation to resume after the delay
   * @param workflowContext the workflow context for creating timers
   * @param dispatcher the dispatcher to use for resuming the coroutine
   */
  fun scheduleResumeAfterDelay(
    timeMillis: Long,
    continuation: CancellableContinuation<Unit>,
    workflowContext: KotlinWorkflowContext,
    dispatcher: KotlinCoroutineDispatcher
  ) {
    if (timeMillis <= 0) {
      // Zero or negative delay - resume immediately
      dispatcher.dispatch(continuation.context) {
        continuation.resume(Unit)
      }
      return
    }

    val duration = Duration.ofMillis(timeMillis)
    val cancelled = AtomicBoolean(false)

    // Create a Temporal timer
    val cancellationHandle = workflowContext.replayContext.newTimer(
      duration,
      null // No user metadata
    ) { exception ->
      if (!cancelled.get()) {
        dispatcher.dispatch(continuation.context) {
          if (exception != null) {
            continuation.resumeWithException(
              CancellationException(exception.message, exception)
            )
          } else {
            continuation.resume(Unit)
          }
        }
      }
    }

    // Handle cancellation from the coroutine side
    continuation.invokeOnCancellation { cause ->
      if (cancelled.compareAndSet(false, true)) {
        cancellationHandle.apply(
          cause as? RuntimeException
            ?: RuntimeException(cause?.message ?: "Delay cancelled")
        )
      }
    }
  }

  /**
   * Invokes a block after a delay using Temporal timers.
   *
   * @param timeMillis the delay in milliseconds
   * @param block the block to execute after the delay
   * @param workflowContext the workflow context for creating timers
   * @param dispatcher the dispatcher to use for executing the block
   * @return a handle to dispose (cancel) the delayed execution
   */
  fun invokeOnTimeout(
    timeMillis: Long,
    block: Runnable,
    workflowContext: KotlinWorkflowContext,
    dispatcher: KotlinCoroutineDispatcher
  ): DisposableHandle {
    if (timeMillis <= 0) {
      // Zero or negative delay - execute immediately
      dispatcher.dispatch(kotlinx.coroutines.Dispatchers.Unconfined) {
        block.run()
      }
      return object : DisposableHandle {
        override fun dispose() {
          // Already executed, nothing to dispose
        }
      }
    }

    val duration = Duration.ofMillis(timeMillis)
    val cancelled = AtomicBoolean(false)

    val cancellationHandle = workflowContext.replayContext.newTimer(
      duration,
      null
    ) { exception ->
      if (!cancelled.get() && exception == null) {
        dispatcher.dispatch(kotlinx.coroutines.Dispatchers.Unconfined) {
          block.run()
        }
      }
    }

    return object : DisposableHandle {
      override fun dispose() {
        if (cancelled.compareAndSet(false, true)) {
          cancellationHandle.apply(RuntimeException("Timeout disposed"))
        }
      }
    }
  }
}
