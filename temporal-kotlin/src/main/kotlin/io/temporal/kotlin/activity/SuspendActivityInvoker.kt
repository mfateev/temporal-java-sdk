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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend

/**
 * Invokes suspend activity methods using Kotlin coroutines.
 *
 * This invoker uses the `useLocalManualCompletion()` pattern from the Java SDK
 * to achieve truly non-blocking execution:
 *
 * 1. When invoked, it obtains a `ManualActivityCompletionClient`
 * 2. Launches a coroutine on the configured dispatcher
 * 3. Returns immediately, freeing the activity executor thread
 * 4. The coroutine executes the suspend function
 * 5. On completion, calls `completionClient.complete()` or `fail()`
 *
 * Thread model:
 * - The `invoke()` method is called on a Java SDK activity executor thread
 * - The coroutine runs on threads from the `dispatcher` (separate thread pool)
 * - Coroutines can suspend and resume on different threads within the dispatcher
 * - The completion client is thread-safe and can be called from any thread
 *
 * @param activityInstance The activity implementation instance
 * @param method The suspend function to invoke
 * @param dispatcher The coroutine dispatcher for executing the suspend function
 */
internal class SuspendActivityInvoker(
  private val activityInstance: Any,
  private val method: KFunction<*>,
  private val dispatcher: CoroutineDispatcher = Dispatchers.Default
) {

  /**
   * Invokes the suspend activity method.
   *
   * This method returns immediately after launching the coroutine.
   * The actual activity result is sent via the `ManualActivityCompletionClient`.
   *
   * @param context The activity execution context from the Java SDK
   * @param args The deserialized arguments for the activity method
   * @return Always returns null; the actual result is sent via completion client
   */
  fun invoke(
    context: ActivityExecutionContext,
    args: Array<Any?>
  ): Any? {
    // Get manual completion client - this marks the activity for async completion
    // and ensures the slot permit is released when we call complete/fail/reportCancellation
    val completionClient = context.useLocalManualCompletion()

    // Create a supervisor job so failures don't propagate to parent scopes
    val job = SupervisorJob()
    val scope = CoroutineScope(dispatcher + job)

    // Launch coroutine - this returns immediately, freeing the executor thread
    scope.launch {
      // Create activity context for coroutine access
      val suspendContext = SuspendActivityContext(context, completionClient, job)

      withContext(SuspendActivityContextElement(suspendContext)) {
        try {
          // Execute the suspend function
          val result = method.callSuspend(activityInstance, *args)

          // Complete successfully - uses Dispatchers.IO for the blocking gRPC call
          withContext(Dispatchers.IO) {
            completionClient.complete(result)
          }
        } catch (e: CancellationException) {
          // Coroutine was cancelled (activity cancellation detected via heartbeat)
          withContext(Dispatchers.IO + NonCancellable) {
            completionClient.reportCancellation(null)
          }
        } catch (e: Throwable) {
          // Activity failed with an exception
          withContext(Dispatchers.IO + NonCancellable) {
            completionClient.fail(e)
          }
        }
      }
    }

    // Return immediately - actual result sent via completion client
    return null
  }
}
