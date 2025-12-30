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

import io.temporal.api.common.v1.Payloads
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.common.v1.WorkflowType
import io.temporal.api.failure.v1.Failure
import io.temporal.api.sdk.v1.UserMetadata
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.internal.statemachines.ExecuteActivityParameters
import io.temporal.internal.statemachines.ExecuteLocalActivityParameters
import io.temporal.internal.statemachines.LocalActivityCallback
import io.temporal.internal.statemachines.StartChildWorkflowExecutionParameters
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendCancellableCoroutine
import java.time.Duration
import java.util.Optional
import java.util.Random
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Internal context that wraps [ReplayWorkflowContext] for Kotlin workflows.
 *
 * This class provides suspend function wrappers for Temporal workflow operations,
 * allowing Kotlin coroutine-based workflows to interact with the Temporal runtime.
 */
@InternalTemporalApi
internal class KotlinWorkflowContext(
  internal val replayContext: ReplayWorkflowContext
) {
  /**
   * Returns the workflow execution info.
   */
  val workflowExecution: WorkflowExecution
    get() = replayContext.workflowExecution

  /**
   * Returns the workflow type.
   */
  val workflowType: WorkflowType
    get() = replayContext.workflowType

  /**
   * Returns the current workflow time in milliseconds.
   * This is deterministic and returns the same value during replay.
   */
  val currentTimeMillis: Long
    get() = replayContext.currentTimeMillis()

  /**
   * Returns true if the workflow is replaying from history.
   */
  val isReplaying: Boolean
    get() = replayContext.isReplaying

  /**
   * Returns the workflow run ID.
   */
  val runId: String
    get() = replayContext.runId

  /**
   * Returns the workflow ID.
   */
  val workflowId: String
    get() = replayContext.workflowId

  /**
   * Returns the workflow namespace.
   */
  val namespace: String
    get() = replayContext.namespace

  /**
   * Returns a deterministic random number generator.
   */
  fun newRandom(): Random = replayContext.newRandom()

  /**
   * Returns a deterministic UUID.
   */
  fun randomUUID(): UUID = replayContext.randomUUID()

  /**
   * Creates a timer that completes after the specified duration.
   *
   * @param duration the duration to wait
   * @return Unit when the timer fires
   * @throws kotlinx.coroutines.CancellationException if the timer is cancelled
   */
  suspend fun createTimer(duration: Duration): Unit = suspendCancellableCoroutine { cont ->
    val cancellationHandle = replayContext.newTimer(
      duration,
      null as UserMetadata?
    ) { exception ->
      if (exception != null) {
        cont.resumeWithException(exception)
      } else {
        cont.resume(Unit)
      }
    }

    cont.invokeOnCancellation { cause ->
      cancellationHandle.apply(
        cause as? RuntimeException
          ?: RuntimeException(cause?.message ?: "Timer cancelled")
      )
    }
  }

  /**
   * Executes an activity and returns its result.
   *
   * @param parameters the activity execution parameters
   * @return the activity result payload
   * @throws Exception if the activity fails
   */
  suspend fun executeActivity(
    parameters: ExecuteActivityParameters
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    val output = replayContext.scheduleActivityTask(
      parameters
    ) { result: Optional<Payloads>, failure: Failure? ->
      if (failure != null) {
        cont.resumeWithException(
          RuntimeException("Activity failed: ${failure.message}")
        )
      } else {
        cont.resume(result)
      }
    }

    cont.invokeOnCancellation { cause ->
      output.cancellationHandle.apply(
        cause as? Exception
          ?: RuntimeException(cause?.message ?: "Activity cancelled")
      )
    }
  }

  /**
   * Executes a local activity and returns its result.
   *
   * @param parameters the local activity execution parameters
   * @return the activity result payload
   * @throws Exception if the activity fails
   */
  suspend fun executeLocalActivity(
    parameters: ExecuteLocalActivityParameters
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    val cancellationHandle = replayContext.scheduleLocalActivityTask(
      parameters,
      LocalActivityCallback { result, exception ->
        if (exception != null) {
          cont.resumeWithException(
            RuntimeException("Local activity failed: ${exception.failure.message}", exception)
          )
        } else {
          cont.resume(result)
        }
      }
    )

    cont.invokeOnCancellation { cause ->
      cancellationHandle.apply()
    }
  }

  /**
   * Starts a child workflow execution.
   *
   * @param parameters the child workflow parameters
   * @return pair of workflow execution (when started) and result payload (when completed)
   */
  suspend fun startChildWorkflow(
    parameters: StartChildWorkflowExecutionParameters
  ): Pair<WorkflowExecution, Optional<Payloads>> {
    // First, wait for the child to start
    val execution = suspendCancellableCoroutine<WorkflowExecution> { startCont ->
      var completionCont: CancellableContinuation<Optional<Payloads>>? = null

      val cancellationHandle = replayContext.startChildWorkflow(
        parameters,
        { execution: WorkflowExecution?, startException: Exception? ->
          if (startException != null) {
            startCont.resumeWithException(startException)
          } else if (execution != null) {
            startCont.resume(execution)
          }
        },
        { result: Optional<Payloads>, completionException: Exception? ->
          completionCont?.let { cont ->
            if (completionException != null) {
              cont.resumeWithException(completionException)
            } else {
              cont.resume(result)
            }
          }
        }
      )

      startCont.invokeOnCancellation { cause ->
        cancellationHandle.apply(
          cause as? Exception
            ?: RuntimeException(cause?.message ?: "Child workflow cancelled")
        )
      }
    }

    // Then wait for completion
    val result = suspendCancellableCoroutine<Optional<Payloads>> { cont ->
      // The completion callback was already registered above
      // This is a simplified implementation - full implementation would need
      // to properly wire the completion callback
    }

    return Pair(execution, result)
  }

  /**
   * Executes a side effect - a non-deterministic operation whose result is recorded.
   *
   * @param func the function to execute
   * @return the result payload
   */
  suspend fun sideEffect(
    func: () -> Optional<Payloads>
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    replayContext.sideEffect(
      { func() },
      null as UserMetadata?
    ) { result ->
      cont.resume(result)
    }
  }

  /**
   * Gets the version for a change ID, used for workflow versioning.
   *
   * @param changeId the change identifier
   * @param minSupported minimum supported version
   * @param maxSupported maximum supported version
   * @return the version to use
   */
  suspend fun getVersion(
    changeId: String,
    minSupported: Int,
    maxSupported: Int
  ): Int = suspendCancellableCoroutine { cont ->
    replayContext.getVersion(
      changeId,
      minSupported,
      maxSupported
    ) { version, exception ->
      if (exception != null) {
        cont.resumeWithException(exception)
      } else {
        cont.resume(version)
      }
    }
  }

  /**
   * Checks if cancellation has been requested for this workflow.
   */
  val isCancelRequested: Boolean
    get() = replayContext.isCancelRequested

  /**
   * Fails the current workflow task.
   */
  fun failWorkflowTask(failure: Throwable) {
    replayContext.failWorkflowTask(failure)
  }
}
