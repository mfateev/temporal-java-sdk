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

import io.temporal.kotlin.workflow.KWorkflow
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Delay
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Runnable
import java.util.LinkedList
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.CoroutineContext

/**
 * A deterministic coroutine dispatcher for Temporal workflow execution.
 *
 * This dispatcher ensures that coroutines are executed in a deterministic order,
 * which is essential for Temporal workflow replay. It maintains a queue of ready
 * coroutines and executes them one at a time in FIFO order.
 *
 * The dispatcher integrates with [KotlinDelay] to handle timer-based suspension
 * using Temporal's timer mechanism.
 */
@InternalTemporalApi
@OptIn(InternalCoroutinesApi::class)
internal class KotlinCoroutineDispatcher(
  private val workflowContext: KotlinWorkflowContext
) : CoroutineDispatcher(), Delay {

  private val lock = ReentrantLock()
  private val readyQueue = LinkedList<Runnable>()
  private var closed = false
  private var currentlyExecuting = false

  /**
   * Returns true if dispatch is needed.
   *
   * When already executing within the workflow context (during a callback),
   * we return false to allow immediate resumption without queuing.
   * This is crucial for activity/timer callbacks that need to resume
   * the coroutine synchronously.
   */
  override fun isDispatchNeeded(context: CoroutineContext): Boolean {
    return lock.withLock {
      // If we're already executing a workflow task, don't dispatch - run immediately
      !currentlyExecuting
    }
  }

  /**
   * Dispatches a coroutine for execution.
   *
   * The coroutine is added to the ready queue and will be executed
   * when [runUntilAllBlocked] is called.
   */
  override fun dispatch(context: CoroutineContext, block: Runnable) {
    lock.withLock {
      if (closed) {
        throw IllegalStateException("Dispatcher has been closed")
      }
      readyQueue.addLast(block)
    }
  }

  /**
   * Schedules a coroutine to resume after a delay.
   *
   * This delegates to [KotlinDelay] which uses Temporal timers.
   */
  override fun scheduleResumeAfterDelay(
    timeMillis: Long,
    continuation: kotlinx.coroutines.CancellableContinuation<Unit>
  ) {
    // Delegate to KotlinDelay for timer handling
    KotlinDelay.scheduleResumeAfterDelay(
      timeMillis,
      continuation,
      workflowContext,
      this
    )
  }

  /**
   * Invokes a block after a delay.
   *
   * This is used for delayed coroutine execution.
   */
  override fun invokeOnTimeout(
    timeMillis: Long,
    block: Runnable,
    context: CoroutineContext
  ): DisposableHandle {
    return KotlinDelay.invokeOnTimeout(
      timeMillis,
      block,
      workflowContext,
      this
    )
  }

  /**
   * Executes all ready coroutines until all are blocked or completed.
   *
   * This method runs the coroutine event loop, executing each ready
   * coroutine in FIFO order. It continues until all coroutines are
   * either completed or waiting for external events (timers, activities, etc.).
   *
   * @param deadlockDetectionTimeoutMs maximum time in milliseconds to wait
   *        for a single coroutine to yield. If exceeded, a deadlock is assumed.
   * @return true if all coroutines have completed, false if some are still blocked
   * @throws WorkflowDeadlockException if a coroutine runs longer than the timeout
   */
  fun runUntilAllBlocked(deadlockDetectionTimeoutMs: Long): Boolean {
    // Process all ready tasks
    processAllReadyTasks(deadlockDetectionTimeoutMs)

    // After processing all tasks, notify condition waiters to re-check.
    // Any task execution could have changed state that conditions depend on.
    workflowContext.notifyConditionWaiters()

    // Process any newly ready tasks (from conditions that became true)
    processAllReadyTasks(deadlockDetectionTimeoutMs)

    return lock.withLock { readyQueue.isEmpty() }
  }

  private fun processAllReadyTasks(deadlockDetectionTimeoutMs: Long) {
    while (true) {
      val task = lock.withLock {
        if (readyQueue.isEmpty()) {
          return
        }
        readyQueue.removeFirst()
      }

      executeWithDeadlockDetection(task, deadlockDetectionTimeoutMs)
    }
  }

  private fun executeWithDeadlockDetection(task: Runnable, timeoutMs: Long) {
    lock.withLock {
      currentlyExecuting = true
    }

    val startTime = System.nanoTime()
    // Set the workflow context for KWorkflow APIs before running the task
    val previousContext = KWorkflow.currentContext.get()
    KWorkflow.currentContext.set(workflowContext)
    try {
      task.run()
    } finally {
      // Restore previous context (usually null)
      KWorkflow.currentContext.set(previousContext)
      lock.withLock {
        currentlyExecuting = false
      }
    }

    val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
    if (elapsedMs > timeoutMs) {
      throw WorkflowDeadlockException(
        "Potential deadlock detected: coroutine ran for ${elapsedMs}ms " +
          "without yielding (timeout: ${timeoutMs}ms)"
      )
    }
  }

  /**
   * Checks if there are any coroutines ready to execute.
   */
  fun hasReadyCoroutines(): Boolean = lock.withLock {
    readyQueue.isNotEmpty()
  }

  /**
   * Checks if all coroutines have completed.
   */
  fun isDone(): Boolean = lock.withLock {
    readyQueue.isEmpty() && !currentlyExecuting
  }

  /**
   * Closes the dispatcher and cancels any pending coroutines.
   */
  fun close() {
    lock.withLock {
      closed = true
      readyQueue.clear()
    }
  }

  /**
   * Executes a runnable immediately in the workflow context.
   *
   * This is used for signal and update handlers that need to run
   * before other coroutines.
   */
  fun executeImmediately(block: Runnable) {
    lock.withLock {
      if (closed) {
        throw IllegalStateException("Dispatcher has been closed")
      }
      // Add to front of queue to execute before other pending work
      readyQueue.addFirst(block)
    }
  }
}

/**
 * Exception thrown when a potential deadlock is detected in workflow code.
 */
@InternalTemporalApi
class WorkflowDeadlockException(message: String) : RuntimeException(message)
