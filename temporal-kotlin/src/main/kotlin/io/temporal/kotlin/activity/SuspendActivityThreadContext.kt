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
import io.temporal.activity.ManualActivityCompletionClient
import kotlinx.coroutines.ThreadContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Holds the activity execution context and manual completion client for suspend activities.
 *
 * This context is stored in a thread-local and propagated to coroutine threads via
 * [SuspendActivityThreadContextElement], allowing [KActivityContext.current.heartbeat] to work
 * correctly in suspend activities.
 */
internal data class SuspendActivityExecutionContext(
  val executionContext: ActivityExecutionContext,
  val completionClient: ManualActivityCompletionClient
)

/**
 * Thread-local storage for suspend activity context.
 *
 * This is used by [KActivityContext.current] to access the activity context from suspend activities
 * when the Java SDK's thread-local context is not available.
 */
internal object CurrentSuspendActivityContext {
  private val CURRENT = ThreadLocal<SuspendActivityExecutionContext?>()

  fun get(): SuspendActivityExecutionContext? = CURRENT.get()

  fun set(context: SuspendActivityExecutionContext?) {
    CURRENT.set(context)
  }
}

/**
 * Coroutine context element that propagates [SuspendActivityExecutionContext] to coroutine threads.
 *
 * This element implements [ThreadContextElement] to ensure the activity context is available
 * in the thread-local whenever a coroutine is executing on a thread. This allows
 * [KActivityContext.current] to access the activity context
 * even when the coroutine switches threads.
 *
 * Thread model:
 * - When a coroutine starts executing on a thread: [updateThreadContext] saves any existing
 *   context and sets our context in the thread-local
 * - When the coroutine suspends or completes: [restoreThreadContext] restores the previous context
 * - This ensures the correct context is always available for the currently executing coroutine
 */
internal class SuspendActivityThreadContextElement(
  private val context: SuspendActivityExecutionContext
) : ThreadContextElement<SuspendActivityExecutionContext?> {

  companion object Key : CoroutineContext.Key<SuspendActivityThreadContextElement>

  override val key: CoroutineContext.Key<*> = Key

  /**
   * Called when the coroutine starts executing on a thread.
   * Saves the current thread-local value and sets our context.
   */
  override fun updateThreadContext(context: CoroutineContext): SuspendActivityExecutionContext? {
    val oldValue = CurrentSuspendActivityContext.get()
    CurrentSuspendActivityContext.set(this.context)
    return oldValue
  }

  /**
   * Called when the coroutine suspends or completes on this thread.
   * Restores the previous thread-local value.
   */
  override fun restoreThreadContext(context: CoroutineContext, oldState: SuspendActivityExecutionContext?) {
    CurrentSuspendActivityContext.set(oldState)
  }
}
