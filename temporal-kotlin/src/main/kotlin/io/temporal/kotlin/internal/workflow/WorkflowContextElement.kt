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

package io.temporal.kotlin.internal.workflow

import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.workflow.KWorkflow
import kotlinx.coroutines.ThreadContextElement
import kotlin.coroutines.CoroutineContext

/**
 * A [ThreadContextElement] that propagates [KotlinWorkflowContext] through the coroutine tree.
 *
 * This ensures that when coroutines are launched with `async` or `launch`, the workflow context
 * is properly set in the ThreadLocal before the coroutine code runs and restored afterwards.
 *
 * This is important because:
 * 1. [KWorkflow] static methods access the context via ThreadLocal
 * 2. Kotlin coroutines may run on different threads or be suspended/resumed
 * 3. Without this, nested `async` blocks might not have access to the workflow context
 *
 * Usage: Add this element to the CoroutineScope when starting workflow execution:
 * ```kotlin
 * val scope = CoroutineScope(
 *   dispatcher + WorkflowContextElement(workflowContext) + ...
 * )
 * ```
 */
@InternalTemporalApi
internal class WorkflowContextElement(
  private val workflowContext: KotlinWorkflowContext
) : ThreadContextElement<KotlinWorkflowContext?> {

  companion object Key : CoroutineContext.Key<WorkflowContextElement>

  override val key: CoroutineContext.Key<WorkflowContextElement> = Key

  /**
   * Called when a coroutine is about to start or resume on a thread.
   * Sets the workflow context in the ThreadLocal and returns the previous value.
   */
  override fun updateThreadContext(context: CoroutineContext): KotlinWorkflowContext? {
    val oldContext = KWorkflow.currentContext.get()
    KWorkflow.currentContext.set(workflowContext)
    return oldContext
  }

  /**
   * Called when a coroutine is suspended or completes on a thread.
   * Restores the previous ThreadLocal value.
   */
  override fun restoreThreadContext(context: CoroutineContext, oldState: KotlinWorkflowContext?) {
    KWorkflow.currentContext.set(oldState)
  }
}
