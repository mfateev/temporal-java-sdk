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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.workflow

import io.temporal.common.SearchAttributes
import io.temporal.common.context.ContextPropagator
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.toJava
import io.temporal.workflow.ContinueAsNewOptions
import kotlin.time.Duration

/**
 * Options for continuing a workflow as a new execution.
 *
 * Continue-as-new allows a workflow to complete and immediately start a new execution
 * with fresh history. This is useful for:
 * - **Long-running workflows**: Prevent history from growing too large by periodically
 *   continuing as new to reset the history size
 * - **Recurring workflows**: Implement patterns where a workflow processes batches and
 *   then continues with the next batch
 * - **State reset**: Start fresh with new parameters while maintaining the workflow ID
 *
 * All fields are optional. When null, the value is inherited from the current workflow execution.
 *
 * Example:
 * ```kotlin
 * // Continue with same settings but new arguments
 * KWorkflow.continueAsNew(newBatchId, newOffset)
 *
 * // Continue with modified options
 * KWorkflow.continueAsNew(
 *   KContinueAsNewOptions(
 *     taskQueue = "high-priority-queue",
 *     workflowRunTimeout = 1.hours
 *   ),
 *   newBatchId, newOffset
 * )
 *
 * // Continue as a different workflow type
 * KWorkflow.continueAsNew(
 *   "ProcessingWorkflowV2",
 *   KContinueAsNewOptions(workflowRunTimeout = 2.hours),
 *   newData
 * )
 * ```
 *
 * @property workflowRunTimeout Maximum time for a single workflow run. Resets on each continue-as-new.
 *   Null means inherit from the current workflow.
 * @property taskQueue Task queue for the new workflow execution.
 *   Null means use the same task queue as the current workflow.
 * @property retryOptions Retry options for the new workflow execution.
 *   Null means inherit from the current workflow.
 * @property workflowTaskTimeout Maximum time for a single workflow task.
 *   Null means inherit from the current workflow.
 * @property memo Memo fields for the new workflow execution.
 *   Null means inherit from the current workflow.
 * @property typedSearchAttributes Search attributes for the new workflow execution.
 *   Null means inherit from the current workflow.
 * @property contextPropagators Context propagators for the new workflow execution.
 *   Null means inherit from the current workflow.
 */
public data class KContinueAsNewOptions(
  val workflowRunTimeout: Duration? = null,
  val taskQueue: String? = null,
  val retryOptions: KRetryOptions? = null,
  val workflowTaskTimeout: Duration? = null,
  val memo: Map<String, Any>? = null,
  val typedSearchAttributes: SearchAttributes? = null,
  val contextPropagators: List<ContextPropagator>? = null
) {

  /**
   * Converts this Kotlin options class to the Java SDK's [ContinueAsNewOptions].
   *
   * @return the equivalent Java SDK options
   */
  public fun toJavaOptions(): ContinueAsNewOptions {
    val builder = ContinueAsNewOptions.newBuilder()

    workflowRunTimeout?.let { builder.setWorkflowRunTimeout(it.toJava()) }
    taskQueue?.let { builder.setTaskQueue(it) }
    retryOptions?.let { builder.setRetryOptions(it.toJavaOptions()) }
    workflowTaskTimeout?.let { builder.setWorkflowTaskTimeout(it.toJava()) }
    memo?.let { builder.setMemo(it) }
    typedSearchAttributes?.let { builder.setTypedSearchAttributes(it) }
    contextPropagators?.let { builder.setContextPropagators(it) }

    return builder.build()
  }

  public companion object {
    /**
     * Default options that inherit all settings from the current workflow.
     */
    public val DEFAULT: KContinueAsNewOptions = KContinueAsNewOptions()
  }
}
