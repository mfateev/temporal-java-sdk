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

package io.temporal.kotlin.client.schedules

import io.temporal.common.metadata.POJOWorkflowInterfaceMetadata
import io.temporal.kotlin.client.KWorkflowOptions
import kotlin.reflect.KClass

/**
 * Schedule action to start a workflow.
 *
 * Example:
 * ```kotlin
 * // Start a workflow with type and options
 * KScheduleActionStartWorkflow(
 *     workflowType = "MyWorkflow",
 *     options = KWorkflowOptions(
 *         workflowId = "my-schedule-workflow",
 *         taskQueue = "my-task-queue"
 *     ),
 *     arguments = listOf("arg1", 42)
 * )
 *
 * // Start a workflow using interface class
 * KScheduleActionStartWorkflow.fromWorkflowInterface(
 *     MyWorkflow::class,
 *     options = KWorkflowOptions(
 *         workflowId = "my-schedule-workflow",
 *         taskQueue = "my-task-queue"
 *     ),
 *     arguments = listOf("arg1", 42)
 * )
 * ```
 *
 * @property workflowType Name of the workflow type.
 * @property options Workflow options. ID and TaskQueue are required.
 * @property arguments Arguments for the workflow.
 */
public data class KScheduleActionStartWorkflow(
  val workflowType: String,
  val options: KWorkflowOptions,
  val arguments: List<Any?> = emptyList()
) : KScheduleAction() {
  public companion object {
    /**
     * Create a KScheduleActionStartWorkflow from a workflow interface class.
     *
     * @param workflowInterface The workflow interface class.
     * @param options Workflow options. ID and TaskQueue are required.
     * @param arguments Arguments for the workflow.
     */
    @JvmStatic
    public fun <T : Any> fromWorkflowInterface(
      workflowInterface: KClass<T>,
      options: KWorkflowOptions,
      arguments: List<Any?> = emptyList()
    ): KScheduleActionStartWorkflow {
      val metadata = POJOWorkflowInterfaceMetadata.newInstance(workflowInterface.java, true)
      val workflowType = metadata.workflowType.orElseThrow {
        IllegalArgumentException("${workflowInterface.simpleName} is not a valid workflow interface")
      }
      return KScheduleActionStartWorkflow(
        workflowType = workflowType,
        options = options,
        arguments = arguments
      )
    }

    /**
     * Create a KScheduleActionStartWorkflow from a workflow interface class.
     */
    public inline fun <reified T : Any> fromWorkflowInterface(
      options: KWorkflowOptions,
      arguments: List<Any?> = emptyList()
    ): KScheduleActionStartWorkflow = fromWorkflowInterface(T::class, options, arguments)
  }
}
