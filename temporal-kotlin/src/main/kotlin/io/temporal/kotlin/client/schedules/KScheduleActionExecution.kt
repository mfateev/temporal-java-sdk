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

import io.temporal.client.schedules.ScheduleActionExecution
import io.temporal.client.schedules.ScheduleActionExecutionStartWorkflow

/**
 * Base class for an action execution.
 *
 * @see KScheduleActionExecutionStartWorkflow
 */
public sealed class KScheduleActionExecution {
  public companion object {
    /**
     * Create a KScheduleActionExecution from a Java SDK ScheduleActionExecution.
     */
    @JvmStatic
    public fun fromJava(execution: ScheduleActionExecution): KScheduleActionExecution = when (execution) {
      is ScheduleActionExecutionStartWorkflow ->
        KScheduleActionExecutionStartWorkflow.fromJava(execution)
      else -> throw IllegalArgumentException("Unknown schedule action execution type: ${execution::class}")
    }
  }
}

/**
 * Action execution representing a scheduled workflow start.
 *
 * @property workflowId The workflow ID of the scheduled workflow.
 * @property firstExecutionRunId The workflow run ID of the scheduled workflow.
 */
public data class KScheduleActionExecutionStartWorkflow(
  val workflowId: String,
  val firstExecutionRunId: String
) : KScheduleActionExecution() {
  public companion object {
    /**
     * Create from a Java SDK ScheduleActionExecutionStartWorkflow.
     */
    @JvmStatic
    public fun fromJava(execution: ScheduleActionExecutionStartWorkflow): KScheduleActionExecutionStartWorkflow =
      KScheduleActionExecutionStartWorkflow(
        workflowId = execution.workflowId,
        firstExecutionRunId = execution.firstExecutionRunId
      )
  }
}
