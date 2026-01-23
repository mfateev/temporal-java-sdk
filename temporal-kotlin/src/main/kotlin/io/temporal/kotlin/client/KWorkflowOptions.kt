@file:OptIn(kotlin.time.ExperimentalTime::class)

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

package io.temporal.kotlin.client

import io.temporal.api.common.v1.Callback
import io.temporal.api.common.v1.Link
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.common.Priority
import io.temporal.common.SearchAttributes
import io.temporal.common.VersioningOverride
import io.temporal.common.context.ContextPropagator
import io.temporal.kotlin.common.KRetryOptions
import kotlin.time.Duration

/**
 * Kotlin-native workflow options with Duration support.
 *
 * This data class provides a Kotlin-idiomatic way to configure workflow execution
 * using native `kotlin.time.Duration` values instead of `java.time.Duration`.
 *
 * Example:
 * ```kotlin
 * val options = KWorkflowOptions(
 *     workflowId = "my-workflow-123",
 *     taskQueue = "my-task-queue",
 *     workflowExecutionTimeout = 1.hours,
 *     retryOptions = KRetryOptions(maximumAttempts = 3)
 * )
 * ```
 *
 * @property workflowId Workflow id to use when starting. If not specified, a UUID is generated.
 * @property workflowIdReusePolicy Specifies server behavior if a completed workflow with the same id exists.
 * @property workflowIdConflictPolicy Specifies server behavior if a Running workflow with the same id exists.
 * @property workflowRunTimeout The time after which workflow run is automatically terminated.
 * @property workflowExecutionTimeout The time after which workflow execution is automatically terminated.
 * @property workflowTaskTimeout Maximum execution time of a single workflow task. Default is 10 seconds.
 * @property taskQueue Task queue to use for workflow tasks.
 * @property retryOptions Retry policy for the workflow.
 * @property cronSchedule Cron schedule for the workflow.
 * @property memo Additional non-indexed information in result of list workflow.
 * @property typedSearchAttributes Additional indexed information in result of list workflow.
 * @property contextPropagators List of context propagators to use during this workflow.
 * @property disableEagerExecution If true, disables eager local execution of the workflow task.
 * @property startDelay Time to wait before dispatching the first workflow task.
 * @property staticSummary Single-line fixed summary for this workflow execution.
 * @property staticDetails General fixed details for this workflow execution.
 * @property requestId A unique identifier for this start request.
 * @property completionCallbacks Callbacks to be called by the server when this workflow reaches a terminal state.
 * @property links Links to be associated with the workflow.
 * @property onConflictOptions Workflow ID conflict options used with WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING.
 * @property priority Priority settings that control relative ordering of task processing.
 * @property versioningOverride Versioning override to use when starting this workflow.
 */
public data class KWorkflowOptions(
  val workflowId: String? = null,
  val workflowIdReusePolicy: WorkflowIdReusePolicy? = null,
  val workflowIdConflictPolicy: WorkflowIdConflictPolicy? = null,
  val workflowRunTimeout: Duration? = null,
  val workflowExecutionTimeout: Duration? = null,
  val workflowTaskTimeout: Duration? = null,
  val taskQueue: String? = null,
  val retryOptions: KRetryOptions? = null,
  val cronSchedule: String? = null,
  val memo: Map<String, Any>? = null,
  val typedSearchAttributes: SearchAttributes? = null,
  val contextPropagators: List<ContextPropagator>? = null,
  val disableEagerExecution: Boolean = true,
  val startDelay: Duration? = null,
  val staticSummary: String? = null,
  val staticDetails: String? = null,
  val requestId: String? = null,
  val completionCallbacks: List<Callback>? = null,
  val links: List<Link>? = null,
  val onConflictOptions: KOnConflictOptions? = null,
  val priority: Priority? = null,
  val versioningOverride: VersioningOverride? = null
)
