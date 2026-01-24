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

package io.temporal.kotlin.workflow

import io.temporal.api.enums.v1.ParentClosePolicy
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.common.SearchAttributes
import io.temporal.common.VersioningIntent
import io.temporal.common.context.ContextPropagator
import io.temporal.kotlin.common.KPriority
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.workflow.ChildWorkflowCancellationType
import kotlin.time.Duration

/**
 * Kotlin-native child workflow options with Duration support.
 *
 * This data class provides a Kotlin-idiomatic way to configure child workflow execution
 * using native `kotlin.time.Duration` values instead of `java.time.Duration`.
 *
 * Example:
 * ```kotlin
 * val options = KChildWorkflowOptions(
 *     workflowId = "child-workflow-123",
 *     taskQueue = "my-task-queue",
 *     workflowExecutionTimeout = 1.hours,
 *     retryOptions = KRetryOptions(maximumAttempts = 3)
 * )
 * ```
 *
 * @property namespace Namespace in which the child workflow should be started.
 * @property workflowId Workflow id to use when starting. If not specified, a UUID is generated.
 * @property workflowIdReusePolicy Specifies server behavior if a completed workflow with the same id exists.
 * @property workflowRunTimeout The time after which child workflow run is automatically terminated.
 * @property workflowExecutionTimeout The time after which child workflow execution is automatically terminated.
 * @property workflowTaskTimeout Maximum execution time of a single workflow task. Default is 10 seconds.
 * @property taskQueue Task queue to use for workflow tasks.
 * @property retryOptions Retry policy for the child workflow.
 * @property cronSchedule Cron schedule for the child workflow.
 * @property parentClosePolicy Specifies how this workflow reacts to the death of the parent workflow.
 * @property memo Additional non-indexed information in result of list workflow.
 * @property typedSearchAttributes Additional indexed information in result of list workflow.
 * @property contextPropagators List of context propagators to use during this workflow.
 * @property cancellationType Defines at which point the CanceledFailure exception is thrown.
 * @property versioningIntent Specifies whether this child workflow should run on a worker with a compatible Build Id.
 * @property staticSummary Single-line fixed summary for this workflow execution.
 * @property staticDetails General fixed details for this workflow execution.
 * @property priority Priority settings that control relative ordering of task processing.
 */
@Suppress("DEPRECATION")
public data class KChildWorkflowOptions(
  val namespace: String? = null,
  val workflowId: String? = null,
  val workflowIdReusePolicy: WorkflowIdReusePolicy? = null,
  val workflowRunTimeout: Duration? = null,
  val workflowExecutionTimeout: Duration? = null,
  val workflowTaskTimeout: Duration? = null,
  val taskQueue: String? = null,
  val retryOptions: KRetryOptions? = null,
  val cronSchedule: String? = null,
  val parentClosePolicy: ParentClosePolicy? = null,
  val memo: Map<String, Any>? = null,
  val typedSearchAttributes: SearchAttributes? = null,
  @property:InternalTemporalApi
  val contextPropagators: List<ContextPropagator>? = null,
  val cancellationType: ChildWorkflowCancellationType? = null,
  @Deprecated("Worker Versioning is now deprecated, please migrate to the Worker Deployment API")
  val versioningIntent: VersioningIntent? = null,
  val staticSummary: String? = null,
  val staticDetails: String? = null,
  val priority: KPriority? = null
)
