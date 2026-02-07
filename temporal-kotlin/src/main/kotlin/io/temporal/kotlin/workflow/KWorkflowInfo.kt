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

import io.temporal.internal.common.ProtoConverters
import io.temporal.internal.common.RetryOptionsUtils
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.kotlin.common.KPriority
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.workflow.KotlinWorkflowContext
import io.temporal.kotlin.toKotlin
import io.temporal.workflow.WorkflowInfo
import java.time.Instant
import kotlin.time.Duration

/**
 * Provides information about the current Workflow Execution and Run.
 *
 * This is the Kotlin-friendly version of [WorkflowInfo] that uses nullable types
 * instead of Optional for better Kotlin interoperability.
 */
public interface KWorkflowInfo {

  /**
   * The Workflow Namespace.
   */
  public val namespace: String

  /**
   * The Workflow ID.
   */
  public val workflowId: String

  /**
   * The Workflow Type name.
   */
  public val workflowType: String

  /**
   * The current Run ID.
   *
   * Note: RunId is unique identifier of one workflow code execution. Reset changes RunId.
   */
  public val runId: String

  /**
   * The very first original RunId of the current Workflow Execution preserved along the
   * chain of ContinueAsNew, Retry, Cron and Reset.
   */
  public val firstExecutionRunId: String

  /**
   * Run ID of the previous Workflow Run which continued-as-new or retried or cron-scheduled
   * into the current Workflow Run. Null if this is the first run.
   */
  public val continuedExecutionRunId: String?

  /**
   * Original RunId of the current Workflow Run. This value is preserved during Reset
   * which changes RunID.
   */
  public val originalExecutionRunId: String

  /**
   * The Workflow Task Queue name.
   */
  public val taskQueue: String

  /**
   * The retry options for this workflow, or null if not set.
   */
  public val retryOptions: KRetryOptions?

  /**
   * Timeout for a Workflow Run.
   */
  public val workflowRunTimeout: Duration

  /**
   * Timeout for the Workflow Execution.
   */
  public val workflowExecutionTimeout: Duration

  /**
   * The time workflow run has started.
   */
  public val runStartedTimestamp: Instant

  /**
   * Workflow ID of the parent Workflow, or null if this is a top-level workflow.
   */
  public val parentWorkflowId: String?

  /**
   * Run ID of the parent Workflow, or null if this is a top-level workflow.
   */
  public val parentRunId: String?

  /**
   * Workflow ID of the root Workflow, or null if the workflow is its own root.
   */
  public val rootWorkflowId: String?

  /**
   * Run ID of the root Workflow, or null if the workflow is its own root.
   */
  public val rootRunId: String?

  /**
   * Workflow retry attempt handled by this Workflow code execution. Starts at 1.
   */
  public val attempt: Int

  /**
   * Workflow cron schedule, or empty string if not a cron workflow.
   */
  public val cronSchedule: String

  /**
   * Length of Workflow history up until the current moment of execution.
   * This value changes during the lifetime of a Workflow Execution.
   */
  public val historyLength: Long

  /**
   * Size of Workflow history in bytes up until the current moment of execution.
   * This value changes during the lifetime of a Workflow Execution.
   */
  public val historySize: Long

  /**
   * True if the server suggests continuing as new.
   * This value changes during the lifetime of a Workflow Execution.
   */
  public val isContinueAsNewSuggested: Boolean

  /**
   * The Build ID of the worker which executed the current Workflow Task.
   * May be null if the task was completed by a worker without a Build ID.
   */
  public val currentBuildId: String?

  /**
   * The priority of the workflow task.
   */
  public val priority: KPriority
}

// Internal implementation of KWorkflowInfo that wraps a Java WorkflowInfo.
internal class KWorkflowInfoImpl(private val javaInfo: WorkflowInfo) : KWorkflowInfo {

  override val namespace: String
    get() = javaInfo.namespace

  override val workflowId: String
    get() = javaInfo.workflowId

  override val workflowType: String
    get() = javaInfo.workflowType

  override val runId: String
    get() = javaInfo.runId

  override val firstExecutionRunId: String
    get() = javaInfo.firstExecutionRunId

  override val continuedExecutionRunId: String?
    get() = javaInfo.continuedExecutionRunId.orElse(null)

  override val originalExecutionRunId: String
    get() = javaInfo.originalExecutionRunId

  override val taskQueue: String
    get() = javaInfo.taskQueue

  override val retryOptions: KRetryOptions?
    get() = javaInfo.retryOptions?.toKotlin()

  override val workflowRunTimeout: Duration
    get() = javaInfo.workflowRunTimeout.toKotlin()

  override val workflowExecutionTimeout: Duration
    get() = javaInfo.workflowExecutionTimeout.toKotlin()

  override val runStartedTimestamp: Instant
    get() = Instant.ofEpochMilli(javaInfo.runStartedTimestampMillis)

  override val parentWorkflowId: String?
    get() = javaInfo.parentWorkflowId.orElse(null)

  override val parentRunId: String?
    get() = javaInfo.parentRunId.orElse(null)

  override val rootWorkflowId: String?
    get() = javaInfo.rootWorkflowId.orElse(null)

  override val rootRunId: String?
    get() = javaInfo.rootRunId.orElse(null)

  override val attempt: Int
    get() = javaInfo.attempt

  override val cronSchedule: String
    get() = javaInfo.cronSchedule

  override val historyLength: Long
    get() = javaInfo.historyLength

  override val historySize: Long
    get() = javaInfo.historySize

  override val isContinueAsNewSuggested: Boolean
    get() = javaInfo.isContinueAsNewSuggested

  override val currentBuildId: String?
    get() = javaInfo.currentBuildId.orElse(null)

  override val priority: KPriority
    get() = javaInfo.priority.toKotlin()
}

// Internal implementation of KWorkflowInfo that gets data from KotlinWorkflowContext.
// This implementation is used for Kotlin coroutine-based workflows and does not rely
// on Java SDK thread-local context.
@InternalTemporalApi
internal class KWorkflowInfoFromContext(private val context: KotlinWorkflowContext) : KWorkflowInfo {

  private val replayContext: ReplayWorkflowContext
    get() = context.replayContext

  override val namespace: String
    get() = replayContext.namespace

  override val workflowId: String
    get() = replayContext.workflowId

  override val workflowType: String
    get() = replayContext.workflowType.name

  override val runId: String
    get() = replayContext.runId

  override val firstExecutionRunId: String
    get() = replayContext.firstExecutionRunId

  override val continuedExecutionRunId: String?
    get() = replayContext.continuedExecutionRunId.orElse(null)

  override val originalExecutionRunId: String
    get() = replayContext.originalExecutionRunId

  override val taskQueue: String
    get() = replayContext.taskQueue

  override val retryOptions: KRetryOptions?
    get() = replayContext.retryPolicy?.let { RetryOptionsUtils.toRetryOptions(it).toKotlin() }

  override val workflowRunTimeout: Duration
    get() = replayContext.workflowRunTimeout.toKotlin()

  override val workflowExecutionTimeout: Duration
    get() = replayContext.workflowExecutionTimeout.toKotlin()

  override val runStartedTimestamp: Instant
    get() = Instant.ofEpochMilli(replayContext.runStartedTimestampMillis)

  override val parentWorkflowId: String?
    get() = replayContext.parentWorkflowExecution?.workflowId

  override val parentRunId: String?
    get() = replayContext.parentWorkflowExecution?.runId

  override val rootWorkflowId: String?
    get() = replayContext.rootWorkflowExecution?.workflowId

  override val rootRunId: String?
    get() = replayContext.rootWorkflowExecution?.runId

  override val attempt: Int
    get() = replayContext.attempt

  override val cronSchedule: String
    get() = replayContext.cronSchedule

  override val historyLength: Long
    get() = replayContext.lastWorkflowTaskStartedEventId

  override val historySize: Long
    get() = replayContext.historySize

  override val isContinueAsNewSuggested: Boolean
    get() = replayContext.isContinueAsNewSuggested

  override val currentBuildId: String?
    get() = replayContext.currentBuildId.orElse(null)

  override val priority: KPriority
    get() = ProtoConverters.fromProto(replayContext.priority).toKotlin()
}
