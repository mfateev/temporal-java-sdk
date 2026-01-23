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

package io.temporal.kotlin.internal

import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.client.OnConflictOptions
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.client.KOnConflictOptions
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.toJava
import io.temporal.kotlin.workflow.KChildWorkflowOptions
import io.temporal.kotlin.workflow.KContinueAsNewOptions
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.ContinueAsNewOptions

/**
 * Internal converter for Kotlin options to Java SDK options.
 *
 * This object keeps Java SDK types out of the public API by centralizing
 * all conversion logic here.
 */
@InternalTemporalApi
public object KOptionsConverters {

  /**
   * Converts [KActivityOptions] to [ActivityOptions].
   */
  fun toJava(options: KActivityOptions): ActivityOptions {
    return ActivityOptions.newBuilder().apply {
      options.startToCloseTimeout?.let { setStartToCloseTimeout(it.toJava()) }
      options.scheduleToCloseTimeout?.let { setScheduleToCloseTimeout(it.toJava()) }
      options.scheduleToStartTimeout?.let { setScheduleToStartTimeout(it.toJava()) }
      options.heartbeatTimeout?.let { setHeartbeatTimeout(it.toJava()) }
      options.taskQueue?.let { setTaskQueue(it) }
      options.retryOptions?.let { setRetryOptions(toJava(it)) }
      options.cancellationType?.let { setCancellationType(it) }
      setDisableEagerExecution(options.disableEagerExecution)
    }.build()
  }

  /**
   * Converts [KLocalActivityOptions] to [LocalActivityOptions].
   */
  fun toJava(options: KLocalActivityOptions): LocalActivityOptions {
    return LocalActivityOptions.newBuilder().apply {
      options.startToCloseTimeout?.let { setStartToCloseTimeout(it.toJava()) }
      options.scheduleToCloseTimeout?.let { setScheduleToCloseTimeout(it.toJava()) }
      options.localRetryThreshold?.let { setLocalRetryThreshold(it.toJava()) }
      options.retryOptions?.let { setRetryOptions(toJava(it)) }
    }.build()
  }

  /**
   * Converts [KRetryOptions] to [RetryOptions].
   */
  fun toJava(options: KRetryOptions): RetryOptions {
    return RetryOptions.newBuilder().apply {
      setInitialInterval(options.initialInterval.toJava())
      setBackoffCoefficient(options.backoffCoefficient)
      options.maximumInterval?.let { setMaximumInterval(it.toJava()) }
      if (options.maximumAttempts > 0) {
        setMaximumAttempts(options.maximumAttempts)
      }
      if (options.doNotRetry.isNotEmpty()) {
        setDoNotRetry(*options.doNotRetry.toTypedArray())
      }
    }.build()
  }

  /**
   * Converts [KChildWorkflowOptions] to [ChildWorkflowOptions].
   */
  @Suppress("DEPRECATION")
  fun toJava(options: KChildWorkflowOptions): ChildWorkflowOptions {
    return ChildWorkflowOptions.newBuilder().apply {
      options.namespace?.let { setNamespace(it) }
      options.workflowId?.let { setWorkflowId(it) }
      options.workflowIdReusePolicy?.let { setWorkflowIdReusePolicy(it) }
      options.workflowRunTimeout?.let { setWorkflowRunTimeout(it.toJava()) }
      options.workflowExecutionTimeout?.let { setWorkflowExecutionTimeout(it.toJava()) }
      options.workflowTaskTimeout?.let { setWorkflowTaskTimeout(it.toJava()) }
      options.taskQueue?.let { setTaskQueue(it) }
      options.retryOptions?.let { setRetryOptions(toJava(it)) }
      options.cronSchedule?.let { setCronSchedule(it) }
      options.parentClosePolicy?.let { setParentClosePolicy(it) }
      options.memo?.let { setMemo(it) }
      options.typedSearchAttributes?.let { setTypedSearchAttributes(it) }
      options.contextPropagators?.let { setContextPropagators(it) }
      options.cancellationType?.let { setCancellationType(it) }
      @Suppress("DEPRECATION")
      options.versioningIntent?.let { setVersioningIntent(it) }
      options.staticSummary?.let { setStaticSummary(it) }
      options.staticDetails?.let { setStaticDetails(it) }
      options.priority?.let { setPriority(it) }
    }.build()
  }

  /**
   * Converts [KChildWorkflowOptions] to [ChildWorkflowOptions] with a default workflowId.
   *
   * @param options the Kotlin options
   * @param workflowId the workflow ID to use if not already specified in options
   */
  @Suppress("DEPRECATION")
  fun toJava(options: KChildWorkflowOptions, workflowId: String): ChildWorkflowOptions {
    return ChildWorkflowOptions.newBuilder().apply {
      options.namespace?.let { setNamespace(it) }
      // Use workflowId from options if present, otherwise use the provided one
      setWorkflowId(options.workflowId ?: workflowId)
      options.workflowIdReusePolicy?.let { setWorkflowIdReusePolicy(it) }
      options.workflowRunTimeout?.let { setWorkflowRunTimeout(it.toJava()) }
      options.workflowExecutionTimeout?.let { setWorkflowExecutionTimeout(it.toJava()) }
      options.workflowTaskTimeout?.let { setWorkflowTaskTimeout(it.toJava()) }
      options.taskQueue?.let { setTaskQueue(it) }
      options.retryOptions?.let { setRetryOptions(toJava(it)) }
      options.cronSchedule?.let { setCronSchedule(it) }
      options.parentClosePolicy?.let { setParentClosePolicy(it) }
      options.memo?.let { setMemo(it) }
      options.typedSearchAttributes?.let { setTypedSearchAttributes(it) }
      options.contextPropagators?.let { setContextPropagators(it) }
      options.cancellationType?.let { setCancellationType(it) }
      @Suppress("DEPRECATION")
      options.versioningIntent?.let { setVersioningIntent(it) }
      options.staticSummary?.let { setStaticSummary(it) }
      options.staticDetails?.let { setStaticDetails(it) }
      options.priority?.let { setPriority(it) }
    }.build()
  }

  /**
   * Converts [KContinueAsNewOptions] to [ContinueAsNewOptions].
   */
  fun toJava(options: KContinueAsNewOptions): ContinueAsNewOptions {
    val builder = ContinueAsNewOptions.newBuilder()

    options.workflowRunTimeout?.let { builder.setWorkflowRunTimeout(it.toJava()) }
    options.taskQueue?.let { builder.setTaskQueue(it) }
    options.retryOptions?.let { builder.setRetryOptions(toJava(it)) }
    options.workflowTaskTimeout?.let { builder.setWorkflowTaskTimeout(it.toJava()) }
    options.memo?.let { builder.setMemo(it) }
    options.typedSearchAttributes?.let { builder.setTypedSearchAttributes(it) }
    options.contextPropagators?.let { builder.setContextPropagators(it) }

    return builder.build()
  }

  /**
   * Converts [KWorkflowOptions] to [WorkflowOptions].
   */
  fun toJava(options: KWorkflowOptions): WorkflowOptions {
    return WorkflowOptions.newBuilder().apply {
      options.workflowId?.let { setWorkflowId(it) }
      options.workflowIdReusePolicy?.let { setWorkflowIdReusePolicy(it) }
      options.workflowIdConflictPolicy?.let { setWorkflowIdConflictPolicy(it) }
      options.workflowRunTimeout?.let { setWorkflowRunTimeout(it.toJava()) }
      options.workflowExecutionTimeout?.let { setWorkflowExecutionTimeout(it.toJava()) }
      options.workflowTaskTimeout?.let { setWorkflowTaskTimeout(it.toJava()) }
      options.taskQueue?.let { setTaskQueue(it) }
      options.retryOptions?.let { setRetryOptions(toJava(it)) }
      options.cronSchedule?.let { setCronSchedule(it) }
      options.memo?.let { setMemo(it) }
      options.typedSearchAttributes?.let { setTypedSearchAttributes(it) }
      options.contextPropagators?.let { setContextPropagators(it) }
      setDisableEagerExecution(options.disableEagerExecution)
      options.startDelay?.let { setStartDelay(it.toJava()) }
      options.staticSummary?.let { setStaticSummary(it) }
      options.staticDetails?.let { setStaticDetails(it) }
      options.requestId?.let { setRequestId(it) }
      options.completionCallbacks?.let { setCompletionCallbacks(it) }
      options.links?.let { setLinks(it) }
      options.onConflictOptions?.let { setOnConflictOptions(toJava(it)) }
      options.priority?.let { setPriority(it) }
      options.versioningOverride?.let { setVersioningOverride(it) }
    }.build()
  }

  /**
   * Converts [KOnConflictOptions] to [OnConflictOptions].
   */
  fun toJava(options: KOnConflictOptions): OnConflictOptions = OnConflictOptions.newBuilder()
    .setAttachRequestId(options.attachRequestId)
    .setAttachCompletionCallbacks(options.attachCompletionCallbacks)
    .setAttachLinks(options.attachLinks)
    .build()
}
