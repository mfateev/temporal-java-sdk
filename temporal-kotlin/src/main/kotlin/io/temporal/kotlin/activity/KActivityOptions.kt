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

package io.temporal.kotlin.activity

import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.toJava
import kotlin.time.Duration

/**
 * Kotlin-native activity options with Duration support.
 *
 * This data class provides a Kotlin-idiomatic way to configure activity execution
 * using native `kotlin.time.Duration` values instead of `java.time.Duration`.
 *
 * Example:
 * ```kotlin
 * val options = KActivityOptions(
 *     startToCloseTimeout = 30.seconds,
 *     heartbeatTimeout = 10.seconds,
 *     retryOptions = KRetryOptions(maximumAttempts = 3)
 * )
 * ```
 *
 * @property startToCloseTimeout Maximum time of a single Activity execution attempt.
 *           At least one of [startToCloseTimeout] or [scheduleToCloseTimeout] is required.
 * @property scheduleToCloseTimeout Total maximum time allowed for the Activity to complete,
 *           including retries. At least one of [startToCloseTimeout] or [scheduleToCloseTimeout]
 *           is required.
 * @property scheduleToStartTimeout Time that the Activity Task can stay in the Task Queue
 *           before it is picked up by a Worker.
 * @property heartbeatTimeout Heartbeat interval. Activity must call heartbeat before this
 *           interval passes after a previous heartbeat or Activity start.
 * @property taskQueue Task queue to use when dispatching the Activity Task.
 *           If not specified, the Activity uses the workflow's task queue.
 * @property retryOptions Retry policy for the Activity.
 * @property cancellationType How the Activity is cancelled when its parent workflow or scope
 *           is cancelled. Default is [ActivityCancellationType.TRY_CANCEL].
 * @property disableEagerExecution When true, disables eager activity execution.
 *           Default is false.
 */
public data class KActivityOptions(
  val startToCloseTimeout: Duration? = null,
  val scheduleToCloseTimeout: Duration? = null,
  val scheduleToStartTimeout: Duration? = null,
  val heartbeatTimeout: Duration? = null,
  val taskQueue: String? = null,
  val retryOptions: KRetryOptions? = null,
  val cancellationType: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
  val disableEagerExecution: Boolean = false
) {
  init {
    require(startToCloseTimeout != null || scheduleToCloseTimeout != null) {
      "At least one of startToCloseTimeout or scheduleToCloseTimeout must be specified"
    }
  }

  /**
   * Converts this [KActivityOptions] to the Java SDK [ActivityOptions].
   *
   * This conversion happens once when the activity is scheduled,
   * so there's no runtime overhead during workflow execution.
   */
  public fun toJavaOptions(): ActivityOptions {
    return ActivityOptions.newBuilder().apply {
      startToCloseTimeout?.let { setStartToCloseTimeout(it.toJava()) }
      scheduleToCloseTimeout?.let { setScheduleToCloseTimeout(it.toJava()) }
      scheduleToStartTimeout?.let { setScheduleToStartTimeout(it.toJava()) }
      heartbeatTimeout?.let { setHeartbeatTimeout(it.toJava()) }
      taskQueue?.let { setTaskQueue(it) }
      retryOptions?.let { setRetryOptions(it.toJavaOptions()) }
      setCancellationType(cancellationType)
      setDisableEagerExecution(disableEagerExecution)
    }.build()
  }
}
