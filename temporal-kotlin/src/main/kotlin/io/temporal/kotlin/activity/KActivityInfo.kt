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

import io.temporal.activity.ActivityInfo
import java.time.Duration
import java.time.Instant

/**
 * Kotlin-friendly interface for activity execution information.
 *
 * Provides access to metadata about the currently executing activity with
 * Kotlin-idiomatic nullable types instead of Optional.
 *
 * Example:
 * ```kotlin
 * val info = KActivityContext.current.info
 * println("Activity ${info.activityType} attempt ${info.attempt}")
 * ```
 */
public interface KActivityInfo {
  /**
   * The namespace the activity is running in.
   */
  public val namespace: String

  /**
   * The ID of the workflow that started this activity.
   */
  public val workflowId: String

  /**
   * The run ID of the workflow that started this activity.
   */
  public val runId: String

  /**
   * The type/name of this activity.
   */
  public val activityType: String

  /**
   * The ID of this activity execution.
   */
  public val activityId: String

  /**
   * The task queue this activity is running on.
   */
  public val taskQueue: String

  /**
   * The current attempt number (1-based).
   */
  public val attempt: Int

  /**
   * The time when this activity task was scheduled.
   */
  public val scheduledTime: Instant?

  /**
   * The time when this activity task was started.
   */
  public val startedTime: Instant?

  /**
   * The timeout for schedule-to-close duration.
   */
  public val scheduleToCloseTimeout: Duration?

  /**
   * The timeout for start-to-close duration.
   */
  public val startToCloseTimeout: Duration?

  /**
   * The heartbeat timeout.
   */
  public val heartbeatTimeout: Duration?

  /**
   * Details from the last heartbeat, if any.
   * Useful for resuming work after a retry.
   */
  public val heartbeatDetails: Any?

  /**
   * Whether this is a local activity.
   */
  public val isLocal: Boolean

  /**
   * The task token used for async activity completion.
   */
  public val taskToken: ByteArray
}

// Internal implementation that wraps Java ActivityInfo
internal class KActivityInfoImpl(private val info: ActivityInfo) : KActivityInfo {
  override val namespace: String get() = info.namespace
  override val workflowId: String get() = info.workflowId
  override val runId: String get() = info.runId
  override val activityType: String get() = info.activityType
  override val activityId: String get() = info.activityId
  override val taskQueue: String get() = info.activityTaskQueue
  override val attempt: Int get() = info.attempt

  override val scheduledTime: Instant?
    get() = info.scheduledTimestamp.takeIf { it > 0 }?.let { Instant.ofEpochMilli(it) }

  override val startedTime: Instant?
    get() = info.currentAttemptScheduledTimestamp.takeIf { it > 0 }?.let { Instant.ofEpochMilli(it) }

  override val scheduleToCloseTimeout: Duration?
    get() = info.scheduleToCloseTimeout?.takeIf { !it.isZero }

  override val startToCloseTimeout: Duration?
    get() = info.startToCloseTimeout?.takeIf { !it.isZero }

  override val heartbeatTimeout: Duration?
    get() = info.heartbeatTimeout.takeIf { !it.isZero }

  override val heartbeatDetails: Any?
    get() = null // Retrieved separately via Activity.getHeartbeatDetails()

  override val isLocal: Boolean get() = info.isLocal

  override val taskToken: ByteArray get() = info.taskToken
}
