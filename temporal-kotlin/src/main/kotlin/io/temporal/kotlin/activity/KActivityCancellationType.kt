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

/**
 * Specifies how an activity's cancellation is handled when its parent workflow or scope is cancelled.
 *
 * This is the Kotlin equivalent of [ActivityCancellationType].
 */
public enum class KActivityCancellationType {
  /**
   * Wait for the Activity Execution to confirm any requested cancellation.
   *
   * An Activity Execution must heartbeat to receive a cancellation notification.
   * This can block the cancellation of a Workflow Execution for a long time if
   * the Activity Execution doesn't heartbeat or chooses to ignore the cancellation request.
   * The activity stub call will fail with [io.temporal.failure.CanceledFailure] only
   * after cancellation confirmation from the Activity Execution has been received.
   */
  WAIT_CANCELLATION_COMPLETED,

  /**
   * Send an Activity cancellation request to the server, and report cancellation to the
   * Workflow Execution by causing the activity stub call to fail with
   * [io.temporal.failure.CanceledFailure].
   */
  TRY_CANCEL,

  /**
   * Do not request cancellation of the Activity Execution at all (no request is sent to the server)
   * and immediately report cancellation to the Workflow Execution by causing the activity stub call
   * to fail with [io.temporal.failure.CanceledFailure] immediately.
   */
  ABANDON;

  /**
   * Converts this Kotlin enum to the Java SDK [ActivityCancellationType].
   */
  public fun toJava(): ActivityCancellationType = when (this) {
    WAIT_CANCELLATION_COMPLETED -> ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
    TRY_CANCEL -> ActivityCancellationType.TRY_CANCEL
    ABANDON -> ActivityCancellationType.ABANDON
  }

  public companion object {
    /**
     * Converts a Java SDK [ActivityCancellationType] to the Kotlin equivalent.
     */
    @JvmStatic
    public fun fromJava(java: ActivityCancellationType): KActivityCancellationType = when (java) {
      ActivityCancellationType.WAIT_CANCELLATION_COMPLETED -> WAIT_CANCELLATION_COMPLETED
      ActivityCancellationType.TRY_CANCEL -> TRY_CANCEL
      ActivityCancellationType.ABANDON -> ABANDON
    }
  }
}

/**
 * Converts a Java SDK [ActivityCancellationType] to the Kotlin SDK [KActivityCancellationType].
 */
public fun ActivityCancellationType.toKotlin(): KActivityCancellationType =
  KActivityCancellationType.fromJava(this)
