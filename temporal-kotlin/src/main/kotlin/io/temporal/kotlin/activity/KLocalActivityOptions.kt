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

import io.temporal.activity.LocalActivityOptions
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.toJava
import kotlin.time.Duration

/**
 * Kotlin-native local activity options with Duration support.
 *
 * This data class provides a Kotlin-idiomatic way to configure local activity execution
 * using native `kotlin.time.Duration` values instead of `java.time.Duration`.
 *
 * Local activities are activities that execute in the same process as the workflow,
 * avoiding the overhead of scheduling through the Temporal service. They are best
 * suited for short operations that don't need heartbeating or independent retries.
 *
 * Example:
 * ```kotlin
 * val options = KLocalActivityOptions(
 *     startToCloseTimeout = 5.seconds,
 *     retryOptions = KRetryOptions(maximumAttempts = 3)
 * )
 * ```
 *
 * @property startToCloseTimeout Maximum time of a single Local Activity execution attempt.
 *           At least one of [startToCloseTimeout] or [scheduleToCloseTimeout] is required.
 * @property scheduleToCloseTimeout Total maximum time allowed for the Local Activity to complete,
 *           including retries. At least one of [startToCloseTimeout] or [scheduleToCloseTimeout]
 *           is required.
 * @property localRetryThreshold Maximum time to retry locally before routing to the server.
 *           If not set, defaults to [scheduleToCloseTimeout].
 * @property retryOptions Retry policy for the Local Activity.
 */
public data class KLocalActivityOptions(
  val startToCloseTimeout: Duration? = null,
  val scheduleToCloseTimeout: Duration? = null,
  val localRetryThreshold: Duration? = null,
  val retryOptions: KRetryOptions? = null
) {
  init {
    require(startToCloseTimeout != null || scheduleToCloseTimeout != null) {
      "At least one of startToCloseTimeout or scheduleToCloseTimeout must be specified"
    }
  }

  /**
   * Converts this [KLocalActivityOptions] to the Java SDK [LocalActivityOptions].
   *
   * This conversion happens once when the local activity is scheduled,
   * so there's no runtime overhead during workflow execution.
   */
  public fun toJavaOptions(): LocalActivityOptions {
    return LocalActivityOptions.newBuilder().apply {
      startToCloseTimeout?.let { setStartToCloseTimeout(it.toJava()) }
      scheduleToCloseTimeout?.let { setScheduleToCloseTimeout(it.toJava()) }
      localRetryThreshold?.let { setLocalRetryThreshold(it.toJava()) }
      retryOptions?.let { setRetryOptions(it.toJavaOptions()) }
    }.build()
  }
}
