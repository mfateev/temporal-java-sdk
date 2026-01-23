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

import io.temporal.client.schedules.ScheduleIntervalSpec
import java.time.Duration

/**
 * Specification for scheduling on an interval.
 *
 * Matching times are expressed as: epoch + (n * every) + offset
 *
 * Example:
 * ```kotlin
 * // Run every hour
 * KScheduleIntervalSpec(every = Duration.ofHours(1))
 *
 * // Run every hour at minute 15 (e.g., 1:15, 2:15, 3:15)
 * KScheduleIntervalSpec(
 *     every = Duration.ofHours(1),
 *     offset = Duration.ofMinutes(15)
 * )
 *
 * // Run every 30 minutes
 * KScheduleIntervalSpec(every = Duration.ofMinutes(30))
 * ```
 *
 * @property every Period to repeat the interval. Required.
 * @property offset Fixed offset added to each interval period. Default is zero.
 */
public data class KScheduleIntervalSpec(
  val every: Duration,
  val offset: Duration = Duration.ZERO
) {
  init {
    require(!every.isNegative && !every.isZero) { "every must be positive" }
    require(!offset.isNegative) { "offset must not be negative" }
  }

  public companion object {
    /**
     * Create a KScheduleIntervalSpec from a Java SDK ScheduleIntervalSpec.
     */
    @JvmStatic
    public fun fromJava(spec: ScheduleIntervalSpec): KScheduleIntervalSpec =
      KScheduleIntervalSpec(
        every = spec.every,
        offset = spec.offset ?: Duration.ZERO
      )
  }
}
