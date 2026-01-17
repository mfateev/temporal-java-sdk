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

import io.temporal.client.schedules.ScheduleRange

/**
 * Inclusive range for a schedule match value.
 *
 * Example:
 * ```kotlin
 * // Match value 5 only
 * KScheduleRange(5)
 *
 * // Match values 1 through 10
 * KScheduleRange(1, 10)
 *
 * // Match values 0, 2, 4, 6, 8, 10 (step of 2)
 * KScheduleRange(0, 10, 2)
 * ```
 *
 * @property start The inclusive start of the range
 * @property end The inclusive end of the range. Default if less than start is start.
 * @property step The step to take between each value. Default if 0 is 1.
 */
public data class KScheduleRange(
  val start: Int,
  val end: Int = 0,
  val step: Int = 0
) {
  init {
    require(start >= 0) { "start must be non-negative" }
    require(end >= 0) { "end must be non-negative" }
    require(step >= 0) { "step must be non-negative" }
  }

  /**
   * Converts this KScheduleRange to a Java SDK ScheduleRange.
   */
  internal fun toJava(): ScheduleRange = ScheduleRange(start, end, step)

  public companion object {
    /**
     * Create a KScheduleRange from a Java SDK ScheduleRange.
     */
    @JvmStatic
    public fun fromJava(range: ScheduleRange): KScheduleRange =
      KScheduleRange(range.start, range.end, range.step)
  }
}
