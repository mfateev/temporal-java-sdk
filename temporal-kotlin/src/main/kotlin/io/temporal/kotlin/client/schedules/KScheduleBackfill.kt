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

import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import io.temporal.client.schedules.ScheduleBackfill
import java.time.Instant

/**
 * Time period and policy for actions taken as if their scheduled time has already passed.
 *
 * Use this to run scheduled actions for time periods in the past.
 *
 * Example:
 * ```kotlin
 * // Backfill the last 5 days
 * val backfill = KScheduleBackfill(
 *     startAt = Instant.now().minus(5, ChronoUnit.DAYS),
 *     endAt = Instant.now()
 * )
 *
 * // Backfill with allow-all overlap policy
 * val backfill = KScheduleBackfill(
 *     startAt = Instant.now().minus(1, ChronoUnit.HOURS),
 *     endAt = Instant.now(),
 *     overlapPolicy = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
 * )
 * ```
 *
 * @property startAt Start of the range to evaluate the schedule in (exclusive).
 * @property endAt End of the range to evaluate the schedule in (inclusive).
 * @property overlapPolicy Overlap policy to use for this backfill request.
 */
public data class KScheduleBackfill(
  val startAt: Instant,
  val endAt: Instant,
  val overlapPolicy: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
) {
  public companion object {
    /**
     * Create a KScheduleBackfill from a Java SDK ScheduleBackfill.
     */
    @JvmStatic
    public fun fromJava(backfill: ScheduleBackfill): KScheduleBackfill = KScheduleBackfill(
      startAt = backfill.startAt,
      endAt = backfill.endAt,
      overlapPolicy = backfill.overlapPolicy
    )
  }
}
