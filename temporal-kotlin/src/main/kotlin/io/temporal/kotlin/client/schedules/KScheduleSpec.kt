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

import io.temporal.client.schedules.ScheduleSpec
import java.time.Duration
import java.time.Instant

/**
 * Specification of the times scheduled actions may occur.
 *
 * The times are the union of [calendars], [intervals], and [cronExpressions]
 * excluding anything in [skip].
 *
 * Example:
 * ```kotlin
 * // Run every hour
 * KScheduleSpec(
 *     intervals = listOf(KScheduleIntervalSpec(every = Duration.ofHours(1)))
 * )
 *
 * // Run at noon every weekday
 * KScheduleSpec(
 *     calendars = listOf(
 *         KScheduleCalendarSpec(
 *             hour = listOf(KScheduleRange(12)),
 *             dayOfWeek = listOf(KScheduleRange(1, 5))  // Mon-Fri
 *         )
 *     )
 * )
 *
 * // Run every 30 minutes with 1 minute jitter, in Pacific time
 * KScheduleSpec(
 *     intervals = listOf(KScheduleIntervalSpec(every = Duration.ofMinutes(30))),
 *     jitter = Duration.ofMinutes(1),
 *     timeZoneName = "US/Pacific"
 * )
 * ```
 *
 * @property calendars Calendar-based specification of times.
 * @property intervals Interval-based specification of times.
 * @property cronExpressions Cron expressions. For migration from legacy cron workflows.
 *           New uses should prefer [calendars] or [intervals].
 * @property skip Calendar-based specification of times to skip.
 * @property startAt Start time of the schedule; times before this are skipped.
 * @property endAt End time of the schedule; times after this are skipped.
 * @property jitter Random jitter applied to each action (up to this duration).
 * @property timeZoneName IANA time zone name (e.g., "US/Pacific").
 */
public data class KScheduleSpec(
  val calendars: List<KScheduleCalendarSpec> = emptyList(),
  val intervals: List<KScheduleIntervalSpec> = emptyList(),
  val cronExpressions: List<String> = emptyList(),
  val skip: List<KScheduleCalendarSpec> = emptyList(),
  val startAt: Instant? = null,
  val endAt: Instant? = null,
  val jitter: Duration? = null,
  val timeZoneName: String? = null
) {
  /**
   * Converts this KScheduleSpec to a Java SDK ScheduleSpec.
   */
  internal fun toJava(): ScheduleSpec {
    val builder = ScheduleSpec.newBuilder()
    if (calendars.isNotEmpty()) builder.setCalendars(calendars.map { it.toJava() })
    if (intervals.isNotEmpty()) builder.setIntervals(intervals.map { it.toJava() })
    if (cronExpressions.isNotEmpty()) builder.setCronExpressions(cronExpressions)
    if (skip.isNotEmpty()) builder.setSkip(skip.map { it.toJava() })
    startAt?.let { builder.setStartAt(it) }
    endAt?.let { builder.setEndAt(it) }
    jitter?.let { builder.setJitter(it) }
    timeZoneName?.let { builder.setTimeZoneName(it) }
    return builder.build()
  }

  public companion object {
    /**
     * Create a KScheduleSpec from a Java SDK ScheduleSpec.
     */
    @JvmStatic
    public fun fromJava(spec: ScheduleSpec): KScheduleSpec = KScheduleSpec(
      calendars = spec.calendars?.map { KScheduleCalendarSpec.fromJava(it) } ?: emptyList(),
      intervals = spec.intervals?.map { KScheduleIntervalSpec.fromJava(it) } ?: emptyList(),
      cronExpressions = spec.cronExpressions ?: emptyList(),
      skip = spec.skip?.map { KScheduleCalendarSpec.fromJava(it) } ?: emptyList(),
      startAt = spec.startAt,
      endAt = spec.endAt,
      jitter = spec.jitter,
      timeZoneName = spec.timeZoneName
    )
  }
}
