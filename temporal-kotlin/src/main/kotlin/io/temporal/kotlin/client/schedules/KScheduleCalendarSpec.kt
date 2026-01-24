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

/**
 * Specification of when to run an action in relation to calendar time.
 *
 * A timestamp matches if at least one range of each field matches except for year.
 * If year is missing, that means all years match. For all fields besides year,
 * at least one range must be present to match anything.
 *
 * Example:
 * ```kotlin
 * // Run at noon every day
 * KScheduleCalendarSpec(
 *     hour = listOf(KScheduleRange(12))
 * )
 *
 * // Run at 9am on weekdays (Monday=1 through Friday=5)
 * KScheduleCalendarSpec(
 *     hour = listOf(KScheduleRange(9)),
 *     dayOfWeek = listOf(KScheduleRange(1, 5))
 * )
 *
 * // Run at midnight on the 1st and 15th of every month
 * KScheduleCalendarSpec(
 *     dayOfMonth = listOf(KScheduleRange(1), KScheduleRange(15))
 * )
 * ```
 *
 * @property seconds Second ranges (0-59). Default matches 0.
 * @property minutes Minute ranges (0-59). Default matches 0.
 * @property hour Hour ranges (0-23). Default matches 0.
 * @property dayOfMonth Day of month ranges (1-31). Default matches all days.
 * @property month Month ranges (1-12). Default matches all months.
 * @property year Year ranges. Default (empty) matches all years.
 * @property dayOfWeek Day of week ranges (0-6, 0 is Sunday). Default matches all days.
 * @property comment Description of this specification.
 */
public data class KScheduleCalendarSpec(
  val seconds: List<KScheduleRange> = BEGINNING,
  val minutes: List<KScheduleRange> = BEGINNING,
  val hour: List<KScheduleRange> = BEGINNING,
  val dayOfMonth: List<KScheduleRange> = ALL_MONTH_DAYS,
  val month: List<KScheduleRange> = ALL_MONTHS,
  val year: List<KScheduleRange> = emptyList(),
  val dayOfWeek: List<KScheduleRange> = ALL_WEEK_DAYS,
  val comment: String = ""
) {
  public companion object {
    /** Default range set for zero. */
    public val BEGINNING: List<KScheduleRange> = listOf(KScheduleRange(0))

    /** Default range set for all days in a month (1-31). */
    public val ALL_MONTH_DAYS: List<KScheduleRange> = listOf(KScheduleRange(1, 31))

    /** Default range set for all months in a year (1-12). */
    public val ALL_MONTHS: List<KScheduleRange> = listOf(KScheduleRange(1, 12))

    /** Default range set for all days in a week (0-6). */
    public val ALL_WEEK_DAYS: List<KScheduleRange> = listOf(KScheduleRange(0, 6))
  }
}
