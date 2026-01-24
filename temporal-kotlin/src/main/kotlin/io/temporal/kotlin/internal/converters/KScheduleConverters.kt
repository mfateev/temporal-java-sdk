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

package io.temporal.kotlin.internal.converters

import io.temporal.client.schedules.Schedule
import io.temporal.client.schedules.ScheduleAction
import io.temporal.client.schedules.ScheduleActionStartWorkflow
import io.temporal.client.schedules.ScheduleBackfill
import io.temporal.client.schedules.ScheduleCalendarSpec
import io.temporal.client.schedules.ScheduleIntervalSpec
import io.temporal.client.schedules.ScheduleOptions
import io.temporal.client.schedules.SchedulePolicy
import io.temporal.client.schedules.ScheduleRange
import io.temporal.client.schedules.ScheduleSpec
import io.temporal.client.schedules.ScheduleState
import io.temporal.client.schedules.ScheduleUpdate
import io.temporal.kotlin.client.schedules.KSchedule
import io.temporal.kotlin.client.schedules.KScheduleAction
import io.temporal.kotlin.client.schedules.KScheduleActionStartWorkflow
import io.temporal.kotlin.client.schedules.KScheduleBackfill
import io.temporal.kotlin.client.schedules.KScheduleCalendarSpec
import io.temporal.kotlin.client.schedules.KScheduleIntervalSpec
import io.temporal.kotlin.client.schedules.KScheduleOptions
import io.temporal.kotlin.client.schedules.KSchedulePolicy
import io.temporal.kotlin.client.schedules.KScheduleRange
import io.temporal.kotlin.client.schedules.KScheduleSpec
import io.temporal.kotlin.client.schedules.KScheduleState
import io.temporal.kotlin.client.schedules.KScheduleUpdate
import io.temporal.kotlin.internal.InternalTemporalApi

/**
 * Internal converter for Kotlin schedule types to Java SDK types.
 *
 * This object keeps Java SDK types out of the public API by centralizing
 * all conversion logic here.
 */
@InternalTemporalApi
public object KScheduleConverters {

  /**
   * Converts [KScheduleRange] to [ScheduleRange].
   */
  fun toJava(range: KScheduleRange): ScheduleRange =
    ScheduleRange(range.start, range.end, range.step)

  /**
   * Converts [KScheduleBackfill] to [ScheduleBackfill].
   */
  fun toJava(backfill: KScheduleBackfill): ScheduleBackfill =
    ScheduleBackfill(backfill.startAt, backfill.endAt, backfill.overlapPolicy)

  /**
   * Converts [KScheduleIntervalSpec] to [ScheduleIntervalSpec].
   */
  fun toJava(spec: KScheduleIntervalSpec): ScheduleIntervalSpec =
    ScheduleIntervalSpec(spec.every, spec.offset)

  /**
   * Converts [KScheduleCalendarSpec] to [ScheduleCalendarSpec].
   */
  fun toJava(spec: KScheduleCalendarSpec): ScheduleCalendarSpec =
    ScheduleCalendarSpec.newBuilder()
      .setSeconds(spec.seconds.map { toJava(it) })
      .setMinutes(spec.minutes.map { toJava(it) })
      .setHour(spec.hour.map { toJava(it) })
      .setDayOfMonth(spec.dayOfMonth.map { toJava(it) })
      .setMonth(spec.month.map { toJava(it) })
      .setYear(spec.year.map { toJava(it) })
      .setDayOfWeek(spec.dayOfWeek.map { toJava(it) })
      .setComment(spec.comment)
      .build()

  /**
   * Converts [KSchedulePolicy] to [SchedulePolicy].
   */
  fun toJava(policy: KSchedulePolicy): SchedulePolicy {
    val builder = SchedulePolicy.newBuilder()
      .setOverlap(policy.overlap)
      .setPauseOnFailure(policy.pauseOnFailure)
    policy.catchupWindow?.let { builder.setCatchupWindow(it) }
    return builder.build()
  }

  /**
   * Converts [KScheduleState] to [ScheduleState].
   */
  fun toJava(state: KScheduleState): ScheduleState {
    val builder = ScheduleState.newBuilder()
      .setPaused(state.paused)
      .setLimitedAction(state.limitedActions)
      .setRemainingActions(state.remainingActions)
    state.note?.let { builder.setNote(it) }
    return builder.build()
  }

  /**
   * Converts [KScheduleSpec] to [ScheduleSpec].
   */
  fun toJava(spec: KScheduleSpec): ScheduleSpec {
    val builder = ScheduleSpec.newBuilder()
    if (spec.calendars.isNotEmpty()) builder.setCalendars(spec.calendars.map { toJava(it) })
    if (spec.intervals.isNotEmpty()) builder.setIntervals(spec.intervals.map { toJava(it) })
    if (spec.cronExpressions.isNotEmpty()) builder.setCronExpressions(spec.cronExpressions)
    if (spec.skip.isNotEmpty()) builder.setSkip(spec.skip.map { toJava(it) })
    spec.startAt?.let { builder.setStartAt(it) }
    spec.endAt?.let { builder.setEndAt(it) }
    spec.jitter?.let { builder.setJitter(it) }
    spec.timeZoneName?.let { builder.setTimeZoneName(it) }
    return builder.build()
  }

  /**
   * Converts [KScheduleOptions] to [ScheduleOptions].
   */
  fun toJava(options: KScheduleOptions): ScheduleOptions {
    val builder = ScheduleOptions.newBuilder()
      .setTriggerImmediately(options.triggerImmediately)
    if (options.backfills.isNotEmpty()) {
      builder.setBackfills(options.backfills.map { toJava(it) })
    }
    options.memo?.let { builder.setMemo(it) }
    options.searchAttributes?.let { builder.setTypedSearchAttributes(it) }
    return builder.build()
  }

  /**
   * Converts [KScheduleAction] to [ScheduleAction].
   */
  fun toJava(action: KScheduleAction): ScheduleAction = when (action) {
    is KScheduleActionStartWorkflow -> toJava(action)
  }

  /**
   * Converts [KScheduleActionStartWorkflow] to [ScheduleActionStartWorkflow].
   */
  fun toJava(action: KScheduleActionStartWorkflow): ScheduleAction {
    val builder = ScheduleActionStartWorkflow.newBuilder()
      .setWorkflowType(action.workflowType)
      .setOptions(KOptionsConverters.toJava(action.options))
    if (action.arguments.isNotEmpty()) {
      builder.setArguments(*action.arguments.toTypedArray())
    }
    return builder.build()
  }

  /**
   * Converts [KSchedule] to [Schedule].
   */
  fun toJava(schedule: KSchedule): Schedule {
    val builder = Schedule.newBuilder()
      .setAction(toJava(schedule.action))
      .setSpec(toJava(schedule.spec))
    schedule.policy?.let { builder.setPolicy(toJava(it)) }
    schedule.state?.let { builder.setState(toJava(it)) }
    return builder.build()
  }

  /**
   * Converts [KScheduleUpdate] to [ScheduleUpdate].
   */
  fun toJava(update: KScheduleUpdate): ScheduleUpdate =
    if (update.searchAttributes != null) {
      ScheduleUpdate(toJava(update.schedule), update.searchAttributes)
    } else {
      ScheduleUpdate(toJava(update.schedule))
    }
}
