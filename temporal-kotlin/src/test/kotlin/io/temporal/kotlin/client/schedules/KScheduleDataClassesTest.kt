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
import io.temporal.client.WorkflowOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.converters.KScheduleConverters
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Duration
import java.time.Instant

/**
 * Unit tests for Kotlin schedule data classes.
 * Tests conversion between Kotlin and Java SDK types.
 */
@OptIn(InternalTemporalApi::class)
class KScheduleDataClassesTest {

  @Test
  fun `KScheduleRange converts to and from Java correctly`() {
    val kotlinRange = KScheduleRange(start = 1, end = 10, step = 2)
    val javaRange = kotlinRange.let { KScheduleConverters.toJava(it) }

    assertEquals(1, javaRange.start)
    assertEquals(10, javaRange.end)
    assertEquals(2, javaRange.step)

    val backToKotlin = KScheduleRange.fromJava(javaRange)
    assertEquals(kotlinRange, backToKotlin)
  }

  @Test
  fun `KScheduleRange with defaults converts correctly`() {
    val kotlinRange = KScheduleRange(start = 5)
    val javaRange = kotlinRange.let { KScheduleConverters.toJava(it) }

    assertEquals(5, javaRange.start)
    assertEquals(0, javaRange.end)
    assertEquals(0, javaRange.step)
  }

  @Test
  fun `KScheduleCalendarSpec converts to and from Java correctly`() {
    val kotlinSpec = KScheduleCalendarSpec(
      seconds = listOf(KScheduleRange(0, 59, 10)),
      minutes = listOf(KScheduleRange(0)),
      hour = listOf(KScheduleRange(9, 17)),
      dayOfMonth = listOf(KScheduleRange(1, 15)),
      month = listOf(KScheduleRange(1, 12)),
      year = listOf(KScheduleRange(2024, 2025)),
      dayOfWeek = listOf(KScheduleRange(1, 5)),
      comment = "Test calendar spec"
    )

    val javaSpec = kotlinSpec.let { KScheduleConverters.toJava(it) }
    assertEquals("Test calendar spec", javaSpec.comment)
    assertEquals(1, javaSpec.seconds.size)
    assertEquals(0, javaSpec.seconds[0].start)
    assertEquals(59, javaSpec.seconds[0].end)
    assertEquals(10, javaSpec.seconds[0].step)

    val backToKotlin = KScheduleCalendarSpec.fromJava(javaSpec)
    assertEquals(kotlinSpec, backToKotlin)
  }

  @Test
  fun `KScheduleIntervalSpec converts to and from Java correctly`() {
    val kotlinSpec = KScheduleIntervalSpec(
      every = Duration.ofHours(1),
      offset = Duration.ofMinutes(5)
    )

    val javaSpec = kotlinSpec.let { KScheduleConverters.toJava(it) }
    assertEquals(Duration.ofHours(1), javaSpec.every)
    assertEquals(Duration.ofMinutes(5), javaSpec.offset)

    val backToKotlin = KScheduleIntervalSpec.fromJava(javaSpec)
    assertEquals(kotlinSpec, backToKotlin)
  }

  @Test
  fun `KScheduleIntervalSpec with null offset converts correctly`() {
    val kotlinSpec = KScheduleIntervalSpec(every = Duration.ofMinutes(30))
    val javaSpec = kotlinSpec.let { KScheduleConverters.toJava(it) }

    assertEquals(Duration.ofMinutes(30), javaSpec.every)
    assertEquals(Duration.ZERO, javaSpec.offset)
  }

  @Test
  fun `KScheduleSpec converts to and from Java correctly`() {
    val kotlinSpec = KScheduleSpec(
      calendars = listOf(
        KScheduleCalendarSpec(
          hour = listOf(KScheduleRange(9)),
          minutes = listOf(KScheduleRange(0))
        )
      ),
      intervals = listOf(
        KScheduleIntervalSpec(Duration.ofHours(2))
      ),
      cronExpressions = listOf("0 12 * * MON"),
      startAt = Instant.parse("2024-01-01T00:00:00Z"),
      endAt = Instant.parse("2024-12-31T23:59:59Z"),
      jitter = Duration.ofMinutes(5),
      timeZoneName = "America/New_York"
    )

    val javaSpec = kotlinSpec.let { KScheduleConverters.toJava(it) }
    assertEquals(1, javaSpec.calendars.size)
    assertEquals(1, javaSpec.intervals.size)
    assertEquals(Duration.ofHours(2), javaSpec.intervals[0].every)
    assertEquals(listOf("0 12 * * MON"), javaSpec.cronExpressions)
    assertEquals(Instant.parse("2024-01-01T00:00:00Z"), javaSpec.startAt)
    assertEquals(Instant.parse("2024-12-31T23:59:59Z"), javaSpec.endAt)
    assertEquals(Duration.ofMinutes(5), javaSpec.jitter)
    assertEquals("America/New_York", javaSpec.timeZoneName)

    val backToKotlin = KScheduleSpec.fromJava(javaSpec)
    assertEquals(kotlinSpec.calendars.size, backToKotlin.calendars.size)
    assertEquals(kotlinSpec.intervals.size, backToKotlin.intervals.size)
    assertEquals(kotlinSpec.cronExpressions, backToKotlin.cronExpressions)
    assertEquals(kotlinSpec.startAt, backToKotlin.startAt)
    assertEquals(kotlinSpec.endAt, backToKotlin.endAt)
    assertEquals(kotlinSpec.jitter, backToKotlin.jitter)
    assertEquals(kotlinSpec.timeZoneName, backToKotlin.timeZoneName)
  }

  @Test
  fun `KScheduleState converts to and from Java correctly`() {
    val kotlinState = KScheduleState(
      note = "Test note",
      paused = true,
      limitedActions = true,
      remainingActions = 5
    )

    val javaState = kotlinState.let { KScheduleConverters.toJava(it) }
    assertEquals("Test note", javaState.note)
    assertTrue(javaState.isPaused)
    assertTrue(javaState.isLimitedAction)
    assertEquals(5, javaState.remainingActions)

    val backToKotlin = KScheduleState.fromJava(javaState)
    assertEquals(kotlinState, backToKotlin)
  }

  @Test
  fun `KScheduleState with defaults converts correctly`() {
    val kotlinState = KScheduleState()
    val javaState = kotlinState.let { KScheduleConverters.toJava(it) }

    assertNull(javaState.note)
    assertFalse(javaState.isPaused)
    assertFalse(javaState.isLimitedAction)
    assertEquals(0, javaState.remainingActions)
  }

  @Test
  fun `KSchedulePolicy converts to and from Java correctly`() {
    val kotlinPolicy = KSchedulePolicy(
      overlap = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
      catchupWindow = Duration.ofMinutes(10),
      pauseOnFailure = true
    )

    val javaPolicy = kotlinPolicy.let { KScheduleConverters.toJava(it) }
    assertEquals(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, javaPolicy.overlap)
    assertEquals(Duration.ofMinutes(10), javaPolicy.catchupWindow)
    assertTrue(javaPolicy.isPauseOnFailure)

    val backToKotlin = KSchedulePolicy.fromJava(javaPolicy)
    assertEquals(kotlinPolicy, backToKotlin)
  }

  @Test
  fun `KScheduleBackfill converts to Java correctly`() {
    val start = Instant.parse("2024-01-01T00:00:00Z")
    val end = Instant.parse("2024-01-02T00:00:00Z")
    val kotlinBackfill = KScheduleBackfill(
      startAt = start,
      endAt = end,
      overlapPolicy = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
    )

    val javaBackfill = kotlinBackfill.let { KScheduleConverters.toJava(it) }
    assertEquals(start, javaBackfill.startAt)
    assertEquals(end, javaBackfill.endAt)
    assertEquals(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, javaBackfill.overlapPolicy)
  }

  @Test
  fun `KScheduleOptions converts to Java correctly`() {
    val kotlinOptions = KScheduleOptions(
      triggerImmediately = true,
      backfills = listOf(
        KScheduleBackfill(
          startAt = Instant.parse("2024-01-01T00:00:00Z"),
          endAt = Instant.parse("2024-01-02T00:00:00Z")
        )
      ),
      memo = mapOf("key" to "value")
    )

    val javaOptions = kotlinOptions.let { KScheduleConverters.toJava(it) }
    assertTrue(javaOptions.isTriggerImmediately)
    assertEquals(1, javaOptions.backfills.size)
  }

  @Test
  fun `KScheduleActionStartWorkflow converts to and from Java correctly`() {
    val workflowOptions = WorkflowOptions.newBuilder()
      .setTaskQueue("test-queue")
      .setWorkflowId("test-workflow-id")
      .build()

    val kotlinAction = KScheduleActionStartWorkflow(
      workflowType = "TestWorkflow",
      options = workflowOptions,
      arguments = listOf("arg1", 42)
    )

    val javaAction = kotlinAction.let { KScheduleConverters.toJava(it) } as io.temporal.client.schedules.ScheduleActionStartWorkflow
    assertEquals("TestWorkflow", javaAction.workflowType)
    assertEquals("test-queue", javaAction.options.taskQueue)

    val backToKotlin = KScheduleActionStartWorkflow.fromJava(javaAction)
    assertEquals("TestWorkflow", backToKotlin.workflowType)
    assertEquals("test-queue", backToKotlin.options.taskQueue)
  }

  @Test
  fun `KSchedule converts to and from Java correctly`() {
    val workflowOptions = WorkflowOptions.newBuilder()
      .setTaskQueue("test-queue")
      .setWorkflowId("test-workflow-id")
      .build()

    val kotlinSchedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestWorkflow",
        options = workflowOptions
      ),
      spec = KScheduleSpec(
        intervals = listOf(KScheduleIntervalSpec(Duration.ofHours(1)))
      ),
      policy = KSchedulePolicy(
        overlap = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP
      ),
      state = KScheduleState(
        paused = true,
        note = "Test schedule"
      )
    )

    val javaSchedule = kotlinSchedule.let { KScheduleConverters.toJava(it) }
    assertEquals("TestWorkflow", (javaSchedule.action as io.temporal.client.schedules.ScheduleActionStartWorkflow).workflowType)
    assertEquals(1, javaSchedule.spec.intervals.size)
    assertEquals(Duration.ofHours(1), javaSchedule.spec.intervals[0].every)
    assertEquals(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP, javaSchedule.policy!!.overlap)
    assertTrue(javaSchedule.state!!.isPaused)
    assertEquals("Test schedule", javaSchedule.state!!.note)

    val backToKotlin = KSchedule.fromJava(javaSchedule)
    assertTrue(backToKotlin.action is KScheduleActionStartWorkflow)
    assertEquals("TestWorkflow", (backToKotlin.action as KScheduleActionStartWorkflow).workflowType)
    assertEquals(1, backToKotlin.spec.intervals.size)
    assertEquals(Duration.ofHours(1), backToKotlin.spec.intervals[0].every)
  }

  @Test
  fun `KScheduleListState converts from Java correctly`() {
    val javaState = io.temporal.client.schedules.ScheduleListState("Test note", true)
    val kotlinState = KScheduleListState.fromJava(javaState)

    assertEquals("Test note", kotlinState.note)
    assertTrue(kotlinState.paused)
  }

  @Test
  fun `KScheduleUpdate converts to Java correctly`() {
    val workflowOptions = WorkflowOptions.newBuilder()
      .setTaskQueue("test-queue")
      .setWorkflowId("test-workflow-id")
      .build()

    val kotlinSchedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestWorkflow",
        options = workflowOptions
      ),
      spec = KScheduleSpec(
        intervals = listOf(KScheduleIntervalSpec(Duration.ofHours(2)))
      )
    )

    val kotlinUpdate = KScheduleUpdate(schedule = kotlinSchedule)
    val javaUpdate = kotlinUpdate.let { KScheduleConverters.toJava(it) }

    assertEquals("TestWorkflow", (javaUpdate.schedule.action as io.temporal.client.schedules.ScheduleActionStartWorkflow).workflowType)
    assertEquals(Duration.ofHours(2), javaUpdate.schedule.spec.intervals[0].every)
  }
}
