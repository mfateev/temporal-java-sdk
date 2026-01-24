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
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.testing.internal.SDKTestWorkflowRule
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.time.Duration
import java.util.UUID

/**
 * Integration tests for Kotlin schedule APIs.
 *
 * These tests require an external Temporal server and will be skipped when
 * running with the test server.
 */
class KScheduleIntegrationTest {

  @WorkflowInterface
  interface TestScheduleWorkflow {
    @WorkflowMethod
    suspend fun execute(input: String): String
  }

  class TestScheduleWorkflowImpl : TestScheduleWorkflow {
    override suspend fun execute(input: String): String {
      return "Scheduled: $input"
    }
  }

  @Rule
  @JvmField
  val testRule = KSDKTestWorkflowRule {
    workflowTypes(TestScheduleWorkflowImpl::class)
  }

  @Before
  fun checkExternalService() {
    assumeTrue("Skipping for test server - schedules require external service", SDKTestWorkflowRule.useExternalService)
  }

  private fun createTestSchedule(): KSchedule {
    val workflowOptions = KWorkflowOptions(
      taskQueue = testRule.taskQueue,
      workflowId = "test-workflow-${UUID.randomUUID()}"
    )

    return KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestScheduleWorkflow",
        options = workflowOptions,
        arguments = listOf("test-arg")
      ),
      spec = KScheduleSpec(
        intervals = listOf(KScheduleIntervalSpec(every = Duration.ofSeconds(1)))
      )
    )
  }

  @Test
  fun `createSchedule creates and describes schedule`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = createTestSchedule()

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)
    try {
      val description = handle.describe()
      assertEquals(scheduleId, description.id)
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `scheduleHandle allows operations on existing schedule`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = createTestSchedule()

    // Create the schedule first
    val createHandle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      // Get handle separately
      val handle = testRule.kClient.scheduleHandle(scheduleId)

      // Verify we can describe it
      val description = handle.describe()
      assertEquals(scheduleId, description.id)
    } finally {
      createHandle.delete()
    }
  }

  @Test
  fun `pause and unpause schedule`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = createTestSchedule()

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      // Initial state - not paused
      var description = handle.describe()
      assertFalse(description.schedule.state!!.paused)

      // Pause
      handle.pause("Test pause")
      description = handle.describe()
      assertTrue(description.schedule.state!!.paused)
      assertEquals("Test pause", description.schedule.state!!.note)

      // Unpause
      handle.unpause("Test unpause")
      description = handle.describe()
      assertFalse(description.schedule.state!!.paused)
      assertEquals("Test unpause", description.schedule.state!!.note)
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `update schedule`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = createTestSchedule()

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      // Update the schedule
      handle.update { input ->
        val updatedSpec = input.description.schedule.spec.copy(
          intervals = listOf(KScheduleIntervalSpec(every = Duration.ofMinutes(5)))
        )
        KScheduleUpdate(
          schedule = input.description.schedule.copy(spec = updatedSpec)
        )
      }

      // Verify the update
      val description = handle.describe()
      assertEquals(Duration.ofMinutes(5), description.schedule.spec.intervals[0].every)
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `trigger schedule`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestScheduleWorkflow",
        options = KWorkflowOptions(
          taskQueue = testRule.taskQueue,
          workflowId = "test-workflow-${UUID.randomUUID()}"
        ),
        arguments = listOf("triggered")
      ),
      spec = KScheduleSpec(
        // Use a very long interval so the schedule doesn't run automatically
        intervals = listOf(KScheduleIntervalSpec(every = Duration.ofHours(24)))
      ),
      state = KScheduleState(paused = true)
    )

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      // Get initial action count
      val initialDescription = handle.describe()
      val initialActions = initialDescription.info.numActions

      // Trigger the schedule
      handle.trigger(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)

      // Wait for action to be recorded (may take a moment)
      Thread.sleep(2000)

      // Verify action count increased
      val updatedDescription = handle.describe()
      assertTrue(
        "Expected actions to increase from $initialActions",
        updatedDescription.info.numActions > initialActions
      )
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `listSchedules returns created schedules`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"
    val schedule = createTestSchedule()

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      // List schedules and find ours
      val schedules = testRule.kClient.listSchedules().toList()

      val foundSchedule = schedules.find { it.scheduleId == scheduleId }
      assertTrue("Should find created schedule in list", foundSchedule != null)
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `schedule with calendar spec`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"

    val workflowOptions = KWorkflowOptions(
      taskQueue = testRule.taskQueue,
      workflowId = "test-workflow-${UUID.randomUUID()}"
    )

    val schedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestScheduleWorkflow",
        options = workflowOptions
      ),
      spec = KScheduleSpec(
        calendars = listOf(
          KScheduleCalendarSpec(
            hour = listOf(KScheduleRange(9)),
            minutes = listOf(KScheduleRange(0)),
            dayOfWeek = listOf(KScheduleRange(1, 5)) // Monday-Friday
          )
        )
      ),
      state = KScheduleState(paused = true) // Keep paused to avoid executions
    )

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      val description = handle.describe()
      assertEquals(scheduleId, description.id)
      assertEquals(1, description.schedule.spec.calendars.size)
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `schedule with cron expression`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"

    val workflowOptions = KWorkflowOptions(
      taskQueue = testRule.taskQueue,
      workflowId = "test-workflow-${UUID.randomUUID()}"
    )

    val schedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestScheduleWorkflow",
        options = workflowOptions
      ),
      spec = KScheduleSpec(
        cronExpressions = listOf("0 9 * * MON-FRI") // 9 AM on weekdays
      ),
      state = KScheduleState(paused = true)
    )

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      val description = handle.describe()
      assertEquals(scheduleId, description.id)
      // Note: The server converts cron expressions to calendar specs internally,
      // so we verify the schedule was created and has calendar specs instead
      assertTrue(
        "Schedule should have either cron expressions or calendars",
        description.schedule.spec.cronExpressions.isNotEmpty() ||
          description.schedule.spec.calendars.isNotEmpty()
      )
    } finally {
      handle.delete()
    }
  }

  @Test
  fun `schedule with policy`() = runBlocking {
    val scheduleId = "test-schedule-${UUID.randomUUID()}"

    val workflowOptions = KWorkflowOptions(
      taskQueue = testRule.taskQueue,
      workflowId = "test-workflow-${UUID.randomUUID()}"
    )

    val schedule = KSchedule(
      action = KScheduleActionStartWorkflow(
        workflowType = "TestScheduleWorkflow",
        options = workflowOptions
      ),
      spec = KScheduleSpec(
        intervals = listOf(KScheduleIntervalSpec(Duration.ofHours(1)))
      ),
      policy = KSchedulePolicy(
        overlap = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP,
        catchupWindow = Duration.ofMinutes(10),
        pauseOnFailure = true
      ),
      state = KScheduleState(paused = true)
    )

    val handle = testRule.kClient.createSchedule(scheduleId, schedule)

    try {
      val description = handle.describe()
      assertEquals(scheduleId, description.id)
      assertEquals(
        ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP,
        description.schedule.policy!!.overlap
      )
      assertEquals(Duration.ofMinutes(10), description.schedule.policy!!.catchupWindow)
      assertTrue(description.schedule.policy!!.pauseOnFailure)
    } finally {
      handle.delete()
    }
  }
}
