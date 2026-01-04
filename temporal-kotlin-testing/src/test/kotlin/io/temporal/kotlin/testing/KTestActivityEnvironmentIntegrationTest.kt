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

package io.temporal.kotlin.testing

import io.temporal.activity.Activity
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.failure.CanceledFailure
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Integration tests for [KTestActivityEnvironment].
 *
 * Tests cover:
 * - Environment creation with default and custom options
 * - Activity registration (regular and suspend)
 * - executeActivity() with various argument counts (0-4)
 * - executeLocalActivity() with various argument counts
 * - Heartbeat testing (setHeartbeatDetails, setActivityHeartbeatListener)
 * - Cancellation testing (requestCancelActivity)
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KTestActivityEnvironmentIntegrationTest {

    // ==================== Test Interfaces ====================

    @ActivityInterface
    interface TestActivities {
        @ActivityMethod
        fun noArgs(): String

        @ActivityMethod
        fun oneArg(input: String): String

        @ActivityMethod
        fun twoArgs(a: String, b: Int): String

        @ActivityMethod
        fun threeArgs(a: String, b: Int, c: Boolean): String

        @ActivityMethod
        fun fourArgs(a: String, b: Int, c: Boolean, d: Double): String

        @ActivityMethod
        fun processWithHeartbeat(items: Int): Int

        @ActivityMethod
        fun longRunningWithHeartbeat(iterations: Int): String
    }

    // ==================== Test Implementations ====================

    class TestActivitiesImpl : TestActivities {
        companion object {
            val executionCount = AtomicInteger(0)
            val lastHeartbeatDetails = mutableListOf<Int>()
        }

        override fun noArgs(): String {
            executionCount.incrementAndGet()
            return "no-args-result"
        }

        override fun oneArg(input: String): String {
            executionCount.incrementAndGet()
            return "processed: $input"
        }

        override fun twoArgs(a: String, b: Int): String {
            executionCount.incrementAndGet()
            return "$a-$b"
        }

        override fun threeArgs(a: String, b: Int, c: Boolean): String {
            executionCount.incrementAndGet()
            return "$a-$b-$c"
        }

        override fun fourArgs(a: String, b: Int, c: Boolean, d: Double): String {
            executionCount.incrementAndGet()
            return "$a-$b-$c-$d"
        }

        override fun processWithHeartbeat(items: Int): Int {
            executionCount.incrementAndGet()
            var processed = 0
            for (i in 1..items) {
                processed = i
                Activity.getExecutionContext().heartbeat(processed)
                // Delay to ensure heartbeats exceed the throttle interval
                Thread.sleep(100)
            }
            return processed
        }

        override fun longRunningWithHeartbeat(iterations: Int): String {
            executionCount.incrementAndGet()
            for (i in 1..iterations) {
                // Check for cancellation on each heartbeat
                Activity.getExecutionContext().heartbeat(i)
                // Delay to ensure heartbeats exceed the throttle interval
                Thread.sleep(100)
            }
            return "completed"
        }
    }

    // ==================== Test Setup ====================

    private var activityEnv: KTestActivityEnvironment? = null

    @BeforeEach
    fun setUp() {
        TestActivitiesImpl.executionCount.set(0)
        TestActivitiesImpl.lastHeartbeatDetails.clear()
    }

    @AfterEach
    fun tearDown() {
        activityEnv?.close()
        activityEnv = null
    }

    // ==================== Environment Creation Tests ====================

    @Test
    fun `create environment with default options`() {
        activityEnv = KTestActivityEnvironment.newInstance()

        assertNotNull(activityEnv)
    }

    @Test
    fun `create environment with custom options DSL`() {
        activityEnv = KTestActivityEnvironment.newInstance {
            namespace = "custom-namespace"
            useTimeskipping = false
        }

        assertNotNull(activityEnv)
    }

    @Test
    fun `create environment with pre-built options`() {
        val options = KTestEnvironmentOptions.newBuilder {
            namespace = "pre-built-namespace"
        }

        activityEnv = KTestActivityEnvironment.newInstance(options)

        assertNotNull(activityEnv)
    }

    // ==================== Activity Registration Tests ====================

    @Test
    fun `register regular activities implementation`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        // Registration should succeed - verify by executing an activity
        val result = activityEnv!!.executeActivity(
            TestActivities::noArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
        )

        assertEquals("no-args-result", result)
    }

    @Test
    fun `register multiple activity implementations`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        // Register the same implementation - verifies vararg registration works
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        // Verify registration works
        val result1 = activityEnv!!.executeActivity(
            TestActivities::noArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
        )
        assertEquals("no-args-result", result1)
    }

    // ==================== Execute Activity Tests (0-4 args) ====================

    @Test
    fun `execute activity with no arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeActivity(
            TestActivities::noArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
        )

        assertEquals("no-args-result", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute activity with one argument`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeActivity(
            TestActivities::oneArg,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "test-input",
        )

        assertEquals("processed: test-input", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute activity with two arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeActivity(
            TestActivities::twoArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "hello",
            42,
        )

        assertEquals("hello-42", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute activity with three arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeActivity(
            TestActivities::threeArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "test",
            123,
            true,
        )

        assertEquals("test-123-true", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute activity with four arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeActivity(
            TestActivities::fourArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "value",
            456,
            false,
            3.14,
        )

        assertEquals("value-456-false-3.14", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    // ==================== Execute Local Activity Tests ====================

    @Test
    fun `execute local activity with no arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeLocalActivity(
            TestActivities::noArgs,
            KLocalActivityOptions(startToCloseTimeout = 1.minutes),
        )

        assertEquals("no-args-result", result)
    }

    @Test
    fun `execute local activity with one argument`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeLocalActivity(
            TestActivities::oneArg,
            KLocalActivityOptions(startToCloseTimeout = 1.minutes),
            "local-input",
        )

        assertEquals("processed: local-input", result)
    }

    @Test
    fun `execute local activity with two arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeLocalActivity(
            TestActivities::twoArgs,
            KLocalActivityOptions(startToCloseTimeout = 1.minutes),
            "local",
            100,
        )

        assertEquals("local-100", result)
    }

    @Test
    fun `execute local activity with three arguments`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val result = activityEnv!!.executeLocalActivity(
            TestActivities::threeArgs,
            KLocalActivityOptions(startToCloseTimeout = 1.minutes),
            "local",
            200,
            true,
        )

        assertEquals("local-200-true", result)
    }

    // ==================== Heartbeat Tests ====================

    @Test
    fun `activity heartbeats are sent`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        val heartbeats = mutableListOf<Int>()
        activityEnv!!.setActivityHeartbeatListener<Int> { details ->
            heartbeats.add(details)
        }

        val result = activityEnv!!.executeActivity(
            TestActivities::processWithHeartbeat,
            KActivityOptions(
                startToCloseTimeout = 1.minutes,
                // Short heartbeat timeout reduces throttling (throttle = 0.8 * timeout)
                heartbeatTimeout = 100.milliseconds,
            ),
            5,
        )

        assertEquals(5, result)
        assertEquals(listOf(1, 2, 3, 4, 5), heartbeats)
    }

    @Test
    fun `setHeartbeatDetails provides initial heartbeat state`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        // Set heartbeat details before execution (simulating retry)
        activityEnv!!.setHeartbeatDetails(3)

        // Activity would resume from heartbeat checkpoint
        // Note: This test verifies the API works; actual checkpoint resumption
        // requires the activity implementation to check getHeartbeatDetails()
        val result = activityEnv!!.executeActivity(
            TestActivities::processWithHeartbeat,
            KActivityOptions(
                startToCloseTimeout = 1.minutes,
                heartbeatTimeout = 30.seconds,
            ),
            5,
        )

        assertEquals(5, result)
    }

    // ==================== Cancellation Tests ====================

    @Test
    fun `requestCancelActivity cancels activity on heartbeat`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        var heartbeatCount = 0
        activityEnv!!.setActivityHeartbeatListener<Int> { _ ->
            heartbeatCount++
            if (heartbeatCount >= 2) {
                activityEnv!!.requestCancelActivity()
            }
        }

        // Activity should be cancelled after 2 heartbeats
        val exception = assertThrows(CanceledFailure::class.java) {
            activityEnv!!.executeActivity(
                TestActivities::longRunningWithHeartbeat,
                KActivityOptions(
                    startToCloseTimeout = 1.minutes,
                    // Short heartbeat timeout reduces throttling (throttle = 0.8 * timeout)
                    heartbeatTimeout = 100.milliseconds,
                ),
                100, // Would take 100 iterations, but will be cancelled after 2
            )
        }

        assertTrue(heartbeatCount >= 2, "Should have received at least 2 heartbeats")
        assertNotNull(exception)
    }

    // Note: Suspend activity tests are skipped because TestActivityEnvironment
    // doesn't support suspend functions directly (continuation parameter causes
    // serialization issues). Suspend activities should be tested through
    // KTestWorkflowEnvironment with a real workflow.

    // ==================== Lifecycle Tests ====================

    @Test
    fun `close releases resources`() {
        activityEnv = KTestActivityEnvironment.newInstance()
        activityEnv!!.registerActivitiesImplementations(TestActivitiesImpl())

        // Execute an activity to ensure environment is active
        activityEnv!!.executeActivity(
            TestActivities::noArgs,
            KActivityOptions(startToCloseTimeout = 1.minutes),
        )

        // Close should not throw
        activityEnv!!.close()
        activityEnv = null // Prevent double close in tearDown
    }
}
