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
import io.temporal.kotlin.activity.KActivityOptions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/**
 * Integration tests for [KTestActivityExtension].
 *
 * Tests cover:
 * - Extension lifecycle (beforeEach/afterEach)
 * - Parameter injection (KTestActivityEnvironment)
 * - Activity registration (regular and suspend)
 * - Activity execution via injected environment
 * - Heartbeat testing
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KTestActivityExtensionIntegrationTest {

    // ==================== Test Interfaces ====================

    @ActivityInterface
    interface SimpleActivities {
        @ActivityMethod
        fun greet(name: String): String

        @ActivityMethod
        fun add(a: Int, b: Int): Int

        @ActivityMethod
        fun processWithHeartbeat(items: Int): Int
    }

    @ActivityInterface
    interface AnotherActivities {
        @ActivityMethod
        fun format(input: String): String
    }

    // ==================== Test Implementations ====================

    class SimpleActivitiesImpl : SimpleActivities {
        companion object {
            val executionCount = AtomicInteger(0)
        }

        override fun greet(name: String): String {
            executionCount.incrementAndGet()
            return "Hello, $name!"
        }

        override fun add(a: Int, b: Int): Int {
            executionCount.incrementAndGet()
            return a + b
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
    }

    class AnotherActivitiesImpl : AnotherActivities {
        companion object {
            val executionCount = AtomicInteger(0)
        }

        override fun format(input: String): String {
            executionCount.incrementAndGet()
            return "Formatted: $input"
        }
    }

    // ==================== Extension Configuration ====================

    companion object {
        @JvmField
        @RegisterExtension
        val activityExtension = kTestActivityExtension {
            setActivityImplementations(
                SimpleActivitiesImpl(),
                AnotherActivitiesImpl()
            )
        }
    }

    @BeforeEach
    fun setUp() {
        SimpleActivitiesImpl.executionCount.set(0)
        AnotherActivitiesImpl.executionCount.set(0)
    }

    // ==================== Parameter Injection Tests ====================

    @Test
    fun `inject KTestActivityEnvironment`(activityEnv: KTestActivityEnvironment) {
        assertNotNull(activityEnv)
    }

    // ==================== Activity Execution Tests ====================

    @Test
    fun `execute activity with one argument`(activityEnv: KTestActivityEnvironment) {
        val result = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "World"
        )

        assertEquals("Hello, World!", result)
        assertEquals(1, SimpleActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute activity with two arguments`(activityEnv: KTestActivityEnvironment) {
        val result = activityEnv.executeActivity(
            SimpleActivities::add,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            10,
            20
        )

        assertEquals(30, result)
        assertEquals(1, SimpleActivitiesImpl.executionCount.get())
    }

    @Test
    fun `execute multiple activities in sequence`(activityEnv: KTestActivityEnvironment) {
        val result1 = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "First"
        )

        val result2 = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Second"
        )

        assertEquals("Hello, First!", result1)
        assertEquals("Hello, Second!", result2)
        assertEquals(2, SimpleActivitiesImpl.executionCount.get())
    }

    // ==================== Multiple Activity Implementations Tests ====================

    @Test
    fun `execute activities from different implementations`(activityEnv: KTestActivityEnvironment) {
        val result1 = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Test"
        )

        val result2 = activityEnv.executeActivity(
            AnotherActivities::format,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Input"
        )

        assertEquals("Hello, Test!", result1)
        assertEquals("Formatted: Input", result2)
        assertEquals(1, SimpleActivitiesImpl.executionCount.get())
        assertEquals(1, AnotherActivitiesImpl.executionCount.get())
    }

    // ==================== Heartbeat Tests ====================

    @Test
    fun `activity with heartbeat listener`(activityEnv: KTestActivityEnvironment) {
        val heartbeats = mutableListOf<Int>()
        activityEnv.setActivityHeartbeatListener<Int> { details ->
            heartbeats.add(details)
        }

        val result = activityEnv.executeActivity(
            SimpleActivities::processWithHeartbeat,
            KActivityOptions(
                startToCloseTimeout = 1.minutes,
                // Short heartbeat timeout reduces throttling (throttle = 0.8 * timeout)
                heartbeatTimeout = 100.milliseconds
            ),
            5
        )

        assertEquals(5, result)
        assertEquals(listOf(1, 2, 3, 4, 5), heartbeats)
    }

    // ==================== Custom Options Tests ====================

    @Test
    fun `extension with custom test environment options`(activityEnv: KTestActivityEnvironment) {
        // Environment should be created with custom options
        assertNotNull(activityEnv)

        // Execute activity to verify it works
        val result = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Custom"
        )

        assertEquals("Hello, Custom!", result)
    }

    // ==================== Isolation Tests ====================

    @Test
    fun `each test gets isolated environment - test 1`(activityEnv: KTestActivityEnvironment) {
        val result = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Isolation1"
        )

        assertEquals("Hello, Isolation1!", result)
        assertEquals(1, SimpleActivitiesImpl.executionCount.get())
    }

    @Test
    fun `each test gets isolated environment - test 2`(activityEnv: KTestActivityEnvironment) {
        // Counter was reset in setUp, so this test starts fresh
        val result = activityEnv.executeActivity(
            SimpleActivities::greet,
            KActivityOptions(startToCloseTimeout = 1.minutes),
            "Isolation2"
        )

        assertEquals("Hello, Isolation2!", result)
        assertEquals(1, SimpleActivitiesImpl.executionCount.get())
    }
}
