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

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.common.kargs
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.Mockito
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes

/**
 * Integration tests for activity mocking support in [KTestWorkflowExtension].
 *
 * Tests cover:
 * - Mocking regular activities with Mockito
 * - Verifying mock invocations
 * - Error handling for unregistered activity types
 * - Exception propagation from mocks
 *
 * All tests use suspend workflows with [KWorkflow.executeActivity] and the method reference pattern.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KActivityMockingIntegrationTest {

    // ==================== Test Interfaces ====================

    @WorkflowInterface
    interface WorkflowWithActivity {
        @WorkflowMethod
        suspend fun process(input: String): String
    }

    @WorkflowInterface
    interface WorkflowWithMultipleActivities {
        @WorkflowMethod
        suspend fun processMultiple(input: String): String
    }

    @ActivityInterface
    interface GreetingActivity {
        @ActivityMethod
        fun formatGreeting(name: String): String
    }

    @ActivityInterface
    interface CounterActivity {
        @ActivityMethod
        fun increment(value: Int): Int
    }

    // ==================== Test Workflow Implementations ====================

    class WorkflowWithActivityImpl : WorkflowWithActivity {
        override suspend fun process(input: String): String {
            return KWorkflow.executeActivity(
                GreetingActivity::formatGreeting,
                input,
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )
        }
    }

    class WorkflowWithMultipleActivitiesImpl : WorkflowWithMultipleActivities {
        override suspend fun processMultiple(input: String): String {
            val greeting = KWorkflow.executeActivity(
                GreetingActivity::formatGreeting,
                input,
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )
            val count = KWorkflow.executeActivity(
                CounterActivity::increment,
                1,
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )
            return "$greeting (count: $count)"
        }
    }

    // ==================== Tests with Activity Mocking ====================

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(
                WorkflowWithActivityImpl::class,
                WorkflowWithMultipleActivitiesImpl::class,
            )
            // Note: No activity implementations are registered here
            // They will be mocked via testEnv.registerActivitiesImplementations()
        }
    }

    @Test
    fun `mock activity returns expected value`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Create mock using Mockito
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting(Mockito.anyString()))
            .thenReturn("Mocked: Hello!")

        // Register mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // Execute workflow
        val result = client.executeWorkflow(
            WorkflowWithActivity::process,
            "World",
            options.copy(workflowId = "test-${UUID.randomUUID()}"),
        )

        // Verify result
        assertEquals("Mocked: Hello!", result)
    }

    @Test
    fun `mock activity receives correct arguments`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Create mock
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting("TestInput"))
            .thenReturn("Formatted: TestInput")

        // Register mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // Execute workflow
        val result = client.executeWorkflow(
            WorkflowWithActivity::process,
            "TestInput",
            options.copy(workflowId = "test-${UUID.randomUUID()}"),
        )

        // Verify
        assertEquals("Formatted: TestInput", result)
        Mockito.verify(mockActivity).formatGreeting("TestInput")
    }

    // Note: Activity exception propagation tests are covered in KJavaWorkflowCompatibilityTest
    // using Java-style workflows, as they work more reliably with the test server's time skipping.

    @Test
    fun `mock multiple activities in same workflow`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Create mocks
        val mockGreeting = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockGreeting.formatGreeting(Mockito.anyString()))
            .thenReturn("Hello, Test!")

        val mockCounter = Mockito.mock(CounterActivity::class.java)
        Mockito.`when`(mockCounter.increment(Mockito.anyInt()))
            .thenReturn(42)

        // Register both mocks
        testEnv.registerActivitiesImplementations(mockGreeting, mockCounter)

        // Execute workflow
        val result = client.executeWorkflow(
            WorkflowWithMultipleActivities::processMultiple,
            "Test",
            options.copy(workflowId = "test-multi-${UUID.randomUUID()}"),
        )

        // Verify
        assertEquals("Hello, Test! (count: 42)", result)
        Mockito.verify(mockGreeting).formatGreeting("Test")
        Mockito.verify(mockCounter).increment(1)
    }

    // Note: Missing activity error tests are covered in KJavaWorkflowCompatibilityTest
    // using Java-style workflows, as they work more reliably with the test server's time skipping.

    @Test
    fun `can register activities at any time before workflow call`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Verify environment is already started
        assertTrue(testEnv.isStarted)

        // Register mock after environment started (this is the key feature)
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting(Mockito.anyString()))
            .thenReturn("Late registration works!")

        testEnv.registerActivitiesImplementations(mockActivity)

        // Execute workflow
        val result = client.executeWorkflow(
            WorkflowWithActivity::process,
            "Test",
            options.copy(workflowId = "test-${UUID.randomUUID()}"),
        )

        // Verify
        assertEquals("Late registration works!", result)
    }
}

/**
 * Tests for real activity implementations registered via the mocking API.
 *
 * Verifies that registerActivitiesImplementations works for both mocks and real implementations.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KRealActivityRegistrationTest {

    @WorkflowInterface
    interface CalculatorWorkflow {
        @WorkflowMethod
        suspend fun calculate(a: Int, b: Int): Int
    }

    @ActivityInterface
    interface Calculator {
        @ActivityMethod
        fun add(a: Int, b: Int): Int
    }

    class CalculatorWorkflowImpl : CalculatorWorkflow {
        override suspend fun calculate(a: Int, b: Int): Int {
            return KWorkflow.executeActivity(
                Calculator::add,
                kargs(a, b),
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )
        }
    }

    // Real activity implementation
    class CalculatorImpl : Calculator {
        override fun add(a: Int, b: Int): Int = a + b
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(CalculatorWorkflowImpl::class)
        }
    }

    @Test
    fun `real activity implementation works via registerActivitiesImplementations`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Register real implementation (not a mock)
        testEnv.registerActivitiesImplementations(CalculatorImpl())

        // Execute workflow
        val result = client.executeWorkflow(
            CalculatorWorkflow::calculate,
            kargs(5, 3),
            options.copy(workflowId = "calculator-${UUID.randomUUID()}"),
        )

        // Verify
        assertEquals(8, result)
    }
}

/**
 * Tests for KDynamicActivity fallback combined with regular activities.
 *
 * Verifies that the registry correctly routes:
 * - Known activity types to registered implementations
 * - Unknown activity types to the KDynamicActivity fallback
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KDynamicActivityFallbackTest {

    @WorkflowInterface
    interface MixedActivityWorkflow {
        @WorkflowMethod
        suspend fun process(input: String): String
    }

    @ActivityInterface
    interface KnownActivity {
        @ActivityMethod
        fun greet(name: String): String
    }

    class MixedActivityWorkflowImpl : MixedActivityWorkflow {
        override suspend fun process(input: String): String {
            // Call a known activity type (registered via mock)
            val knownResult = KWorkflow.executeActivity(
                KnownActivity::greet,
                input,
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )

            // Call an unknown activity type (handled by dynamic fallback)
            val dynamicResult = KWorkflow.executeActivity<String>(
                "unknownActivityType",
                listOf(input),
                KActivityOptions(startToCloseTimeout = 1.minutes),
            )

            return "$knownResult | $dynamicResult"
        }
    }

    class KnownActivityImpl : KnownActivity {
        override fun greet(name: String): String = "Hello, $name!"
    }

    /**
     * Dynamic activity fallback that handles any unknown activity type.
     */
    class FallbackDynamicActivity : io.temporal.kotlin.activity.KDynamicActivity {
        override fun execute(args: io.temporal.kotlin.common.KEncodedValues): Any? {
            val activityType = io.temporal.activity.Activity.getExecutionContext().info.activityType
            val input = args.get<String>(0)
            return "Dynamic[$activityType]: $input"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(MixedActivityWorkflowImpl::class)
            // Register KDynamicActivity as fallback via the extension
            activityImplementations = listOf(FallbackDynamicActivity())
        }
    }

    @Test
    fun `KDynamicActivity fallback handles unknown activity types while known types use registry`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Register a known activity implementation
        testEnv.registerActivitiesImplementations(KnownActivityImpl())

        // Execute workflow that calls both known and unknown activity types
        val result = client.executeWorkflow(
            MixedActivityWorkflow::process,
            "World",
            options.copy(workflowId = "mixed-${UUID.randomUUID()}"),
        )

        // Verify both activities were handled correctly
        assertEquals("Hello, World! | Dynamic[unknownActivityType]: World", result)
    }

    @Test
    fun `mocked activity takes precedence over KDynamicActivity fallback`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        // Register a mock for the known activity
        val mockActivity = Mockito.mock(KnownActivity::class.java)
        Mockito.`when`(mockActivity.greet(Mockito.anyString()))
            .thenReturn("Mocked greeting!")

        testEnv.registerActivitiesImplementations(mockActivity)

        // Execute workflow
        val result = client.executeWorkflow(
            MixedActivityWorkflow::process,
            "Test",
            options.copy(workflowId = "mock-precedence-${UUID.randomUUID()}"),
        )

        // Verify mock was used for known activity, fallback for unknown
        assertEquals("Mocked greeting! | Dynamic[unknownActivityType]: Test", result)
        Mockito.verify(mockActivity).greet("Test")
    }
}
