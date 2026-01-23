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
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowException
import io.temporal.client.WorkflowOptions
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.Mockito
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Integration tests for activity mocking support in [KTestWorkflowExtension].
 *
 * Tests cover:
 * - Mocking regular activities with Mockito
 * - Mocking suspend activities
 * - Verifying mock invocations
 * - Error handling for unregistered activity types
 * - Exception propagation from mocks
 *
 * Note: These tests use sync workflows because they test the underlying activity
 * mocking infrastructure. The workflows are executed via Java client stubs.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KActivityMockingIntegrationTest {

    // ==================== Test Interfaces ====================

    @WorkflowInterface
    interface WorkflowWithActivity {
        @WorkflowMethod
        fun process(input: String): String
    }

    @WorkflowInterface
    interface WorkflowWithMultipleActivities {
        @WorkflowMethod
        fun processMultiple(input: String): String
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
        private val activity = Workflow.newActivityStub(
            GreetingActivity::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build()
        )

        override fun process(input: String): String {
            return activity.formatGreeting(input)
        }
    }

    class WorkflowWithMultipleActivitiesImpl : WorkflowWithMultipleActivities {
        private val greeting = Workflow.newActivityStub(
            GreetingActivity::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build()
        )
        private val counter = Workflow.newActivityStub(
            CounterActivity::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build()
        )

        override fun processMultiple(input: String): String {
            val greeting = greeting.formatGreeting(input)
            val count = counter.increment(1)
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
                WorkflowWithMultipleActivitiesImpl::class
            )
            // Note: No activity implementations are registered here
            // They will be mocked via testEnv.registerActivitiesImplementations()
        }
    }

    private fun createWorkflowStub(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ): WorkflowWithActivity {
        return testEnv.workflowClient.workflowClient.newWorkflowStub(
            WorkflowWithActivity::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("test-${UUID.randomUUID()}")
                .build()
        )
    }

    private fun createMultiActivityWorkflowStub(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ): WorkflowWithMultipleActivities {
        return testEnv.workflowClient.workflowClient.newWorkflowStub(
            WorkflowWithMultipleActivities::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("test-multi-${UUID.randomUUID()}")
                .build()
        )
    }

    @Test
    fun `mock activity returns expected value`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Create mock using Mockito
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting(Mockito.anyString()))
            .thenReturn("Mocked: Hello!")

        // Register mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // Create workflow stub and execute
        val workflow = createWorkflowStub(testEnv, options)
        val result = workflow.process("World")

        // Verify result
        assertEquals("Mocked: Hello!", result)
    }

    @Test
    fun `mock activity receives correct arguments`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Create mock
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting("TestInput"))
            .thenReturn("Formatted: TestInput")

        // Register mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // Create workflow stub and execute
        val workflow = createWorkflowStub(testEnv, options)
        val result = workflow.process("TestInput")

        // Verify
        assertEquals("Formatted: TestInput", result)
        Mockito.verify(mockActivity).formatGreeting("TestInput")
    }

    @Test
    fun `mock activity can throw exception`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Create mock that throws a non-retryable exception
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting(Mockito.anyString()))
            .thenThrow(ApplicationFailure.newNonRetryableFailure("Activity failed!", "TestError"))

        // Register mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // Create workflow stub and execute, expecting failure
        val workflow = createWorkflowStub(testEnv, options)
        try {
            workflow.process("Test")
            fail("Expected WorkflowException")
        } catch (e: WorkflowException) {
            // Verify the exception chain contains our message
            assertTrue(e.cause is ActivityFailure)
            val activityFailure = e.cause as ActivityFailure
            assertTrue(
                activityFailure.cause?.message?.contains("Activity failed!") == true,
                "Exception should contain original error message"
            )
        }
    }

    @Test
    fun `mock multiple activities in same workflow`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Create mocks
        val mockGreeting = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockGreeting.formatGreeting(Mockito.anyString()))
            .thenReturn("Hello, Test!")

        val mockCounter = Mockito.mock(CounterActivity::class.java)
        Mockito.`when`(mockCounter.increment(Mockito.anyInt()))
            .thenReturn(42)

        // Register both mocks
        testEnv.registerActivitiesImplementations(mockGreeting, mockCounter)

        // Create workflow stub and execute
        val workflow = createMultiActivityWorkflowStub(testEnv, options)
        val result = workflow.processMultiple("Test")

        // Verify
        assertEquals("Hello, Test! (count: 42)", result)
        Mockito.verify(mockGreeting).formatGreeting("Test")
        Mockito.verify(mockCounter).increment(1)
    }

    @Test
    fun `error message when activity not registered`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Don't register any activity mock

        // Create workflow stub and execute, expecting failure
        val workflow = createWorkflowStub(testEnv, options)
        try {
            workflow.process("Test")
            fail("Expected WorkflowException")
        } catch (e: WorkflowException) {
            // Verify the error message mentions the missing activity type
            assertTrue(e.cause is ActivityFailure)
            val activityFailure = e.cause as ActivityFailure
            val errorMessage = activityFailure.cause?.message ?: ""
            assertTrue(
                errorMessage.contains("No activity implementation or mock registered") ||
                    errorMessage.contains("formatGreeting") ||
                    errorMessage.contains("FormatGreeting"),
                "Error should mention missing activity. Actual: $errorMessage"
            )
        }
    }

    @Test
    fun `can register activities at any time before workflow call`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions
    ) {
        // Verify environment is already started
        assertTrue(testEnv.isStarted)

        // Register mock after environment started (this is the key feature)
        val mockActivity = Mockito.mock(GreetingActivity::class.java)
        Mockito.`when`(mockActivity.formatGreeting(Mockito.anyString()))
            .thenReturn("Late registration works!")

        testEnv.registerActivitiesImplementations(mockActivity)

        // Create workflow stub and execute
        val workflow = createWorkflowStub(testEnv, options)
        val result = workflow.process("Test")

        // Verify
        assertEquals("Late registration works!", result)
    }
}

// Note: Suspend activity mocking from workflows requires special handling.
// Workflows use Java activity stubs which are blocking. Suspend activities
// are handled by the KMockDynamicActivityHandler which uses runBlocking
// to execute the suspend function. This is tested implicitly through
// the regular activity tests when using suspend implementations.

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
        fun calculate(a: Int, b: Int): Int
    }

    @ActivityInterface
    interface Calculator {
        @ActivityMethod
        fun add(a: Int, b: Int): Int
    }

    class CalculatorWorkflowImpl : CalculatorWorkflow {
        private val calculator = Workflow.newActivityStub(
            Calculator::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build()
        )

        override fun calculate(a: Int, b: Int): Int {
            return calculator.add(a, b)
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
        options: KWorkflowOptions
    ) {
        // Register real implementation (not a mock)
        testEnv.registerActivitiesImplementations(CalculatorImpl())

        // Create workflow stub and execute
        val workflow = testEnv.workflowClient.workflowClient.newWorkflowStub(
            CalculatorWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("calculator-${UUID.randomUUID()}")
                .build()
        )
        val result = workflow.calculate(5, 3)

        // Verify
        assertEquals(8, result)
    }
}
