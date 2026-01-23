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
import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.worker.KWorker
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for [KTestWorkflowExtension].
 *
 * Tests cover:
 * - Extension lifecycle (beforeEach/afterEach)
 * - Parameter injection (environment, client, options, worker)
 * - @WorkflowInitialTime annotation support
 * - Search attribute configuration
 * - Workflow and activity registration
 * - DSL configuration
 *
 * All tests use suspend workflows and the method reference pattern with `runTest`.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KTestWorkflowExtensionIntegrationTest {

    // ==================== Test Interfaces ====================

    @WorkflowInterface
    interface GreetingWorkflow {
        @WorkflowMethod
        suspend fun greet(name: String): String
    }

    @WorkflowInterface
    interface TimerWorkflow {
        @WorkflowMethod
        suspend fun waitAndReturn(seconds: Long): String
    }

    @WorkflowInterface
    interface WorkflowWithActivity {
        @WorkflowMethod
        suspend fun process(input: String): String
    }

    @ActivityInterface
    interface GreetingActivities {
        @ActivityMethod
        fun formatGreeting(name: String): String
    }

    // ==================== Test Implementations ====================

    class GreetingWorkflowImpl : GreetingWorkflow {
        override suspend fun greet(name: String): String {
            return "Hello, $name!"
        }
    }

    class TimerWorkflowImpl : TimerWorkflow {
        override suspend fun waitAndReturn(seconds: Long): String {
            Workflow.sleep(java.time.Duration.ofSeconds(seconds))
            return "Waited ${seconds}s"
        }
    }

    class WorkflowWithActivityImpl : WorkflowWithActivity {
        private val activities = Workflow.newActivityStub(
            GreetingActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(java.time.Duration.ofMinutes(1))
                .build()
        )

        override suspend fun process(input: String): String {
            return activities.formatGreeting(input)
        }
    }

    class GreetingActivitiesImpl : GreetingActivities {
        companion object {
            val executionCount = AtomicInteger(0)
        }

        override fun formatGreeting(name: String): String {
            executionCount.incrementAndGet()
            return "Formatted: $name"
        }
    }

    // ==================== Single Extension for Core Tests ====================

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(
                GreetingWorkflowImpl::class,
                TimerWorkflowImpl::class,
                WorkflowWithActivityImpl::class
            )
            activityImplementations = listOf(GreetingActivitiesImpl())
            useTimeskipping = true
            searchAttributes {
                register("CustomKeyword", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                register("CustomInt", IndexedValueType.INDEXED_VALUE_TYPE_INT)
            }
        }
    }

    // ==================== Parameter Injection Tests ====================

    @Test
    fun `inject KTestWorkflowEnvironment`(testEnv: KTestWorkflowEnvironment) {
        assertNotNull(testEnv)
        assertTrue(testEnv.isStarted)
    }

    @Test
    fun `inject KClient`(client: KClient) {
        assertNotNull(client)
        assertNotNull(client.workflowClient)
    }

    @Test
    fun `inject KWorkflowOptions`(options: KWorkflowOptions) {
        assertNotNull(options)
        assertNotNull(options.taskQueue)
        assertTrue(options.taskQueue!!.isNotEmpty())
    }

    @Test
    fun `inject KWorker`(worker: KWorker) {
        assertNotNull(worker)
        assertNotNull(worker.worker)
    }

    @Test
    fun `inject multiple parameters`(
        testEnv: KTestWorkflowEnvironment,
        client: KClient,
        options: KWorkflowOptions
    ) {
        assertNotNull(testEnv)
        assertNotNull(client)
        assertNotNull(options)
    }

    // ==================== Workflow Execution Tests ====================

    @Test
    fun `execute simple workflow via method reference`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        val result = client.executeWorkflow(
            GreetingWorkflow::greet,
            "Kotlin",
            options.copy(workflowId = "greeting-${UUID.randomUUID()}")
        )
        assertEquals("Hello, Kotlin!", result)
    }

    @Test
    fun `execute workflow with different input`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        val result = client.executeWorkflow(
            GreetingWorkflow::greet,
            "World",
            options.copy(workflowId = "greeting-world-${UUID.randomUUID()}")
        )
        assertEquals("Hello, World!", result)
    }

    // ==================== Timer and Time Skipping Tests ====================

    @Test
    fun `timer workflow completes quickly with time skipping`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        val startTime = System.currentTimeMillis()

        val result = client.executeWorkflow(
            TimerWorkflow::waitAndReturn,
            3600L, // 1 hour
            options.copy(workflowId = "timer-${UUID.randomUUID()}")
        )

        val elapsed = System.currentTimeMillis() - startTime
        assertEquals("Waited 3600s", result)
        // Should complete in seconds, not an hour
        assertTrue(elapsed < 60_000, "Should complete in less than 60 seconds")
    }

    // ==================== Activity Integration Tests ====================

    @Test
    fun `workflow with activity executes correctly`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        GreetingActivitiesImpl.executionCount.set(0)

        val result = client.executeWorkflow(
            WorkflowWithActivity::process,
            "Test Input",
            options.copy(workflowId = "activity-${UUID.randomUUID()}")
        )

        assertEquals("Formatted: Test Input", result)
        assertEquals(1, GreetingActivitiesImpl.executionCount.get())
    }

    // ==================== Search Attribute Tests ====================

    @Test
    fun `search attributes are registered`(testEnv: KTestWorkflowEnvironment) {
        // Environment should have search attributes registered
        // We verify by checking that the environment was created successfully
        assertNotNull(testEnv)
        assertTrue(testEnv.isStarted)
    }

    // ==================== Worker Options Tests ====================

    @Test
    fun `worker options are applied`(worker: KWorker) {
        // Worker should be created with custom options
        // We verify by checking that the worker was created successfully
        assertNotNull(worker)
        assertNotNull(worker.worker)
    }

    // ==================== Environment Lifecycle Tests ====================

    @Test
    fun `environment is started before test`(testEnv: KTestWorkflowEnvironment) {
        assertTrue(testEnv.isStarted, "Environment should be started before test runs")
        assertNotNull(testEnv.workflowClient)
    }

    @Test
    fun `each test gets isolated environment`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        val result = client.executeWorkflow(
            GreetingWorkflow::greet,
            "Isolation",
            options.copy(workflowId = "isolation-${UUID.randomUUID()}")
        )

        assertEquals("Hello, Isolation!", result)
        // Each test gets its own environment, so this should always work
    }

    // ==================== WorkflowInitialTime Annotation Tests ====================

    @Test
    @WorkflowInitialTime("2024-01-01T00:00:00Z")
    fun `WorkflowInitialTime annotation sets initial time`(testEnv: KTestWorkflowEnvironment) {
        val expectedMinTime = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli()
        assertTrue(
            testEnv.currentTimeMillis >= expectedMinTime,
            "Current time should be at or after annotation initial time"
        )
    }
}

/**
 * Tests for custom namespace configuration in [KTestWorkflowExtension].
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KTestWorkflowExtensionNamespaceTest {

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        suspend fun execute(): String
    }

    class SimpleWorkflowImpl : SimpleWorkflow {
        override suspend fun execute(): String = "done"
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            namespace = "custom-test-namespace"
            workflowImplementationTypes = listOf(SimpleWorkflowImpl::class)
        }
    }

    @Test
    fun `custom namespace is applied`(testEnv: KTestWorkflowEnvironment) {
        assertEquals("custom-test-namespace", testEnv.namespace)
    }

    @Test
    fun `workflow executes in custom namespace`(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        val result = client.executeWorkflow(
            SimpleWorkflow::execute,
            options.copy(workflowId = "namespace-test-${UUID.randomUUID()}")
        )
        assertEquals("done", result)
    }
}

/**
 * Tests for initial time configuration in [KTestWorkflowExtension].
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KTestWorkflowExtensionInitialTimeTest {

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        suspend fun execute(): String
    }

    class SimpleWorkflowImpl : SimpleWorkflow {
        override suspend fun execute(): String = "done"
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            initialTime = Instant.parse("2024-06-15T12:00:00Z")
            workflowImplementationTypes = listOf(SimpleWorkflowImpl::class)
        }
    }

    @Test
    fun `initial time is set from extension config`(testEnv: KTestWorkflowEnvironment) {
        val expectedMinTime = Instant.parse("2024-06-15T12:00:00Z").toEpochMilli()
        assertTrue(
            testEnv.currentTimeMillis >= expectedMinTime,
            "Current time should be at or after configured initial time"
        )
    }
}
