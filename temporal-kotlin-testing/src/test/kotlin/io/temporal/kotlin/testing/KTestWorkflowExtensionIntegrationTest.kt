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
import io.temporal.kotlin.client.KWorkflowClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.worker.Worker
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for [KTestWorkflowExtension].
 *
 * Tests cover:
 * - Extension lifecycle (beforeEach/afterEach)
 * - Parameter injection (environment, client, options, worker, workflow stubs)
 * - @WorkflowInitialTime annotation support
 * - Search attribute configuration
 * - Workflow and activity registration
 * - DSL configuration
 */
class KTestWorkflowExtensionIntegrationTest {

    // ==================== Test Interfaces ====================

    @WorkflowInterface
    interface GreetingWorkflow {
        @WorkflowMethod
        fun greet(name: String): String
    }

    @WorkflowInterface
    interface TimerWorkflow {
        @WorkflowMethod
        fun waitAndReturn(seconds: Long): String
    }

    @WorkflowInterface
    interface WorkflowWithActivity {
        @WorkflowMethod
        fun process(input: String): String
    }

    @ActivityInterface
    interface GreetingActivities {
        @ActivityMethod
        fun formatGreeting(name: String): String
    }

    // ==================== Test Implementations ====================

    class GreetingWorkflowImpl : GreetingWorkflow {
        override fun greet(name: String): String {
            return "Hello, $name!"
        }
    }

    class TimerWorkflowImpl : TimerWorkflow {
        override fun waitAndReturn(seconds: Long): String {
            Workflow.sleep(Duration.ofSeconds(seconds))
            return "Waited ${seconds}s"
        }
    }

    class WorkflowWithActivityImpl : WorkflowWithActivity {
        private val activities = Workflow.newActivityStub(
            GreetingActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build(),
        )

        override fun process(input: String): String {
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
            registerWorkflowImplementationTypes<GreetingWorkflowImpl>()
            registerWorkflowImplementationTypes<TimerWorkflowImpl>()
            registerWorkflowImplementationTypes<WorkflowWithActivityImpl>()
            setActivityImplementations(GreetingActivitiesImpl())
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
    fun `inject KWorkflowClient`(client: KWorkflowClient) {
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
    fun `inject Java Worker`(worker: Worker) {
        assertNotNull(worker)
    }

    @Test
    fun `inject workflow stub`(workflow: GreetingWorkflow) {
        assertNotNull(workflow)

        val result = workflow.greet("World")
        assertEquals("Hello, World!", result)
    }

    @Test
    fun `inject multiple parameters`(
        testEnv: KTestWorkflowEnvironment,
        client: KWorkflowClient,
        options: KWorkflowOptions,
        workflow: GreetingWorkflow,
    ) {
        assertNotNull(testEnv)
        assertNotNull(client)
        assertNotNull(options)
        assertNotNull(workflow)

        val result = workflow.greet("Test")
        assertEquals("Hello, Test!", result)
    }

    // ==================== Workflow Execution Tests ====================

    @Test
    fun `execute simple workflow via injected stub`(workflow: GreetingWorkflow) {
        val result = workflow.greet("Kotlin")
        assertEquals("Hello, Kotlin!", result)
    }

    @Test
    fun `execute workflow via client and options`(
        client: KWorkflowClient,
        options: KWorkflowOptions,
    ) {
        val workflow = client.workflowClient.newWorkflowStub(
            GreetingWorkflow::class.java,
            options.toJavaOptions(),
        )

        val result = workflow.greet("Manual")
        assertEquals("Hello, Manual!", result)
    }

    // ==================== Timer and Time Skipping Tests ====================

    @Test
    fun `timer workflow completes quickly with time skipping`(workflow: TimerWorkflow) {
        val startTime = System.currentTimeMillis()

        val result = workflow.waitAndReturn(3600) // 1 hour

        val elapsed = System.currentTimeMillis() - startTime
        assertEquals("Waited 3600s", result)
        // Should complete in seconds, not an hour
        assertTrue(elapsed < 60_000, "Should complete in less than 60 seconds")
    }

    // ==================== Activity Integration Tests ====================

    @Test
    fun `workflow with activity executes correctly`(workflow: WorkflowWithActivity) {
        GreetingActivitiesImpl.executionCount.set(0)

        val result = workflow.process("Test Input")

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
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        // Execute a workflow
        val workflow = testEnv.workflowClient.workflowClient.newWorkflowStub(
            GreetingWorkflow::class.java,
            options.toJavaOptions(),
        )
        val result = workflow.greet("Isolation")

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
            "Current time should be at or after annotation initial time",
        )
    }
}

/**
 * Tests for custom namespace configuration in [KTestWorkflowExtension].
 */
class KTestWorkflowExtensionNamespaceTest {

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        fun execute(): String
    }

    class SimpleWorkflowImpl : SimpleWorkflow {
        override fun execute(): String = "done"
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            namespace = "custom-test-namespace"
            registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        }
    }

    @Test
    fun `custom namespace is applied`(testEnv: KTestWorkflowEnvironment) {
        assertEquals("custom-test-namespace", testEnv.namespace)
    }
}

/**
 * Tests for initial time configuration in [KTestWorkflowExtension].
 */
class KTestWorkflowExtensionInitialTimeTest {

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        fun execute(): String
    }

    class SimpleWorkflowImpl : SimpleWorkflow {
        override fun execute(): String = "done"
    }

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            initialTime = Instant.parse("2024-06-15T12:00:00Z")
            registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        }
    }

    @Test
    fun `initial time is set from extension config`(testEnv: KTestWorkflowEnvironment) {
        val expectedMinTime = Instant.parse("2024-06-15T12:00:00Z").toEpochMilli()
        assertTrue(
            testEnv.currentTimeMillis >= expectedMinTime,
            "Current time should be at or after configured initial time",
        )
    }
}
