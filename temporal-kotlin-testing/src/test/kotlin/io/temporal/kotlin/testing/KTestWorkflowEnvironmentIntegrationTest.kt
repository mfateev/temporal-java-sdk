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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.testing

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.client.WorkflowOptions
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Integration tests for [KTestWorkflowEnvironment].
 *
 * Tests cover:
 * - Environment creation with default and custom options
 * - Worker creation and registration
 * - Client access and workflow execution
 * - Time manipulation (sleep, currentTime, registerDelayedCallback)
 * - Lifecycle management (start, shutdown, isStarted)
 * - Search attribute registration
 * - Diagnostics output
 * - use() extension function
 */
class KTestWorkflowEnvironmentIntegrationTest {

    // ==================== Test Interfaces ====================

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        fun execute(input: String): String
    }

    @WorkflowInterface
    interface TimerWorkflow {
        @WorkflowMethod
        fun executeWithTimer(waitSeconds: Long): String
    }

    @WorkflowInterface
    interface SignalWorkflow {
        @WorkflowMethod
        fun execute(): String

        @io.temporal.workflow.SignalMethod
        fun signal(value: String)
    }

    @ActivityInterface
    interface TestActivities {
        @ActivityMethod
        fun process(input: String): String
    }

    // ==================== Test Implementations ====================

    class SimpleWorkflowImpl : SimpleWorkflow {
        override fun execute(input: String): String {
            return "Hello, $input!"
        }
    }

    class TimerWorkflowImpl : TimerWorkflow {
        override fun executeWithTimer(waitSeconds: Long): String {
            Workflow.sleep(Duration.ofSeconds(waitSeconds))
            return "Completed after ${waitSeconds}s"
        }
    }

    class SignalWorkflowImpl : SignalWorkflow {
        private var received: String = ""

        override fun execute(): String {
            Workflow.await { received.isNotEmpty() }
            return "Received: $received"
        }

        override fun signal(value: String) {
            received = value
        }
    }

    class TestActivitiesImpl : TestActivities {
        companion object {
            val executionCount = AtomicInteger(0)
        }

        override fun process(input: String): String {
            executionCount.incrementAndGet()
            return "Processed: $input"
        }
    }

    class WorkflowWithActivityImpl : SimpleWorkflow {
        private val activities = Workflow.newActivityStub(
            TestActivities::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build(),
        )

        override fun execute(input: String): String {
            return activities.process(input)
        }
    }

    // ==================== Test Setup ====================

    private var testEnv: KTestWorkflowEnvironment? = null

    @BeforeEach
    fun setUp() {
        TestActivitiesImpl.executionCount.set(0)
    }

    @AfterEach
    fun tearDown() {
        testEnv?.close()
        testEnv = null
    }

    // ==================== Environment Creation Tests ====================

    @Test
    fun `create environment with default options`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        assertNotNull(testEnv)
        assertEquals("UnitTest", testEnv!!.namespace)
        assertFalse(testEnv!!.isStarted)
    }

    @Test
    fun `create environment with custom namespace`() {
        testEnv = KTestWorkflowEnvironment.newInstance {
            namespace = "custom-namespace"
        }

        assertEquals("custom-namespace", testEnv!!.namespace)
    }

    @Test
    fun `create environment with initial time`() {
        val initialTime = Instant.parse("2024-06-15T12:00:00Z")

        testEnv = KTestWorkflowEnvironment.newInstance {
            this.initialTime = initialTime
        }

        // Current time should be at or after the initial time
        assertTrue(testEnv!!.currentTimeMillis >= initialTime.toEpochMilli())
    }

    @Test
    fun `create environment with search attributes`() {
        testEnv = KTestWorkflowEnvironment.newInstance {
            searchAttributes {
                register("CustomKeyword", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                register("CustomInt", IndexedValueType.INDEXED_VALUE_TYPE_INT)
            }
        }

        // Environment should be created successfully with search attributes
        assertNotNull(testEnv)
    }

    @Test
    fun `create environment with pre-built options`() {
        val options = KTestEnvironmentOptions.newBuilder {
            namespace = "pre-built-namespace"
            useTimeskipping = true
        }

        testEnv = KTestWorkflowEnvironment.newInstance(options)

        assertEquals("pre-built-namespace", testEnv!!.namespace)
    }

    // ==================== Worker Creation Tests ====================

    @Test
    fun `create worker with task queue name`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")

        assertNotNull(worker)
        assertNotNull(worker.worker)
    }

    @Test
    fun `create worker with options DSL`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue") {
            setMaxConcurrentActivityExecutionSize(100)
            setMaxConcurrentWorkflowTaskExecutionSize(50)
        }

        assertNotNull(worker)
    }

    @Test
    fun `register workflow implementation using reified generics`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()

        // Should not throw, workflow is registered
        testEnv!!.start()
        assertTrue(testEnv!!.isStarted)
    }

    @Test
    fun `register workflow implementation with options DSL`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl> {
            setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
        }

        testEnv!!.start()
        assertTrue(testEnv!!.isStarted)
    }

    @Test
    fun `register activities implementation`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<WorkflowWithActivityImpl>()
        worker.registerActivitiesImplementations(TestActivitiesImpl())

        testEnv!!.start()
        assertTrue(testEnv!!.isStarted)
    }

    // ==================== Workflow Execution Tests ====================

    @Test
    fun `execute simple workflow`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        val client = testEnv!!.workflowClient
        val workflow = client.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val result = workflow.execute("World")

        assertEquals("Hello, World!", result)
    }

    @Test
    fun `execute workflow with activity`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<WorkflowWithActivityImpl>()
        worker.registerActivitiesImplementations(TestActivitiesImpl())
        testEnv!!.start()

        val client = testEnv!!.workflowClient
        val workflow = client.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val result = workflow.execute("test-input")

        assertEquals("Processed: test-input", result)
        assertEquals(1, TestActivitiesImpl.executionCount.get())
    }

    // ==================== Time Manipulation Tests ====================

    @Test
    fun `workflow with timer completes quickly with time skipping`() {
        testEnv = KTestWorkflowEnvironment.newInstance {
            useTimeskipping = true
        }

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<TimerWorkflowImpl>()
        testEnv!!.start()

        val client = testEnv!!.workflowClient
        val workflow = client.workflowClient.newWorkflowStub(
            TimerWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val startTime = System.currentTimeMillis()
        val result = workflow.executeWithTimer(3600) // 1 hour timer
        val elapsed = System.currentTimeMillis() - startTime

        assertEquals("Completed after 3600s", result)
        // With time skipping, should complete in seconds, not hours
        assertTrue(elapsed < 60_000, "Should complete in less than 60 seconds, took ${elapsed}ms")
    }

    @Test
    fun `sleep advances test time`() = runBlocking {
        testEnv = KTestWorkflowEnvironment.newInstance {
            initialTime = Instant.parse("2024-01-01T00:00:00Z")
        }

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        val initialTime = testEnv!!.currentTimeMillis

        testEnv!!.sleep(1.hours)

        val afterSleep = testEnv!!.currentTimeMillis
        val elapsed = afterSleep - initialTime

        // Should have advanced by at least 1 hour (3600000 ms)
        assertTrue(elapsed >= 3600000, "Time should advance by at least 1 hour, advanced by ${elapsed}ms")
    }

    @Test
    fun `sleep with Java Duration`() = runBlocking {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        val initialTime = testEnv!!.currentTimeMillis

        testEnv!!.sleep(Duration.ofMinutes(30))

        val afterSleep = testEnv!!.currentTimeMillis
        val elapsed = afterSleep - initialTime

        assertTrue(elapsed >= 30 * 60 * 1000, "Time should advance by at least 30 minutes")
    }

    @Test
    fun `registerDelayedCallback executes after delay`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SignalWorkflowImpl>()
        testEnv!!.start()

        val client = testEnv!!.workflowClient
        val options = WorkflowOptions.newBuilder()
            .setTaskQueue("test-task-queue")
            .build()
        val workflow = client.workflowClient.newWorkflowStub(SignalWorkflow::class.java, options)

        // Start workflow asynchronously
        val handle = client.workflowClient.newUntypedWorkflowStub("SignalWorkflow", options)
        handle.start()

        // Register a delayed callback to send signal after 1 minute
        testEnv!!.registerDelayedCallback(1.minutes) {
            val signalWorkflow = client.workflowClient.newWorkflowStub(
                SignalWorkflow::class.java,
                handle.execution!!.workflowId,
            )
            signalWorkflow.signal("delayed-signal")
        }

        // Get result - time skipping should allow the callback to execute
        val result = handle.getResult(String::class.java)

        assertEquals("Received: delayed-signal", result)
    }

    @Test
    fun `currentTime returns Instant`() {
        testEnv = KTestWorkflowEnvironment.newInstance {
            initialTime = Instant.parse("2024-06-15T12:00:00Z")
        }

        val currentTime = testEnv!!.currentTime

        assertNotNull(currentTime)
        assertTrue(currentTime.toEpochMilli() >= Instant.parse("2024-06-15T12:00:00Z").toEpochMilli())
    }

    // ==================== Lifecycle Management Tests ====================

    @Test
    fun `lifecycle states are correct`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()

        assertFalse(testEnv!!.isStarted)
        assertFalse(testEnv!!.isShutdown)
        assertFalse(testEnv!!.isTerminated)

        testEnv!!.start()

        assertTrue(testEnv!!.isStarted)
        assertFalse(testEnv!!.isShutdown)

        testEnv!!.shutdown()

        assertTrue(testEnv!!.isShutdown)
    }

    @Test
    fun `awaitTermination waits for shutdown`() = runBlocking {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        testEnv!!.shutdown()
        testEnv!!.awaitTermination(10.seconds)

        assertTrue(testEnv!!.isTerminated)
    }

    @Test
    fun `shutdownNow stops immediately`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        testEnv!!.shutdownNow()

        assertTrue(testEnv!!.isShutdown)
    }

    // ==================== Service Stubs Tests ====================

    @Test
    fun `workflowServiceStubs is accessible`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val stubs = testEnv!!.workflowServiceStubs

        assertNotNull(stubs)
    }

    @Test
    fun `operatorServiceStubs is accessible`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val stubs = testEnv!!.operatorServiceStubs

        assertNotNull(stubs)
    }

    // ==================== Search Attribute Tests ====================

    @Test
    fun `registerSearchAttribute returns true for new attribute`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val registered = testEnv!!.registerSearchAttribute(
            "TestAttribute",
            IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
        )

        assertTrue(registered)
    }

    @Test
    fun `registerSearchAttribute returns false for duplicate`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        testEnv!!.registerSearchAttribute("TestAttribute", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
        val registered = testEnv!!.registerSearchAttribute(
            "TestAttribute",
            IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
        )

        assertFalse(registered)
    }

    // ==================== Diagnostics Tests ====================

    @Test
    fun `getDiagnostics returns information`() {
        testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv!!.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv!!.start()

        // Execute a workflow to generate history
        val client = testEnv!!.workflowClient
        val workflow = client.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )
        workflow.execute("test")

        val diagnostics = testEnv!!.getDiagnostics()

        assertNotNull(diagnostics)
        // Diagnostics should contain workflow history information
        assertTrue(diagnostics.isNotEmpty())
    }

    // ==================== use() Extension Tests ====================

    @Test
    fun `use extension closes environment after block`() {
        val closed = AtomicBoolean(false)

        KTestWorkflowEnvironment.newInstance().use { env ->
            val worker = env.newWorker("test-task-queue")
            worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
            env.start()

            val client = env.workflowClient
            val workflow = client.workflowClient.newWorkflowStub(
                SimpleWorkflow::class.java,
                WorkflowOptions.newBuilder()
                    .setTaskQueue("test-task-queue")
                    .build(),
            )
            val result = workflow.execute("test")
            assertEquals("Hello, test!", result)
        }

        // Environment should be closed after use block
        // We can't directly check if it's closed, but the test completing
        // without hanging indicates proper cleanup
    }

    @Test
    fun `use extension returns block result`() {
        val result = KTestWorkflowEnvironment.newInstance().use { env ->
            val worker = env.newWorker("test-task-queue")
            worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
            env.start()

            val client = env.workflowClient
            val workflow = client.workflowClient.newWorkflowStub(
                SimpleWorkflow::class.java,
                WorkflowOptions.newBuilder()
                    .setTaskQueue("test-task-queue")
                    .build(),
            )
            workflow.execute("World")
        }

        assertEquals("Hello, World!", result)
    }
}
