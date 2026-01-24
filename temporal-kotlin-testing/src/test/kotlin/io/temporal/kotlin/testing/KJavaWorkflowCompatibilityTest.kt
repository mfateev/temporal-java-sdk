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

@file:OptIn(kotlin.time.ExperimentalTime::class, io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin.testing

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowOptions
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.worker.KWorker
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.Mockito
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Compatibility tests for Java-style (non-suspend) workflows and activities.
 *
 * This test class verifies that traditional Java SDK workflow and activity patterns
 * continue to work correctly with the Kotlin testing infrastructure. These tests use:
 * - Non-suspend workflow methods (blocking Java-style)
 * - `Workflow.sleep()` for timers
 * - `Workflow.newActivityStub()` for activity invocation
 * - `Workflow.await()` for conditions
 *
 * This ensures backwards compatibility for users migrating from Java or using
 * mixed Java/Kotlin codebases.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KJavaWorkflowCompatibilityTest {

    // ==================== Java-style Interfaces (non-suspend) ====================

    @WorkflowInterface
    interface JavaStyleWorkflow {
        @WorkflowMethod
        fun execute(input: String): String
    }

    @WorkflowInterface
    interface JavaStyleActivityWorkflow {
        @WorkflowMethod
        fun execute(input: String): String
    }

    @WorkflowInterface
    interface JavaStyleTimerWorkflow {
        @WorkflowMethod
        fun executeWithTimer(waitSeconds: Long): String
    }

    @WorkflowInterface
    interface JavaStyleSignalWorkflow {
        @WorkflowMethod
        fun execute(): String

        @io.temporal.workflow.SignalMethod
        fun signal(value: String)
    }

    @ActivityInterface
    interface JavaStyleActivity {
        @ActivityMethod
        fun process(input: String): String
    }

    // ==================== Java-style Implementations ====================

    class JavaStyleWorkflowImpl : JavaStyleWorkflow {
        override fun execute(input: String): String {
            return "Hello, $input!"
        }
    }

    class JavaStyleTimerWorkflowImpl : JavaStyleTimerWorkflow {
        override fun executeWithTimer(waitSeconds: Long): String {
            // Using Java SDK Workflow.sleep() - this is the Java-style pattern
            Workflow.sleep(Duration.ofSeconds(waitSeconds))
            return "Completed after ${waitSeconds}s"
        }
    }

    class JavaStyleSignalWorkflowImpl : JavaStyleSignalWorkflow {
        private var received: String = ""

        override fun execute(): String {
            // Using Java SDK Workflow.await() - this is the Java-style pattern
            Workflow.await { received.isNotEmpty() }
            return "Received: $received"
        }

        override fun signal(value: String) {
            received = value
        }
    }

    class JavaStyleWorkflowWithActivityImpl : JavaStyleActivityWorkflow {
        // Using Java SDK Workflow.newActivityStub() - this is the Java-style pattern
        private val activity = Workflow.newActivityStub(
            JavaStyleActivity::class.java,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(1))
                .build(),
        )

        override fun execute(input: String): String {
            return activity.process(input)
        }
    }

    class JavaStyleActivityImpl : JavaStyleActivity {
        override fun process(input: String): String {
            return "Processed: $input"
        }
    }

    // ==================== Test Extension ====================

    companion object {
        @JvmField
        @RegisterExtension
        val extension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(
                JavaStyleWorkflowImpl::class,
                JavaStyleTimerWorkflowImpl::class,
                JavaStyleSignalWorkflowImpl::class,
                JavaStyleWorkflowWithActivityImpl::class,
            )
            activityImplementations = listOf(JavaStyleActivityImpl())
            useTimeskipping = true
        }
    }

    // ==================== Helper ====================

    private fun <T> createWorkflowStub(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
        workflowInterface: Class<T>,
    ): T {
        return testEnv.workflowClient.workflowClient.newWorkflowStub(
            workflowInterface,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("java-compat-${UUID.randomUUID()}")
                .build(),
        )
    }

    // ==================== Basic Workflow Tests ====================

    @Test
    fun `Java-style simple workflow executes correctly`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        val workflow = createWorkflowStub(testEnv, options, JavaStyleWorkflow::class.java)
        val result = workflow.execute("World")
        assertEquals("Hello, World!", result)
    }

    @Test
    fun `Java-style workflow with Workflow sleep completes with time skipping`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        val workflow = createWorkflowStub(testEnv, options, JavaStyleTimerWorkflow::class.java)

        val startTime = System.currentTimeMillis()
        val result = workflow.executeWithTimer(3600) // 1 hour timer
        val elapsed = System.currentTimeMillis() - startTime

        assertEquals("Completed after 3600s", result)
        // With time skipping, should complete quickly
        assertTrue(elapsed < 60_000, "Should complete in less than 60 seconds, took ${elapsed}ms")
    }

    @Test
    fun `Java-style workflow with Workflow await and signals works`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        val client = testEnv.workflowClient.workflowClient
        val javaOptions = WorkflowOptions.newBuilder()
            .setTaskQueue(options.taskQueue)
            .setWorkflowId("signal-${UUID.randomUUID()}")
            .build()

        // Start workflow asynchronously
        val handle = client.newUntypedWorkflowStub("JavaStyleSignalWorkflow", javaOptions)
        handle.start()

        // Register a delayed callback to send signal
        testEnv.registerDelayedCallback(kotlin.time.Duration.parse("1m")) {
            val signalWorkflow = client.newWorkflowStub(
                JavaStyleSignalWorkflow::class.java,
                handle.execution!!.workflowId,
            )
            signalWorkflow.signal("test-signal")
        }

        // Get result
        val result = handle.getResult(String::class.java)
        assertEquals("Received: test-signal", result)
    }

    // ==================== Activity Tests ====================

    @Test
    fun `Java-style workflow with Workflow newActivityStub executes correctly`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        // Use the workflow implementation that has activity registered in extension
        val client = testEnv.workflowClient.workflowClient
        val workflow = client.newWorkflowStub(
            JavaStyleActivityWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("activity-${UUID.randomUUID()}")
                .build(),
        )

        val result = workflow.execute("Activity Test")
        assertEquals("Processed: Activity Test", result)
    }

    // ==================== Activity Mocking Tests ====================

    @Test
    fun `Java-style workflow with mocked activity`(
        testEnv: KTestWorkflowEnvironment,
        options: KWorkflowOptions,
    ) {
        // Create a mock activity
        val mockActivity = Mockito.mock(JavaStyleActivity::class.java)
        Mockito.`when`(mockActivity.process(Mockito.anyString()))
            .thenReturn("Mocked result!")

        // Register the mock
        testEnv.registerActivitiesImplementations(mockActivity)

        // The extension has registered JavaStyleWorkflowWithActivityImpl
        // Create a workflow stub for that type
        val client = testEnv.workflowClient.workflowClient
        val workflow = client.newWorkflowStub(
            JavaStyleActivityWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue(options.taskQueue)
                .setWorkflowId("mocked-activity-${UUID.randomUUID()}")
                .build(),
        )

        val result = workflow.execute("test")
        assertEquals("Mocked result!", result)
        Mockito.verify(mockActivity).process("test")
    }

    // ==================== Environment API Tests ====================

    @Test
    fun `KTestWorkflowEnvironment works with Java-style workflows`(
        testEnv: KTestWorkflowEnvironment,
    ) {
        assertNotNull(testEnv)
        assertTrue(testEnv.isStarted)
        assertNotNull(testEnv.workflowClient)
    }

    @Test
    fun `KWorker registration works for Java-style workflows`(
        worker: KWorker,
    ) {
        assertNotNull(worker)
        assertNotNull(worker.worker)
    }
}

/**
 * Tests verifying that Java SDK APIs can be used directly alongside Kotlin wrappers.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KJavaSdkDirectUsageTest {

    @WorkflowInterface
    interface SimpleWorkflow {
        @WorkflowMethod
        fun execute(input: String): String
    }

    class SimpleWorkflowImpl : SimpleWorkflow {
        override fun execute(input: String): String = "Result: $input"
    }

    @Test
    fun `Java TestWorkflowEnvironment works directly`() {
        val javaTestEnv = io.temporal.testing.TestWorkflowEnvironment.newInstance()
        val javaWorker = javaTestEnv.newWorker("test-task-queue")
        javaWorker.registerWorkflowImplementationTypes(SimpleWorkflowImpl::class.java)
        javaTestEnv.start()

        val workflow = javaTestEnv.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val result = workflow.execute("Java Direct")
        assertEquals("Result: Java Direct", result)

        javaTestEnv.close()
    }

    @Test
    fun `KWorker wrapping Java Worker works`() {
        val javaTestEnv = io.temporal.testing.TestWorkflowEnvironment.newInstance()
        val javaWorker = javaTestEnv.newWorker("test-task-queue")

        // Wrap Java Worker with KWorker
        val kWorker = KWorker(javaWorker)
        kWorker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()

        javaTestEnv.start()

        val workflow = javaTestEnv.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val result = workflow.execute("KWorker Wrap")
        assertEquals("Result: KWorker Wrap", result)

        javaTestEnv.close()
    }

    @Test
    fun `KTestWorkflowEnvironment wrapping Java TestWorkflowEnvironment works`() {
        val testEnv = KTestWorkflowEnvironment.newInstance()

        val worker = testEnv.newWorker("test-task-queue")
        worker.registerWorkflowImplementationTypes<SimpleWorkflowImpl>()
        testEnv.start()

        // Use underlying Java client directly
        val workflow = testEnv.workflowClient.workflowClient.newWorkflowStub(
            SimpleWorkflow::class.java,
            WorkflowOptions.newBuilder()
                .setTaskQueue("test-task-queue")
                .build(),
        )

        val result = workflow.execute("KTestEnv Wrap")
        assertEquals("Result: KTestEnv Wrap", result)

        testEnv.close()
    }
}
