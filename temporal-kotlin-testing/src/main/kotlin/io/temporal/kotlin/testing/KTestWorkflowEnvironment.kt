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

import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.api.nexus.v1.Endpoint
import io.temporal.kotlin.activity.KActivityRegistry
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.toJava
import io.temporal.kotlin.worker.KWorker
import io.temporal.serviceclient.OperatorServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.WorkerOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.time.Duration as JavaDuration
import kotlin.time.Duration as KotlinDuration

/**
 * Kotlin test environment for workflow unit testing.
 *
 * Provides an in-memory Temporal service with automatic time skipping,
 * allowing workflows that run for hours/days to be tested in milliseconds.
 *
 * Example:
 * ```kotlin
 * val testEnv = KTestWorkflowEnvironment.newInstance {
 *     namespace = "test-namespace"
 *     initialTime = Instant.parse("2024-01-01T00:00:00Z")
 * }
 *
 * val worker = testEnv.newWorker("task-queue")
 * worker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
 * worker.registerActivitiesImplementations(MyActivitiesImpl())
 *
 * testEnv.start()
 *
 * val result = testEnv.workflowClient.executeWorkflow(
 *     MyWorkflow::execute,
 *     KWorkflowOptions(taskQueue = "task-queue"),
 *     "input"
 * )
 *
 * testEnv.close()
 * ```
 */
public class KTestWorkflowEnvironment private constructor(
    private val testEnvironment: TestWorkflowEnvironment,
    internal val activityRegistry: KActivityRegistry = KActivityRegistry(),
) : Closeable {

    // ========== Commit 7: Basic structure with worker creation ==========

    /**
     * The namespace used by this test environment.
     */
    public val namespace: String
        get() = testEnvironment.namespace

    /**
     * Create a new Kotlin worker for the specified task queue.
     *
     * Example:
     * ```kotlin
     * val worker = testEnv.newWorker("task-queue")
     * worker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
     * ```
     *
     * @param taskQueue The task queue name
     * @return A KWorker instance
     */
    public fun newWorker(taskQueue: String): KWorker {
        return KWorker(testEnvironment.newWorker(taskQueue))
    }

    /**
     * Create a new Kotlin worker with options DSL.
     *
     * Example:
     * ```kotlin
     * val worker = testEnv.newWorker("task-queue") {
     *     maxConcurrentActivityExecutionSize = 100
     *     maxConcurrentWorkflowTaskExecutionSize = 50
     * }
     * ```
     *
     * @param taskQueue The task queue name
     * @param options DSL builder for WorkerOptions
     * @return A KWorker instance
     */
    public fun newWorker(
        taskQueue: String,
        options: WorkerOptions.Builder.() -> Unit,
    ): KWorker {
        val workerOptions = WorkerOptions.newBuilder().apply(options).build()
        return KWorker(testEnvironment.newWorker(taskQueue, workerOptions))
    }

    /**
     * Create a new Kotlin worker with WorkerOptions.
     *
     * Example:
     * ```kotlin
     * val options = WorkerOptions.newBuilder()
     *     .setMaxConcurrentActivityExecutionSize(100)
     *     .build()
     * val worker = testEnv.newWorker("task-queue", options)
     * ```
     *
     * @param taskQueue The task queue name
     * @param options WorkerOptions instance
     * @return A KWorker instance
     */
    public fun newWorker(taskQueue: String, options: WorkerOptions): KWorker {
        return KWorker(testEnvironment.newWorker(taskQueue, options))
    }

    // ========== Commit 8: Client access ==========

    /**
     * The Kotlin workflow client for interacting with workflows.
     *
     * This property wraps the test environment's WorkflowClient which includes
     * the TimeLockingInterceptor needed for time skipping.
     *
     * Example:
     * ```kotlin
     * val result = testEnv.workflowClient.executeWorkflow(
     *     MyWorkflow::execute,
     *     KWorkflowOptions(taskQueue = "task-queue"),
     *     "input"
     * )
     * ```
     */
    public val workflowClient: KClient by lazy {
        KClient(testEnvironment.workflowClient)
    }

    /**
     * Access to WorkflowServiceStubs for advanced scenarios.
     *
     * Use this for low-level operations not covered by the Kotlin API.
     */
    public val workflowServiceStubs: WorkflowServiceStubs
        get() = testEnvironment.workflowServiceStubs

    /**
     * Access to OperatorServiceStubs for administrative operations.
     *
     * Use this for namespace management and other operator tasks.
     */
    public val operatorServiceStubs: OperatorServiceStubs
        get() = testEnvironment.operatorServiceStubs

    // ========== Commit 9: Time manipulation ==========

    /**
     * Current test time in milliseconds since epoch.
     *
     * This time may differ from system time due to time skipping.
     * When time skipping is enabled, this reflects the virtual test time.
     */
    public val currentTimeMillis: Long
        get() = testEnvironment.currentTimeMillis()

    /**
     * Current test time as an Instant.
     *
     * This is a convenience property that wraps [currentTimeMillis].
     */
    public val currentTime: Instant
        get() = Instant.ofEpochMilli(currentTimeMillis)

    /**
     * Sleep for the specified duration with time skipping.
     *
     * This suspend function advances the test time without blocking the thread.
     * When time skipping is enabled, this allows workflows that sleep for long
     * durations to be tested quickly.
     *
     * Example:
     * ```kotlin
     * // Advance test time by 1 hour without actually waiting
     * testEnv.sleep(1.hours)
     * ```
     *
     * @param duration Kotlin Duration to sleep
     */
    public suspend fun sleep(duration: KotlinDuration) {
        withContext(Dispatchers.IO) {
            testEnvironment.sleep(duration.toJava())
        }
    }

    /**
     * Sleep for the specified duration with time skipping.
     *
     * @param duration Java Duration to sleep
     */
    public suspend fun sleep(duration: JavaDuration) {
        withContext(Dispatchers.IO) {
            testEnvironment.sleep(duration)
        }
    }

    // ========== Commit 10: Delayed callbacks ==========

    /**
     * Register a callback to execute after the specified delay in test time.
     *
     * The callback will be executed when the test time advances past the delay.
     * This is useful for sending signals or performing actions at specific points
     * during workflow execution.
     *
     * Example:
     * ```kotlin
     * // Send a signal after 1 hour of test time
     * testEnv.registerDelayedCallback(1.hours) {
     *     workflow.processSignal("signal-input")
     * }
     * ```
     *
     * @param delay Kotlin Duration delay
     * @param callback The callback to execute
     */
    public fun registerDelayedCallback(delay: KotlinDuration, callback: () -> Unit) {
        testEnvironment.registerDelayedCallback(delay.toJava()) { callback() }
    }

    /**
     * Register a callback to execute after the specified delay in test time.
     *
     * @param delay Java Duration delay
     * @param callback The callback to execute
     */
    public fun registerDelayedCallback(delay: JavaDuration, callback: () -> Unit) {
        testEnvironment.registerDelayedCallback(delay) { callback() }
    }

    // ========== Commit 11: Lifecycle management ==========

    /**
     * Whether the workers have been started.
     *
     * Returns true after [start] has been called.
     */
    public val isStarted: Boolean
        get() = testEnvironment.isStarted

    /**
     * Whether shutdown has been initiated.
     *
     * Returns true after [shutdown] or [shutdownNow] has been called.
     */
    public val isShutdown: Boolean
        get() = testEnvironment.isShutdown

    /**
     * Whether all workers have terminated.
     *
     * Returns true when all workers have finished processing after shutdown.
     */
    public val isTerminated: Boolean
        get() = testEnvironment.isTerminated

    /**
     * Start all registered workers.
     *
     * This must be called after registering workflows and activities,
     * before executing any workflows.
     *
     * Example:
     * ```kotlin
     * val worker = testEnv.newWorker("task-queue")
     * worker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
     * testEnv.start()  // Start processing tasks
     * ```
     */
    public fun start() {
        testEnvironment.start()
    }

    /**
     * Initiate graceful shutdown.
     *
     * Workers stop accepting new tasks but complete in-progress work.
     * Use [awaitTermination] to wait for all work to complete.
     */
    public fun shutdown() {
        testEnvironment.shutdown()
    }

    /**
     * Initiate immediate shutdown.
     *
     * Attempts to stop all processing immediately via thread interruption.
     * Use [awaitTermination] to wait for termination.
     */
    public fun shutdownNow() {
        testEnvironment.shutdownNow()
    }

    /**
     * Wait for all workers to terminate.
     *
     * This suspend function blocks until all workers have finished
     * or the timeout expires.
     *
     * @param timeout Maximum time to wait
     */
    public suspend fun awaitTermination(timeout: KotlinDuration) {
        withContext(Dispatchers.IO) {
            testEnvironment.awaitTermination(
                timeout.inWholeMilliseconds,
                TimeUnit.MILLISECONDS,
            )
        }
    }

    /**
     * Wait for all workers to terminate.
     *
     * @param timeout Maximum time to wait (Java Duration)
     */
    public suspend fun awaitTermination(timeout: JavaDuration) {
        withContext(Dispatchers.IO) {
            testEnvironment.awaitTermination(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS,
            )
        }
    }

    /**
     * Close the test environment.
     *
     * This calls shutdownNow() and awaitTermination() to ensure
     * clean shutdown of all resources.
     */
    override fun close() {
        testEnvironment.close()
    }

    // ========== Phase 3.2: Activity Mocking Support ==========

    /**
     * Register activity implementations for this test.
     *
     * This method can be called at any time - before or after the test environment starts.
     * The registered activities are handled by a dynamic activity handler that routes
     * calls to the appropriate implementation.
     *
     * Works with both real implementations and mocks created with frameworks like
     * mockito-kotlin or MockK.
     *
     * Example with mock:
     * ```kotlin
     * @Test
     * fun `test with mocked activity`(testEnv: KTestWorkflowEnvironment, workflow: MyWorkflow) {
     *     val mockActivity = mock<MyActivity> {
     *         on { doSomething(any()) } doReturn "mocked result"
     *     }
     *     testEnv.registerActivitiesImplementations(mockActivity)
     *
     *     val result = workflow.execute("input")
     *     assertEquals("mocked result", result)
     * }
     * ```
     *
     * Example with real implementation:
     * ```kotlin
     * @Test
     * fun `test with real activity`(testEnv: KTestWorkflowEnvironment, workflow: MyWorkflow) {
     *     testEnv.registerActivitiesImplementations(MyActivitiesImpl())
     *
     *     val result = workflow.execute("input")
     *     assertEquals("expected", result)
     * }
     * ```
     *
     * @param activities Activity implementation instances (real or mocks)
     */
    public fun registerActivitiesImplementations(vararg activities: Any) {
        activities.forEach { activity ->
            activityRegistry.registerMockImplementation(activity)
        }
    }

    // ========== Commit 12: Advanced features ==========

    /**
     * Register a search attribute with the test server.
     *
     * Search attributes must be registered before they can be used in workflows.
     *
     * Example:
     * ```kotlin
     * testEnv.registerSearchAttribute(
     *     "CustomKeyword",
     *     IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
     * )
     * ```
     *
     * @param name Search attribute name
     * @param type Search attribute type
     * @return true if registered, false if already exists
     */
    public fun registerSearchAttribute(name: String, type: IndexedValueType): Boolean {
        return testEnvironment.registerSearchAttribute(name, type)
    }

    /**
     * Create a Nexus endpoint for testing.
     *
     * Example:
     * ```kotlin
     * val endpoint = testEnv.createNexusEndpoint("my-service", "nexus-task-queue")
     * ```
     *
     * @param name Endpoint name
     * @param taskQueue Task queue for the endpoint
     * @return The created Endpoint
     */
    public fun createNexusEndpoint(name: String, taskQueue: String): Endpoint {
        return testEnvironment.createNexusEndpoint(name, taskQueue)
    }

    /**
     * Delete a Nexus endpoint.
     *
     * @param endpoint The endpoint to delete
     */
    public fun deleteNexusEndpoint(endpoint: Endpoint) {
        testEnvironment.deleteNexusEndpoint(endpoint)
    }

    /**
     * Get diagnostic information including workflow histories.
     *
     * This is useful for debugging test failures. The returned string
     * contains the histories of all workflow instances in the test environment.
     *
     * Example:
     * ```kotlin
     * try {
     *     // test code
     * } catch (e: Exception) {
     *     println(testEnv.getDiagnostics())
     *     throw e
     * }
     * ```
     *
     * @return Diagnostic data about the internal service state
     */
    public fun getDiagnostics(): String {
        return testEnvironment.diagnostics
    }

    // ========== Companion object: Factory methods ==========

    public companion object {
        /**
         * Create a new test environment with default options.
         *
         * The default namespace is "UnitTest" to match Java SDK conventions.
         *
         * Example:
         * ```kotlin
         * val testEnv = KTestWorkflowEnvironment.newInstance()
         * ```
         */
        public fun newInstance(): KTestWorkflowEnvironment {
            return newInstance {}
        }

        /**
         * Create a new test environment with DSL configuration.
         *
         * Example:
         * ```kotlin
         * val testEnv = KTestWorkflowEnvironment.newInstance {
         *     namespace = "test-namespace"
         *     initialTime = Instant.parse("2024-01-01T00:00:00Z")
         *     useTimeskipping = true
         *
         *     workerFactoryOptions {
         *         maxWorkflowThreadCount = 800
         *     }
         *
         *     searchAttributes {
         *         register("CustomKeyword", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
         *     }
         * }
         * ```
         *
         * @param options DSL builder for test environment options
         * @return A new KTestWorkflowEnvironment instance
         */
        public fun newInstance(
            options: KTestEnvironmentOptionsBuilder.() -> Unit,
        ): KTestWorkflowEnvironment {
            val javaOptions = KTestEnvironmentOptionsBuilder().apply(options).build()
            return KTestWorkflowEnvironment(TestWorkflowEnvironment.newInstance(javaOptions))
        }

        /**
         * Create a new test environment with pre-built options.
         *
         * Example:
         * ```kotlin
         * val options = KTestEnvironmentOptions.newBuilder {
         *     namespace = "test-namespace"
         * }
         * val testEnv = KTestWorkflowEnvironment.newInstance(options)
         * ```
         *
         * @param options Pre-built test environment options
         * @return A new KTestWorkflowEnvironment instance
         */
        public fun newInstance(options: KTestEnvironmentOptions): KTestWorkflowEnvironment {
            return KTestWorkflowEnvironment(
                TestWorkflowEnvironment.newInstance(options.javaOptions),
            )
        }
    }
}

/**
 * Use the test environment with automatic cleanup.
 *
 * This extension function ensures the test environment is properly closed
 * after the block completes, even if an exception is thrown.
 *
 * Example:
 * ```kotlin
 * KTestWorkflowEnvironment.newInstance().use { testEnv ->
 *     val worker = testEnv.newWorker("task-queue")
 *     worker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
 *     testEnv.start()
 *
 *     val result = testEnv.workflowClient.executeWorkflow(
 *         MyWorkflow::execute,
 *         KWorkflowOptions(taskQueue = "task-queue"),
 *         "input"
 *     )
 * } // Automatically closed
 * ```
 *
 * @param block The code block to execute with the test environment
 * @return The result of the block
 */
public inline fun <T> KTestWorkflowEnvironment.use(block: (KTestWorkflowEnvironment) -> T): T {
    return try {
        block(this)
    } finally {
        close()
    }
}
