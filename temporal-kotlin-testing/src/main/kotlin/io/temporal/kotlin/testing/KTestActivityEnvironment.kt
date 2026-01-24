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

@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin.testing

import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.client.WorkflowClientOptions
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.internal.converters.KOptionsConverters
import io.temporal.testing.TestActivityEnvironment
import io.temporal.testing.TestEnvironmentOptions
import io.temporal.workflow.Functions
import java.io.Closeable
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Type
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3
import kotlin.reflect.KFunction4
import kotlin.reflect.KFunction5
import kotlin.reflect.KSuspendFunction1
import kotlin.reflect.KSuspendFunction2
import kotlin.reflect.KSuspendFunction3
import kotlin.reflect.KSuspendFunction4
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.javaMethod

/**
 * Kotlin test environment for activity unit testing.
 *
 * Supports testing both regular and suspend activity implementations
 * in isolation without needing workflows.
 *
 * Example:
 * ```kotlin
 * val activityEnv = KTestActivityEnvironment.newInstance()
 * activityEnv.registerActivitiesImplementations(MyActivitiesImpl())
 *
 * val result = activityEnv.executeActivity(
 *     MyActivities::doSomething,
 *     KActivityOptions(startToCloseTimeout = 30.seconds),
 *     "input"
 * )
 *
 * assertEquals("expected", result)
 * activityEnv.close()
 * ```
 */
public class KTestActivityEnvironment private constructor(
    private val testEnvironment: TestActivityEnvironment,
) : Closeable {

    // ========== Commit 19: Registration Methods ==========

    /**
     * Register activity implementations.
     *
     * Example:
     * ```kotlin
     * activityEnv.registerActivitiesImplementations(
     *     MyActivitiesImpl(),
     *     AnotherActivitiesImpl()
     * )
     * ```
     *
     * @param activities Activity implementation instances to register
     */
    public fun registerActivitiesImplementations(vararg activities: Any) {
        testEnvironment.registerActivitiesImplementations(*activities)
    }

    // ========== Commit 19: Lifecycle ==========

    /**
     * Close the test activity environment and release resources.
     */
    override fun close() {
        testEnvironment.close()
    }

    // ========== Commit 20: Execute Activity (0-2 args) ==========

    /**
     * Execute an activity with no arguments.
     *
     * Example:
     * ```kotlin
     * val result = activityEnv.executeActivity(
     *     MyActivities::getStatus,
     *     KActivityOptions(startToCloseTimeout = 30.seconds)
     * )
     * ```
     *
     * @param activity Method reference to the activity function
     * @param options Activity options including timeout configuration
     * @return The result of the activity execution
     */
    public fun <T, R> executeActivity(
        activity: KFunction1<T, R>,
        options: KActivityOptions,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub) as R }
    }

    /**
     * Execute an activity with one argument.
     *
     * Example:
     * ```kotlin
     * val result = activityEnv.executeActivity(
     *     MyActivities::processItem,
     *     KActivityOptions(startToCloseTimeout = 30.seconds),
     *     "item-123"
     * )
     * ```
     *
     * @param activity Method reference to the activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, R> executeActivity(
        activity: KFunction2<T, A1, R>,
        options: KActivityOptions,
        arg1: A1,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1) as R }
    }

    /**
     * Execute an activity with two arguments.
     *
     * Example:
     * ```kotlin
     * val result = activityEnv.executeActivity(
     *     MyActivities::combine,
     *     KActivityOptions(startToCloseTimeout = 30.seconds),
     *     "value1",
     *     "value2"
     * )
     * ```
     *
     * @param activity Method reference to the activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, A2, R> executeActivity(
        activity: KFunction3<T, A1, A2, R>,
        options: KActivityOptions,
        arg1: A1,
        arg2: A2,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1, arg2) as R }
    }

    // ========== Commit 21: Execute Activity (3+ args) ==========

    /**
     * Execute an activity with three arguments.
     *
     * @param activity Method reference to the activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @param arg3 Third argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, A2, A3, R> executeActivity(
        activity: KFunction4<T, A1, A2, A3, R>,
        options: KActivityOptions,
        arg1: A1,
        arg2: A2,
        arg3: A3,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1, arg2, arg3) as R }
    }

    /**
     * Execute an activity with four arguments.
     *
     * @param activity Method reference to the activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @param arg3 Third argument to pass to the activity
     * @param arg4 Fourth argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, A2, A3, A4, R> executeActivity(
        activity: KFunction5<T, A1, A2, A3, A4, R>,
        options: KActivityOptions,
        arg1: A1,
        arg2: A2,
        arg3: A3,
        arg4: A4,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1, arg2, arg3, arg4) as R }
    }

    // ========== Commit 22: Execute Suspend Activity (0-3 args) ==========

    /**
     * Execute a suspend activity with no arguments.
     *
     * Example:
     * ```kotlin
     * val result = activityEnv.executeActivity(
     *     MySuspendActivities::fetchStatus,
     *     KActivityOptions(startToCloseTimeout = 30.seconds)
     * )
     * ```
     *
     * @param activity Method reference to the suspend activity function
     * @param options Activity options including timeout configuration
     * @return The result of the activity execution
     */
    @JvmName("executeSuspendActivity0")
    public suspend fun <T, R> executeActivity(
        activity: KSuspendFunction1<T, R>,
        options: KActivityOptions,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.callSuspend(stub) as R }
    }

    /**
     * Execute a suspend activity with one argument.
     *
     * Example:
     * ```kotlin
     * val result = activityEnv.executeActivity(
     *     MySuspendActivities::fetchData,
     *     KActivityOptions(startToCloseTimeout = 30.seconds),
     *     "https://example.com/api"
     * )
     * ```
     *
     * @param activity Method reference to the suspend activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @return The result of the activity execution
     */
    @JvmName("executeSuspendActivity1")
    public suspend fun <T, A1, R> executeActivity(
        activity: KSuspendFunction2<T, A1, R>,
        options: KActivityOptions,
        arg1: A1,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.callSuspend(stub, arg1) as R }
    }

    /**
     * Execute a suspend activity with two arguments.
     *
     * @param activity Method reference to the suspend activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @return The result of the activity execution
     */
    @JvmName("executeSuspendActivity2")
    public suspend fun <T, A1, A2, R> executeActivity(
        activity: KSuspendFunction3<T, A1, A2, R>,
        options: KActivityOptions,
        arg1: A1,
        arg2: A2,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.callSuspend(stub, arg1, arg2) as R }
    }

    /**
     * Execute a suspend activity with three arguments.
     *
     * @param activity Method reference to the suspend activity function
     * @param options Activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @param arg3 Third argument to pass to the activity
     * @return The result of the activity execution
     */
    @JvmName("executeSuspendActivity3")
    public suspend fun <T, A1, A2, A3, R> executeActivity(
        activity: KSuspendFunction4<T, A1, A2, A3, R>,
        options: KActivityOptions,
        arg1: A1,
        arg2: A2,
        arg3: A3,
    ): R {
        val stub = createActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.callSuspend(stub, arg1, arg2, arg3) as R }
    }

    // ========== Commit 23: Execute Local Activity (0-3 args) ==========

    /**
     * Execute a local activity with no arguments.
     *
     * Local activities execute in the same process as the workflow,
     * avoiding the overhead of scheduling through the Temporal service.
     *
     * @param activity Method reference to the activity function
     * @param options Local activity options including timeout configuration
     * @return The result of the activity execution
     */
    public fun <T, R> executeLocalActivity(
        activity: KFunction1<T, R>,
        options: KLocalActivityOptions,
    ): R {
        val stub = createLocalActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub) as R }
    }

    /**
     * Execute a local activity with one argument.
     *
     * @param activity Method reference to the activity function
     * @param options Local activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, R> executeLocalActivity(
        activity: KFunction2<T, A1, R>,
        options: KLocalActivityOptions,
        arg1: A1,
    ): R {
        val stub = createLocalActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1) as R }
    }

    /**
     * Execute a local activity with two arguments.
     *
     * @param activity Method reference to the activity function
     * @param options Local activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, A2, R> executeLocalActivity(
        activity: KFunction3<T, A1, A2, R>,
        options: KLocalActivityOptions,
        arg1: A1,
        arg2: A2,
    ): R {
        val stub = createLocalActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1, arg2) as R }
    }

    /**
     * Execute a local activity with three arguments.
     *
     * @param activity Method reference to the activity function
     * @param options Local activity options including timeout configuration
     * @param arg1 First argument to pass to the activity
     * @param arg2 Second argument to pass to the activity
     * @param arg3 Third argument to pass to the activity
     * @return The result of the activity execution
     */
    public fun <T, A1, A2, A3, R> executeLocalActivity(
        activity: KFunction4<T, A1, A2, A3, R>,
        options: KLocalActivityOptions,
        arg1: A1,
        arg2: A2,
        arg3: A3,
    ): R {
        val stub = createLocalActivityStub<T>(activity, KOptionsConverters.toJava(options))
        @Suppress("UNCHECKED_CAST")
        return invokeUnwrapped { activity.call(stub, arg1, arg2, arg3) as R }
    }

    // ========== Commit 24: Heartbeat Testing ==========

    /**
     * Set heartbeat details for the next activity execution.
     * Simulates activity retry with heartbeat checkpoint.
     *
     * This is useful for testing activity implementations that use heartbeating
     * to report progress. Call this method before executing the activity to
     * simulate a retry scenario where the activity was interrupted and is
     * resuming from a checkpoint.
     *
     * Example:
     * ```kotlin
     * // Simulate retry from checkpoint
     * activityEnv.setHeartbeatDetails(ProgressCheckpoint(processedItems = 50))
     *
     * // Activity can retrieve this via Activity.getExecutionContext().getHeartbeatDetails()
     * val result = activityEnv.executeActivity(
     *     MyActivities::processWithProgress,
     *     options,
     *     inputData
     * )
     * ```
     *
     * @param details The heartbeat details to set (must be serializable)
     */
    public fun <T> setHeartbeatDetails(details: T) {
        testEnvironment.setHeartbeatDetails(details)
    }

    /**
     * Set a listener for activity heartbeats.
     *
     * This allows tests to verify that activities are heartbeating correctly
     * and with the expected details.
     *
     * Example:
     * ```kotlin
     * val heartbeatDetails = mutableListOf<Int>()
     *
     * activityEnv.setActivityHeartbeatListener<Int> { progress ->
     *     heartbeatDetails.add(progress)
     * }
     *
     * activityEnv.executeActivity(MyActivities::processItems, options, items)
     *
     * assertEquals(listOf(25, 50, 75, 100), heartbeatDetails)
     * ```
     *
     * @param listener Callback invoked for each heartbeat with the details
     */
    public inline fun <reified T> setActivityHeartbeatListener(
        noinline listener: (T) -> Unit,
    ) {
        setActivityHeartbeatListenerInternal(T::class.java, listener)
    }

    @PublishedApi
    internal fun <T> setActivityHeartbeatListenerInternal(
        detailsClass: Class<T>,
        listener: (T) -> Unit,
    ) {
        testEnvironment.setActivityHeartbeatListener(
            detailsClass,
            Functions.Proc1 { listener(it) },
        )
    }

    /**
     * Set a listener for activity heartbeats with explicit type information.
     *
     * Use this variant when you need to specify complex generic types that
     * cannot be captured via reified type parameters.
     *
     * @param detailsClass The class of the heartbeat details
     * @param detailsType The full generic type of the heartbeat details
     * @param listener Callback invoked for each heartbeat with the details
     */
    public fun <T> setActivityHeartbeatListener(
        detailsClass: Class<T>,
        detailsType: Type,
        listener: (T) -> Unit,
    ) {
        testEnvironment.setActivityHeartbeatListener(
            detailsClass,
            detailsType,
            Functions.Proc1 { listener(it) },
        )
    }

    // ========== Commit 24: Cancellation Testing ==========

    /**
     * Request cancellation of the currently executing activity.
     *
     * Cancellation is delivered on the next heartbeat. This is useful for testing
     * how activities handle cancellation requests.
     *
     * Example:
     * ```kotlin
     * // Set up a listener that requests cancellation after first heartbeat
     * var heartbeatCount = 0
     * activityEnv.setActivityHeartbeatListener<Int> { _ ->
     *     heartbeatCount++
     *     if (heartbeatCount >= 1) {
     *         activityEnv.requestCancelActivity()
     *     }
     * }
     *
     * // Activity should throw ActivityCanceledException
     * assertThrows<ActivityCanceledException> {
     *     activityEnv.executeActivity(MyActivities::longRunning, options)
     * }
     * ```
     */
    public fun requestCancelActivity() {
        testEnvironment.requestCancelActivity()
    }

    // ========== Internal Helpers ==========

    /**
     * Invokes a function and unwraps InvocationTargetException.
     *
     * KFunction.call() wraps exceptions in InvocationTargetException,
     * so we unwrap them to expose the actual exception (e.g., ActivityFailure).
     */
    private inline fun <R> invokeUnwrapped(block: () -> R): R {
        try {
            return block()
        } catch (e: InvocationTargetException) {
            throw e.cause ?: e
        }
    }

    /**
     * Creates an activity stub from a method reference.
     *
     * Extracts the declaring class from the method reference to create
     * a properly typed activity stub.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T> createActivityStub(
        activity: KFunction<*>,
        options: ActivityOptions,
    ): T {
        val activityClass = activity.javaMethod?.declaringClass
            ?: throw IllegalArgumentException(
                "Cannot determine activity interface from method reference. " +
                    "Ensure you're using a method reference like MyActivities::methodName",
            )
        return testEnvironment.newActivityStub(activityClass, options) as T
    }

    /**
     * Creates a local activity stub from a method reference.
     *
     * Extracts the declaring class from the method reference to create
     * a properly typed local activity stub.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T> createLocalActivityStub(
        activity: KFunction<*>,
        options: LocalActivityOptions,
    ): T {
        val activityClass = activity.javaMethod?.declaringClass
            ?: throw IllegalArgumentException(
                "Cannot determine activity interface from method reference. " +
                    "Ensure you're using a method reference like MyActivities::methodName",
            )
        return testEnvironment.newLocalActivityStub(activityClass, options, emptyMap()) as T
    }

    public companion object {
        /**
         * Create a new activity test environment with default options.
         *
         * Example:
         * ```kotlin
         * val activityEnv = KTestActivityEnvironment.newInstance()
         * ```
         */
        public fun newInstance(): KTestActivityEnvironment {
            return KTestActivityEnvironment(TestActivityEnvironment.newInstance())
        }

        /**
         * Create a new activity test environment with options.
         *
         * Example:
         * ```kotlin
         * val activityEnv = KTestActivityEnvironment.newInstance(
         *     KTestEnvironmentOptions(namespace = "test-namespace")
         * )
         * ```
         *
         * @param options Configuration options
         */
        public fun newInstance(options: KTestEnvironmentOptions): KTestActivityEnvironment {
            return KTestActivityEnvironment(
                TestActivityEnvironment.newInstance(buildActivityJavaOptions(options)),
            )
        }

        /**
         * Builds Java SDK TestEnvironmentOptions from Kotlin options for activity testing.
         */
        private fun buildActivityJavaOptions(options: KTestEnvironmentOptions): TestEnvironmentOptions {
            val builder = TestEnvironmentOptions.newBuilder()

            // Build WorkflowClientOptions if namespace or custom options are configured
            if (options.namespace != null || options.workflowClientOptions != null) {
                val clientOptionsBuilder = if (options.workflowClientOptions != null) {
                    options.workflowClientOptions.toBuilder()
                } else {
                    WorkflowClientOptions.newBuilder()
                }
                options.namespace?.let { clientOptionsBuilder.setNamespace(it) }
                builder.setWorkflowClientOptions(clientOptionsBuilder.build())
            }

            // Apply WorkerFactoryOptions if configured (no KotlinPlugin needed for activities)
            options.workerFactoryOptions?.let { builder.setWorkerFactoryOptions(it) }

            // Apply WorkflowServiceStubsOptions if configured
            options.workflowServiceStubsOptions?.let { builder.setWorkflowServiceStubsOptions(it) }

            // Apply simple options
            options.initialTime?.let { builder.setInitialTime(it) }
            builder.setUseTimeskipping(options.useTimeskipping)
            builder.setUseExternalService(options.useExternalService)
            options.target?.let { builder.setTarget(it) }
            options.metricsScope?.let { builder.setMetricsScope(it) }

            // Register search attributes
            options.searchAttributes.forEach { (name, type) ->
                builder.registerSearchAttribute(name, type)
            }

            return builder.build()
        }
    }
}
