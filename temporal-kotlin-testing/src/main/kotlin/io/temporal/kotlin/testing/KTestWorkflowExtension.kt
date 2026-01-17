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

import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.client.WorkflowClientOptions
import io.temporal.kotlin.TemporalDsl
import io.temporal.kotlin.activity.KActivityRegistry
import io.temporal.kotlin.activity.KDynamicActivityHandler
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.worker.KWorker
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import io.temporal.worker.WorkflowImplementationOptions
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.junit.jupiter.api.extension.TestWatcher
import org.junit.platform.commons.support.AnnotationSupport
import java.lang.reflect.Constructor
import java.time.Instant

/**
 * JUnit 5 extension for testing Temporal workflows with Kotlin-idiomatic APIs.
 *
 * This extension simplifies workflow testing by:
 * - Automatically creating an isolated test environment for each test
 * - Registering workflow and activity implementations
 * - Injecting [KTestWorkflowEnvironment], [KClient], and [KWorker] into test methods
 * - Providing diagnostics on test failure
 *
 * This extension only supports suspend workflows and promotes the method reference
 * pattern for workflow execution. Use `runTest` from kotlinx-coroutines-test for
 * suspend test functions.
 *
 * Example:
 * ```kotlin
 * class MyWorkflowTest {
 *     companion object {
 *         @JvmField
 *         @RegisterExtension
 *         val testWorkflow = kTestWorkflowExtension {
 *             registerWorkflowImplementationTypes<MyWorkflowImpl>()
 *             setActivityImplementations(MyActivitiesImpl())
 *         }
 *     }
 *
 *     @Test
 *     fun `test workflow execution`(
 *         client: KClient,
 *         options: KWorkflowOptions,
 *     ) = runTest {
 *         val result = client.executeWorkflow(
 *             MyWorkflow::execute,
 *             options.copy(workflowId = "test-${UUID.randomUUID()}"),
 *             "input"
 *         )
 *         assertEquals("expected", result)
 *     }
 *
 *     @Test
 *     @WorkflowInitialTime("2024-06-15T12:00:00Z")
 *     fun `test workflow with specific initial time`(
 *         client: KClient,
 *         options: KWorkflowOptions,
 *     ) = runTest {
 *         // Test runs with June 15, 2024 as initial time
 *         val result = client.executeWorkflow(MyWorkflow::execute, options.copy(workflowId = "test"), "input")
 *     }
 * }
 * ```
 */
public class KTestWorkflowExtension private constructor(
    private val config: ExtensionConfig,
) : ParameterResolver, TestWatcher, BeforeEachCallback, AfterEachCallback {

    // ========== Commit 13: Configuration data class ==========

    private data class ExtensionConfig(
        val namespace: String?,
        val workflowTypes: Map<Class<*>, WorkflowImplementationOptions>,
        val activityImplementations: Array<Any>,
        val suspendActivityImplementations: Array<Any>,
        val nexusServiceImplementations: Array<Any>,
        val workerOptions: WorkerOptions,
        val workerFactoryOptions: WorkerFactoryOptions?,
        val workflowClientOptions: WorkflowClientOptions?,
        val useExternalService: Boolean,
        val target: String?,
        val doNotStart: Boolean,
        val initialTimeMillis: Long,
        val useTimeskipping: Boolean,
        val searchAttributes: Map<String, IndexedValueType>,
    ) {
        // Override equals/hashCode because arrays are compared by reference by default
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ExtensionConfig) return false

            return namespace == other.namespace &&
                workflowTypes == other.workflowTypes &&
                activityImplementations.contentEquals(other.activityImplementations) &&
                suspendActivityImplementations.contentEquals(other.suspendActivityImplementations) &&
                nexusServiceImplementations.contentEquals(other.nexusServiceImplementations) &&
                workerOptions == other.workerOptions &&
                workerFactoryOptions == other.workerFactoryOptions &&
                workflowClientOptions == other.workflowClientOptions &&
                useExternalService == other.useExternalService &&
                target == other.target &&
                doNotStart == other.doNotStart &&
                initialTimeMillis == other.initialTimeMillis &&
                useTimeskipping == other.useTimeskipping &&
                searchAttributes == other.searchAttributes
        }

        override fun hashCode(): Int {
            var result = namespace?.hashCode() ?: 0
            result = 31 * result + workflowTypes.hashCode()
            result = 31 * result + activityImplementations.contentHashCode()
            result = 31 * result + suspendActivityImplementations.contentHashCode()
            result = 31 * result + nexusServiceImplementations.contentHashCode()
            result = 31 * result + workerOptions.hashCode()
            result = 31 * result + (workerFactoryOptions?.hashCode() ?: 0)
            result = 31 * result + (workflowClientOptions?.hashCode() ?: 0)
            result = 31 * result + useExternalService.hashCode()
            result = 31 * result + (target?.hashCode() ?: 0)
            result = 31 * result + doNotStart.hashCode()
            result = 31 * result + initialTimeMillis.hashCode()
            result = 31 * result + useTimeskipping.hashCode()
            result = 31 * result + searchAttributes.hashCode()
            return result
        }
    }

    /**
     * Set of parameter types that can be resolved by this extension.
     *
     * Only Kotlin-idiomatic types are supported. Use [KClient] with method references
     * (e.g., `client.executeWorkflow(MyWorkflow::execute, ...)`) for workflow execution.
     */
    private val supportedParameterTypes: Set<Class<*>> = setOf(
        KTestWorkflowEnvironment::class.java,
        KClient::class.java,
        KWorkflowOptions::class.java,
        KWorker::class.java,
    )

    // ========== Parameter resolution ==========

    override fun supportsParameter(
        parameterContext: ParameterContext,
        extensionContext: ExtensionContext,
    ): Boolean {
        val parameter = parameterContext.parameter

        // Don't resolve constructor parameters
        if (parameter.declaringExecutable is Constructor<*>) return false

        return supportedParameterTypes.contains(parameter.type)
    }

    override fun resolveParameter(
        parameterContext: ParameterContext,
        extensionContext: ExtensionContext,
    ): Any {
        val parameterType = parameterContext.parameter.type
        val store = getStore(extensionContext)

        return when (parameterType) {
            KTestWorkflowEnvironment::class.java -> getTestEnvironment(store)
            KClient::class.java -> getTestEnvironment(store).workflowClient
            KWorkflowOptions::class.java -> getWorkflowOptions(store)
            KWorker::class.java -> getKWorker(store)
            else -> throw IllegalArgumentException(
                "Unsupported parameter type: ${parameterType.name}. " +
                    "Supported types: KTestWorkflowEnvironment, KClient, KWorkflowOptions, KWorker",
            )
        }
    }

    // ========== Commit 15: Lifecycle callbacks ==========

    override fun beforeEach(context: ExtensionContext) {
        // Check for @WorkflowInitialTime annotation (Commit 18)
        val currentInitialTimeMillis = AnnotationSupport.findAnnotation(
            context.element,
            WorkflowInitialTime::class.java,
        ).map { annotation ->
            Instant.parse(annotation.value).toEpochMilli()
        }.orElse(config.initialTimeMillis)

        // Build test environment options
        val testEnvOptions = io.temporal.testing.TestEnvironmentOptions.newBuilder().apply {
            setUseExternalService(config.useExternalService)
            setUseTimeskipping(config.useTimeskipping)
            config.target?.let { setTarget(it) }
            if (currentInitialTimeMillis > 0) {
                setInitialTimeMillis(currentInitialTimeMillis)
            }

            // Only set WorkflowClientOptions if namespace or custom options are configured
            // When workflowClientOptions is null and namespace is null, the Java SDK handles
            // namespace internally and time skipping works correctly.
            // Setting explicit options can break time skipping.
            if (config.namespace != null || config.workflowClientOptions != null) {
                val clientOptions = (
                    config.workflowClientOptions?.let {
                        WorkflowClientOptions.newBuilder(it)
                    } ?: WorkflowClientOptions.newBuilder()
                    ).apply {
                    config.namespace?.let { setNamespace(it) }
                }.build()
                setWorkflowClientOptions(clientOptions)
            }

            // Configure worker factory options if provided
            config.workerFactoryOptions?.let { setWorkerFactoryOptions(it) }

            // Register search attributes
            config.searchAttributes.forEach { (name, type) ->
                registerSearchAttribute(name, type)
            }
        }.build()

        // Create Java test environment
        val javaTestEnv = TestWorkflowEnvironment.newInstance(testEnvOptions)

        // Create unified activity registry shared between test environment and worker
        val activityRegistry = KActivityRegistry()
        val testEnvironment = createKTestWorkflowEnvironment(javaTestEnv, activityRegistry)

        // Generate unique task queue per test
        val taskQueue = generateTaskQueue(context)
        val worker = javaTestEnv.newWorker(taskQueue, config.workerOptions)

        // Create KWorker (note: for test mocking, activities are registered via the registry/dynamic handler)
        val kWorker = KWorker(worker)

        // Register workflows
        config.workflowTypes.forEach { (workflowType, options) ->
            worker.registerWorkflowImplementationTypes(options, workflowType)
        }

        // Register the unified dynamic activity handler
        // This single handler routes all Kotlin activity calls through the shared registry
        val dynamicHandler = KDynamicActivityHandler(activityRegistry)
        worker.registerActivitiesImplementations(dynamicHandler)

        // Register regular activities via the unified registry
        if (config.activityImplementations.isNotEmpty()) {
            config.activityImplementations.forEach { activity ->
                activityRegistry.register(activity)
            }
        }

        // Register suspend activities via the unified registry
        config.suspendActivityImplementations.forEach { activity ->
            activityRegistry.register(activity)
        }

        // Register Nexus services
        if (config.nexusServiceImplementations.isNotEmpty()) {
            worker.registerNexusServiceImplementation(*config.nexusServiceImplementations)
        }

        // Start unless doNotStart is set
        if (!config.doNotStart) {
            javaTestEnv.start()
        }

        // Store in extension context
        val store = getStore(context)
        store.put(TEST_ENVIRONMENT_KEY, testEnvironment)
        store.put(KWORKER_KEY, kWorker)
        store.put(WORKFLOW_OPTIONS_KEY, KWorkflowOptions(taskQueue = taskQueue))
    }

    override fun afterEach(context: ExtensionContext) {
        val store = getStore(context)
        val testEnv = store.get(TEST_ENVIRONMENT_KEY, KTestWorkflowEnvironment::class.java)
        testEnv?.close()
    }

    // ========== Commit 18: Test failure diagnostics ==========

    override fun testFailed(context: ExtensionContext, cause: Throwable) {
        val store = getStore(context)
        val testEnv = store.get(TEST_ENVIRONMENT_KEY, KTestWorkflowEnvironment::class.java)
        testEnv?.let {
            System.err.println("=== Workflow Execution Histories ===")
            System.err.println(it.getDiagnostics())
            System.err.println("=== End of Diagnostics ===")
        }
    }

    // ========== Internal helpers ==========

    private fun getStore(context: ExtensionContext): ExtensionContext.Store {
        val namespace = ExtensionContext.Namespace.create(
            KTestWorkflowExtension::class.java,
            context.requiredTestMethod,
        )
        return context.getStore(namespace)
    }

    private fun getTestEnvironment(store: ExtensionContext.Store): KTestWorkflowEnvironment {
        return store.get(TEST_ENVIRONMENT_KEY, KTestWorkflowEnvironment::class.java)
            ?: throw IllegalStateException(
                "Test environment not initialized. " +
                    "Ensure the extension is properly registered.",
            )
    }

    private fun getKWorker(store: ExtensionContext.Store): KWorker {
        return store.get(KWORKER_KEY, KWorker::class.java)
            ?: throw IllegalStateException(
                "Worker not initialized. " +
                    "Ensure the extension is properly registered.",
            )
    }

    private fun getWorkflowOptions(store: ExtensionContext.Store): KWorkflowOptions {
        return store.get(WORKFLOW_OPTIONS_KEY, KWorkflowOptions::class.java)
            ?: throw IllegalStateException(
                "Workflow options not initialized. " +
                    "Ensure the extension is properly registered.",
            )
    }

    /**
     * Generate a unique task queue name for each test to ensure isolation.
     */
    private fun generateTaskQueue(context: ExtensionContext): String {
        // Clean up display name to create a valid task queue name
        val testName = context.displayName
            .replace(Regex("[^a-zA-Z0-9_-]"), "_")
            .take(50) // Limit length

        return "WorkflowTest-$testName-${context.uniqueId.hashCode().toUInt()}"
    }

    /**
     * Creates a KTestWorkflowEnvironment wrapping the Java TestWorkflowEnvironment.
     */
    private fun createKTestWorkflowEnvironment(
        javaTestEnv: TestWorkflowEnvironment,
        activityRegistry: KActivityRegistry,
    ): KTestWorkflowEnvironment {
        // Use reflection to access the private constructor
        val constructor = KTestWorkflowEnvironment::class.java.getDeclaredConstructor(
            TestWorkflowEnvironment::class.java,
            KActivityRegistry::class.java,
        )
        constructor.isAccessible = true
        return constructor.newInstance(javaTestEnv, activityRegistry)
    }

    // ========== Commit 13: Companion object with factory ==========

    public companion object {
        private const val TEST_ENVIRONMENT_KEY = "testEnvironment"
        private const val KWORKER_KEY = "kWorker"
        private const val WORKFLOW_OPTIONS_KEY = "workflowOptions"

        /**
         * Create a new extension builder.
         *
         * Example:
         * ```kotlin
         * val extension = KTestWorkflowExtension.newBuilder()
         *     .registerWorkflowImplementationTypes<MyWorkflowImpl>()
         *     .setActivityImplementations(MyActivitiesImpl())
         *     .build()
         * ```
         */
        @JvmStatic
        public fun newBuilder(): Builder = Builder()
    }

    // ========== Commits 13-14: Builder class ==========

    /**
     * Builder for [KTestWorkflowExtension].
     *
     * Provides a fluent API for configuring the test extension with workflow
     * implementations, activity implementations, and test environment options.
     */
    @TemporalDsl
    public class Builder internal constructor() {
        // Commit 13: Basic properties
        private var _namespace: String? = null

        @PublishedApi
        internal val workflowTypes = mutableMapOf<Class<*>, WorkflowImplementationOptions>()
        private var activityImplementations: Array<Any> = emptyArray()
        private var suspendActivityImplementations: Array<Any> = emptyArray()
        private var nexusServiceImplementations: Array<Any> = emptyArray()

        // Commit 14: Service configuration
        private var workerOptions: WorkerOptions = WorkerOptions.getDefaultInstance()
        private var workerFactoryOptions: WorkerFactoryOptions? = null
        private var workflowClientOptions: WorkflowClientOptions? = null
        private var useExternalService: Boolean = false
        private var target: String? = null
        private var _doNotStart: Boolean = false
        private var _initialTime: Instant? = null
        private var _useTimeskipping: Boolean = true
        private val searchAttributes = mutableMapOf<String, IndexedValueType>()

        // ========== Commit 13: Basic properties ==========

        /**
         * The namespace to use for the test environment.
         * Default: null (uses Java SDK default "default")
         *
         * Note: When using an in-memory test server, the namespace is automatically created.
         * Set this only if you need a specific namespace name.
         */
        public var namespace: String?
            get() = _namespace
            set(value) {
                _namespace = value
            }

        /**
         * Initial time for the workflow virtual clock.
         * If not set, the current system time is used.
         */
        public var initialTime: Instant?
            get() = _initialTime
            set(value) {
                _initialTime = value
            }

        /**
         * Whether to enable time skipping in the test environment.
         * Default: true
         *
         * When enabled, workflow timers and sleeps complete instantly.
         */
        public var useTimeskipping: Boolean
            get() = _useTimeskipping
            set(value) {
                _useTimeskipping = value
            }

        /**
         * Whether to defer starting the workers.
         * Default: false
         *
         * If true, you must call start() on the test environment manually.
         */
        public var doNotStart: Boolean
            get() = _doNotStart
            set(value) {
                _doNotStart = value
            }

        // ========== Commit 13: Workflow registration ==========

        /**
         * Register workflow implementation types using reified generics.
         *
         * Example:
         * ```kotlin
         * registerWorkflowImplementationTypes<MyWorkflowImpl>()
         * ```
         */
        public inline fun <reified T : Any> registerWorkflowImplementationTypes() {
            workflowTypes[T::class.java] = WorkflowImplementationOptions.newBuilder().build()
        }

        /**
         * Register workflow implementation types with options DSL.
         *
         * Example:
         * ```kotlin
         * registerWorkflowImplementationTypes<MyWorkflowImpl> {
         *     setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
         * }
         * ```
         */
        public inline fun <reified T : Any> registerWorkflowImplementationTypes(
            options: WorkflowImplementationOptions.Builder.() -> Unit,
        ) {
            workflowTypes[T::class.java] = WorkflowImplementationOptions.newBuilder()
                .apply(options)
                .build()
        }

        /**
         * Register workflow implementation types from Java Class objects.
         *
         * Example:
         * ```kotlin
         * registerWorkflowImplementationTypes(
         *     MyWorkflowImpl::class.java,
         *     AnotherWorkflowImpl::class.java
         * )
         * ```
         */
        public fun registerWorkflowImplementationTypes(vararg classes: Class<*>) {
            val defaultOptions = WorkflowImplementationOptions.newBuilder().build()
            classes.forEach { workflowTypes[it] = defaultOptions }
        }

        /**
         * Register workflow implementation types with options.
         *
         * Example:
         * ```kotlin
         * val options = WorkflowImplementationOptions.newBuilder()
         *     .setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
         *     .build()
         * registerWorkflowImplementationTypes(options, MyWorkflowImpl::class.java)
         * ```
         */
        public fun registerWorkflowImplementationTypes(
            options: WorkflowImplementationOptions,
            vararg classes: Class<*>,
        ) {
            classes.forEach { workflowTypes[it] = options }
        }

        // ========== Commit 13: Activity registration ==========

        /**
         * Set regular activity implementations.
         *
         * Example:
         * ```kotlin
         * setActivityImplementations(
         *     MyActivitiesImpl(),
         *     AnotherActivitiesImpl()
         * )
         * ```
         */
        public fun setActivityImplementations(vararg activities: Any) {
            activityImplementations = arrayOf(*activities)
        }

        /**
         * Set suspend activity implementations.
         *
         * Suspend activities are automatically wrapped for Temporal execution.
         *
         * Example:
         * ```kotlin
         * setSuspendActivityImplementations(MySuspendActivitiesImpl())
         * ```
         */
        public fun setSuspendActivityImplementations(vararg activities: Any) {
            suspendActivityImplementations = arrayOf(*activities)
        }

        /**
         * Set Nexus service implementations.
         *
         * Example:
         * ```kotlin
         * setNexusServiceImplementations(MyNexusServiceImpl())
         * ```
         */
        public fun setNexusServiceImplementations(vararg services: Any) {
            nexusServiceImplementations = arrayOf(*services)
        }

        // ========== Commit 14: Service configuration ==========

        /**
         * Configure worker options with DSL.
         *
         * Example:
         * ```kotlin
         * workerOptions {
         *     maxConcurrentActivityExecutionSize = 100
         *     maxConcurrentWorkflowTaskExecutionSize = 50
         * }
         * ```
         */
        public fun workerOptions(block: WorkerOptions.Builder.() -> Unit) {
            workerOptions = WorkerOptions.newBuilder().apply(block).build()
        }

        /**
         * Configure worker factory options with DSL.
         *
         * Example:
         * ```kotlin
         * workerFactoryOptions {
         *     maxWorkflowThreadCount = 800
         * }
         * ```
         */
        public fun workerFactoryOptions(block: WorkerFactoryOptions.Builder.() -> Unit) {
            workerFactoryOptions = WorkerFactoryOptions.newBuilder().apply(block).build()
        }

        /**
         * Configure workflow client options with DSL.
         *
         * Example:
         * ```kotlin
         * workflowClientOptions {
         *     identity = "test-client"
         * }
         * ```
         */
        public fun workflowClientOptions(block: WorkflowClientOptions.Builder.() -> Unit) {
            workflowClientOptions = WorkflowClientOptions.newBuilder().apply(block).build()
        }

        /**
         * Use the internal in-memory Temporal service (default).
         *
         * This resets any external service configuration.
         */
        public fun useInternalService() {
            useExternalService = false
            target = null
        }

        /**
         * Use an external Temporal service.
         *
         * The service address should be configured via environment
         * variables or system properties (e.g., TEMPORAL_ADDRESS).
         */
        public fun useExternalService() {
            useExternalService = true
            target = null
        }

        /**
         * Use an external Temporal service at the specified address.
         *
         * Example:
         * ```kotlin
         * useExternalService("localhost:7233")
         * ```
         *
         * @param address The Temporal service address (host:port)
         */
        public fun useExternalService(address: String) {
            useExternalService = true
            target = address
        }

        /**
         * Configure search attributes for the test environment.
         *
         * Example:
         * ```kotlin
         * searchAttributes {
         *     register("CustomKeyword", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
         *     register("CustomInt", IndexedValueType.INDEXED_VALUE_TYPE_INT)
         * }
         * ```
         */
        public fun searchAttributes(block: SearchAttributesBuilder.() -> Unit) {
            SearchAttributesBuilder(searchAttributes).apply(block)
        }

        /**
         * Builder for registering search attributes.
         */
        @TemporalDsl
        public class SearchAttributesBuilder internal constructor(
            private val attributes: MutableMap<String, IndexedValueType>,
        ) {
            /**
             * Register a search attribute with the given name and type.
             *
             * @param name The search attribute name
             * @param type The search attribute type
             */
            public fun register(name: String, type: IndexedValueType) {
                attributes[name] = type
            }
        }

        // ========== Build method ==========

        /**
         * Build the [KTestWorkflowExtension] with the configured options.
         *
         * @return A new [KTestWorkflowExtension] instance
         */
        public fun build(): KTestWorkflowExtension {
            return KTestWorkflowExtension(
                ExtensionConfig(
                    namespace = _namespace,
                    workflowTypes = workflowTypes.toMap(),
                    activityImplementations = activityImplementations,
                    suspendActivityImplementations = suspendActivityImplementations,
                    nexusServiceImplementations = nexusServiceImplementations,
                    workerOptions = workerOptions,
                    workerFactoryOptions = workerFactoryOptions,
                    workflowClientOptions = workflowClientOptions,
                    useExternalService = useExternalService,
                    target = target,
                    doNotStart = _doNotStart,
                    initialTimeMillis = _initialTime?.toEpochMilli() ?: 0,
                    useTimeskipping = _useTimeskipping,
                    searchAttributes = searchAttributes.toMap(),
                ),
            )
        }
    }
}

/**
 * DSL function to create a [KTestWorkflowExtension].
 *
 * This is the recommended way to create a test workflow extension in Kotlin.
 * Use `runTest` from kotlinx-coroutines-test for suspend test functions.
 *
 * Example:
 * ```kotlin
 * class MyWorkflowTest {
 *     companion object {
 *         @JvmField
 *         @RegisterExtension
 *         val testWorkflow = kTestWorkflowExtension {
 *             registerWorkflowImplementationTypes<MyWorkflowImpl>()
 *             setActivityImplementations(MyActivitiesImpl())
 *
 *             workerOptions {
 *                 maxConcurrentActivityExecutionSize = 100
 *             }
 *
 *             searchAttributes {
 *                 register("CustomKeyword", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
 *             }
 *         }
 *     }
 *
 *     @Test
 *     fun `test workflow`(
 *         client: KClient,
 *         options: KWorkflowOptions,
 *     ) = runTest {
 *         val result = client.executeWorkflow(
 *             MyWorkflow::execute,
 *             options.copy(workflowId = "test-${UUID.randomUUID()}"),
 *             "input"
 *         )
 *         assertEquals("expected", result)
 *     }
 * }
 * ```
 *
 * @param block Configuration block for the extension builder
 * @return A new [KTestWorkflowExtension] instance
 */
public fun kTestWorkflowExtension(
    block: KTestWorkflowExtension.Builder.() -> Unit,
): KTestWorkflowExtension {
    return KTestWorkflowExtension.newBuilder().apply(block).build()
}
