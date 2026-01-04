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

import com.uber.m3.tally.Scope
import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.client.WorkflowClientOptions
import io.temporal.kotlin.TemporalDsl
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.testing.TestEnvironmentOptions
import io.temporal.worker.WorkerFactoryOptions
import java.time.Instant

/**
 * Kotlin DSL builder for test environment options.
 *
 * Example:
 * ```kotlin
 * val options = KTestEnvironmentOptions.newBuilder {
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
 */
@TemporalDsl
public class KTestEnvironmentOptionsBuilder internal constructor() {

    /**
     * Namespace to use for testing. Default: null (uses Java SDK default "default").
     *
     * Note: When using an in-memory test server, the namespace is automatically created.
     * Set this only if you need a specific namespace name.
     */
    public var namespace: String? = null

    /** Initial time for the workflow virtual clock. Default: current time */
    public var initialTime: Instant? = null

    /** Whether to enable time skipping. Default: true */
    public var useTimeskipping: Boolean = true

    /** Whether to use external Temporal service. Default: false (in-memory) */
    public var useExternalService: Boolean = false

    /** Target endpoint for external service. */
    public var target: String? = null

    /** Metrics scope for reporting. */
    public var metricsScope: Scope? = null

    private var workerFactoryOptionsBuilder: (WorkerFactoryOptions.Builder.() -> Unit)? = null
    private var workflowClientOptionsBuilder: (WorkflowClientOptions.Builder.() -> Unit)? = null
    private var workflowServiceStubsOptionsBuilder: (WorkflowServiceStubsOptions.Builder.() -> Unit)? = null
    private val searchAttributes: MutableMap<String, IndexedValueType> = mutableMapOf()

    /**
     * Configure WorkerFactoryOptions.
     *
     * Example:
     * ```kotlin
     * workerFactoryOptions {
     *     maxWorkflowThreadCount = 800
     * }
     * ```
     */
    public fun workerFactoryOptions(block: WorkerFactoryOptions.Builder.() -> Unit) {
        workerFactoryOptionsBuilder = block
    }

    /**
     * Configure WorkflowClientOptions.
     *
     * Example:
     * ```kotlin
     * workflowClientOptions {
     *     identity = "test-client"
     * }
     * ```
     */
    public fun workflowClientOptions(block: WorkflowClientOptions.Builder.() -> Unit) {
        workflowClientOptionsBuilder = block
    }

    /**
     * Configure WorkflowServiceStubsOptions.
     *
     * Example:
     * ```kotlin
     * workflowServiceStubsOptions {
     *     rpcTimeout = Duration.ofSeconds(30)
     * }
     * ```
     */
    public fun workflowServiceStubsOptions(block: WorkflowServiceStubsOptions.Builder.() -> Unit) {
        workflowServiceStubsOptionsBuilder = block
    }

    /**
     * Register search attributes for the test environment.
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
     * Builder for registering search attributes in the test environment.
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

    internal fun build(): TestEnvironmentOptions {
        val builder = TestEnvironmentOptions.newBuilder()

        // Only set WorkflowClientOptions if namespace or custom options are configured
        // When workflowClientOptions is null, the Java SDK handles namespace internally
        // and everything works correctly. Setting explicit options can break this.
        if (namespace != null || workflowClientOptionsBuilder != null) {
            val clientOptions = WorkflowClientOptions.newBuilder().apply {
                namespace?.let { setNamespace(it) }
                workflowClientOptionsBuilder?.invoke(this)
            }.build()
            builder.setWorkflowClientOptions(clientOptions)
        }

        // Apply WorkerFactoryOptions if configured
        workerFactoryOptionsBuilder?.let { block ->
            builder.setWorkerFactoryOptions(
                WorkerFactoryOptions.newBuilder().apply(block).build(),
            )
        }

        // Apply WorkflowServiceStubsOptions if configured
        workflowServiceStubsOptionsBuilder?.let { block ->
            builder.setWorkflowServiceStubsOptions(
                WorkflowServiceStubsOptions.newBuilder().apply(block).build(),
            )
        }

        // Apply simple options
        initialTime?.let { builder.setInitialTime(it) }
        builder.setUseTimeskipping(useTimeskipping)
        builder.setUseExternalService(useExternalService)
        target?.let { builder.setTarget(it) }
        metricsScope?.let { builder.setMetricsScope(it) }

        // Register search attributes
        searchAttributes.forEach { (name, type) ->
            builder.registerSearchAttribute(name, type)
        }

        return builder.build()
    }
}

/**
 * Immutable configuration for test environments.
 *
 * Use [newBuilder] to create an instance with DSL configuration,
 * or [getDefaultInstance] for default options.
 *
 * Example:
 * ```kotlin
 * val options = KTestEnvironmentOptions.newBuilder {
 *     namespace = "test-namespace"
 *     useTimeskipping = true
 * }
 *
 * val testEnv = KTestWorkflowEnvironment.newInstance(options)
 * ```
 */
public class KTestEnvironmentOptions private constructor(
    internal val javaOptions: TestEnvironmentOptions,
) {

    public companion object {
        /**
         * Create options using DSL builder.
         *
         * @param block Configuration block for the options builder
         * @return A new [KTestEnvironmentOptions] instance
         */
        public fun newBuilder(block: KTestEnvironmentOptionsBuilder.() -> Unit = {}): KTestEnvironmentOptions {
            return KTestEnvironmentOptions(
                KTestEnvironmentOptionsBuilder().apply(block).build(),
            )
        }

        /**
         * Get default options.
         *
         * @return A [KTestEnvironmentOptions] instance with default settings
         */
        public fun getDefaultInstance(): KTestEnvironmentOptions {
            return KTestEnvironmentOptions(TestEnvironmentOptions.getDefaultInstance())
        }
    }
}
