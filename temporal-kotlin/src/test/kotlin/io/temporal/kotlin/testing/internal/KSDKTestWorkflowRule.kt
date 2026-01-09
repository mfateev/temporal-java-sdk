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

package io.temporal.kotlin.testing.internal

import com.uber.m3.tally.Scope
import io.temporal.api.enums.v1.EventType
import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.api.history.v1.History
import io.temporal.api.history.v1.HistoryEvent
import io.temporal.api.nexus.v1.Endpoint
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowStub
import io.temporal.common.SearchAttributeKey
import io.temporal.common.WorkflowExecutionHistory
import io.temporal.common.interceptors.WorkerInterceptor
import io.temporal.kotlin.TemporalDsl
import io.temporal.kotlin.client.KWorkflowClient
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.kotlin.worker.KWorker
import io.temporal.kotlin.worker.KotlinPlugin
import io.temporal.kotlin.worker.KotlinPluginOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.testing.internal.SDKTestWorkflowRule
import io.temporal.testing.internal.TracingWorkerInterceptor
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import io.temporal.worker.WorkflowImplementationOptions
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import java.time.Duration
import kotlin.reflect.KClass

/**
 * Kotlin test rule for workflow testing that wraps [SDKTestWorkflowRule] with Kotlin idioms.
 *
 * Provides:
 * - DSL-style builder pattern with Kotlin properties
 * - [KWorker] and [KWorkflowClient] for Kotlin-idiomatic APIs
 * - Automatic [KotlinPlugin] configuration for suspend workflow support
 * - All utility methods from the Java [SDKTestWorkflowRule]
 *
 * Example usage:
 * ```kotlin
 * @Rule
 * @JvmField
 * val testRule = KSDKTestWorkflowRule {
 *     workflowTypes(MyWorkflowImpl::class)
 *     activityImplementations(MyActivitiesImpl())
 * }
 *
 * @Test
 * fun `test workflow`() {
 *     val workflow = testRule.workflowClient.newWorkflowStub(
 *         MyWorkflow::class.java,
 *         WorkflowOptions.newBuilder()
 *             .setTaskQueue(testRule.taskQueue)
 *             .build()
 *     )
 *     // test workflow...
 * }
 * ```
 *
 * For suspend workflows:
 * ```kotlin
 * @Rule
 * @JvmField
 * val testRule = KSDKTestWorkflowRule {
 *     workflowTypes(MySuspendWorkflowImpl::class)
 *     suspendActivityImplementations(MySuspendActivitiesImpl())
 * }
 * ```
 *
 * With nested DSL options:
 * ```kotlin
 * @Rule
 * @JvmField
 * val testRule = KSDKTestWorkflowRule {
 *     workflowTypes(MyWorkflowImpl::class)
 *     doNotStart = true
 *     workflowClientOptions {
 *         setDataConverter(myConverter)
 *     }
 * }
 * ```
 */
public class KSDKTestWorkflowRule private constructor(
  private val delegate: SDKTestWorkflowRule,
  private val kWorkerInterceptors: List<KWorkerInterceptor>
) : TestRule {

  /** The task queue name for this test. */
  public val taskQueue: String
    get() = delegate.taskQueue

  /** The deployment name for this test. */
  public val deploymentName: String
    get() = delegate.deploymentName

  /** The Nexus endpoint for this test, if configured. */
  public val nexusEndpoint: Endpoint
    get() = delegate.nexusEndpoint

  /** The underlying Java Worker. Use [kWorker] for Kotlin-idiomatic APIs. */
  public val worker: Worker
    get() = delegate.worker

  /** Kotlin worker with idiomatic APIs for registering workflows and activities. */
  public val kWorker: KWorker by lazy {
    KWorker(delegate.worker, kWorkerInterceptors)
  }

  /** The underlying Java WorkflowClient. Use [kWorkflowClient] for Kotlin-idiomatic APIs. */
  public val workflowClient: io.temporal.client.WorkflowClient
    get() = delegate.workflowClient

  /** Kotlin workflow client with suspend functions and type-safe APIs. */
  public val kWorkflowClient: KWorkflowClient by lazy {
    KWorkflowClient(delegate.workflowClient)
  }

  /** The WorkflowServiceStubs for direct service access. */
  public val workflowServiceStubs: WorkflowServiceStubs
    get() = delegate.workflowServiceStubs

  /** Whether an external Temporal service is being used. */
  public val isUseExternalService: Boolean
    get() = delegate.isUseExternalService

  /** The test environment for advanced configuration. */
  public val testEnvironment: TestWorkflowEnvironment
    get() = delegate.testEnvironment

  /** The worker factory options used for this test. */
  public val workerFactoryOptions: WorkerFactoryOptions
    get() = delegate.workerFactoryOptions

  override fun apply(base: Statement, description: Description): Statement {
    return delegate.apply(base, description)
  }

  // ========== Interceptor Access ==========

  /**
   * Get a registered worker interceptor by type.
   */
  public fun <T : WorkerInterceptor> getInterceptor(type: Class<T>): T? {
    return delegate.getInterceptor(type)
  }

  /**
   * Get a registered worker interceptor by type using reified generics.
   */
  public inline fun <reified T : WorkerInterceptor> getInterceptor(): T? {
    return getInterceptor(T::class.java)
  }

  // ========== Workflow Stub Creation ==========

  /**
   * Create a typed workflow stub for the default task queue.
   */
  public fun <T> newWorkflowStub(workflow: Class<T>): T {
    return delegate.newWorkflowStub(workflow)
  }

  /**
   * Create a typed workflow stub using reified generics.
   */
  public inline fun <reified T> newWorkflowStub(): T {
    return newWorkflowStub(T::class.java)
  }

  /**
   * Create a typed workflow stub using KClass.
   */
  public fun <T : Any> newWorkflowStub(workflow: KClass<T>): T {
    return newWorkflowStub(workflow.java)
  }

  /**
   * Create a typed workflow stub with timeout options.
   */
  public fun <T> newWorkflowStubTimeoutOptions(workflow: Class<T>): T {
    return delegate.newWorkflowStubTimeoutOptions(workflow)
  }

  /**
   * Create a typed workflow stub with timeout options using reified generics.
   */
  public inline fun <reified T> newWorkflowStubTimeoutOptions(): T {
    return newWorkflowStubTimeoutOptions(T::class.java)
  }

  /**
   * Create a typed workflow stub with timeout options and workflow ID prefix.
   */
  public fun <T> newWorkflowStubTimeoutOptions(workflow: Class<T>, workflowIdPrefix: String): T {
    return delegate.newWorkflowStubTimeoutOptions(workflow, workflowIdPrefix)
  }

  /**
   * Create a typed workflow stub with 200s timeout options.
   */
  public fun <T> newWorkflowStub200sTimeoutOptions(workflow: Class<T>): T {
    return delegate.newWorkflowStub200sTimeoutOptions(workflow)
  }

  /**
   * Create an untyped workflow stub.
   */
  public fun newUntypedWorkflowStub(workflow: String): WorkflowStub {
    return delegate.newUntypedWorkflowStub(workflow)
  }

  /**
   * Create an untyped workflow stub with timeout options.
   */
  public fun newUntypedWorkflowStubTimeoutOptions(workflow: String): WorkflowStub {
    return delegate.newUntypedWorkflowStubTimeoutOptions(workflow)
  }

  // ========== History Access ==========

  /**
   * Get the execution history for a workflow.
   */
  public fun getExecutionHistory(workflowId: String): WorkflowExecutionHistory {
    return delegate.getExecutionHistory(workflowId)
  }

  /**
   * Get the execution history for a specific workflow run.
   */
  public fun getExecutionHistory(workflowId: String, runId: String): WorkflowExecutionHistory {
    return delegate.getExecutionHistory(workflowId, runId)
  }

  /**
   * Get all history events of a specific type.
   */
  public fun getHistoryEvents(workflowId: String, eventType: EventType): List<HistoryEvent> {
    return delegate.getHistoryEvents(workflowId, eventType)
  }

  /**
   * Get the first history event of a specific type.
   */
  public fun getHistoryEvent(workflowId: String, eventType: EventType): HistoryEvent {
    return delegate.getHistoryEvent(workflowId, eventType)
  }

  // ========== History Assertions ==========

  /**
   * Assert that a history event of the given type exists.
   */
  public fun assertHistoryEvent(workflowId: String, eventType: EventType) {
    delegate.assertHistoryEvent(workflowId, eventType)
  }

  /**
   * Assert that a history event of the given type exists for a specific run.
   */
  public fun assertHistoryEvent(workflowId: String, runId: String, eventType: EventType) {
    delegate.assertHistoryEvent(workflowId, runId, eventType)
  }

  /**
   * Assert that no history event of the given type exists.
   */
  public fun assertNoHistoryEvent(workflowId: String, eventType: EventType) {
    delegate.assertNoHistoryEvent(workflowId, eventType)
  }

  /**
   * Assert that no history event of the given type exists for a specific run.
   */
  public fun assertNoHistoryEvent(workflowId: String, runId: String, eventType: EventType) {
    delegate.assertNoHistoryEvent(workflowId, runId, eventType)
  }

  // ========== Test Utilities ==========

  /**
   * Wait for the end of the current workflow task.
   */
  public fun waitForTheEndOfWFT(workflowId: String) {
    delegate.waitForTheEndOfWFT(workflowId)
  }

  /**
   * Register a delayed callback.
   */
  public fun registerDelayedCallback(delay: Duration, callback: Runnable) {
    delegate.registerDelayedCallback(delay, callback)
  }

  /**
   * Register a delayed callback using Kotlin Duration.
   */
  public fun registerDelayedCallback(delay: kotlin.time.Duration, callback: () -> Unit) {
    delegate.registerDelayedCallback(Duration.ofMillis(delay.inWholeMilliseconds), callback)
  }

  /**
   * Sleep for the specified duration.
   */
  public fun sleep(duration: Duration) {
    delegate.sleep(duration)
  }

  /**
   * Sleep for the specified duration using Kotlin Duration.
   */
  public fun sleep(duration: kotlin.time.Duration) {
    delegate.sleep(Duration.ofMillis(duration.inWholeMilliseconds))
  }

  /**
   * Invalidate the workflow cache, causing eviction of all cached workflows.
   */
  public fun invalidateWorkflowCache() {
    delegate.invalidateWorkflowCache()
  }

  /**
   * Regenerate a history JSON file for replay testing.
   */
  public fun regenerateHistoryForReplay(workflowId: String, fileName: String) {
    delegate.regenerateHistoryForReplay(workflowId, fileName)
  }

  public companion object {
    /** Namespace used for tests. */
    public val NAMESPACE: String = SDKTestWorkflowRule.NAMESPACE

    /** Regex pattern for UUIDs. */
    public val UUID_REGEXP: String = SDKTestWorkflowRule.UUID_REGEXP

    /** Whether to regenerate JSON files for replay testing. */
    public val REGENERATE_JSON_FILES: Boolean = SDKTestWorkflowRule.REGENERATE_JSON_FILES

    /** Whether using an external Temporal service. */
    public val useExternalService: Boolean = SDKTestWorkflowRule.useExternalService

    /** Whether using virtual threads. */
    public val USE_VIRTUAL_THREADS: Boolean = SDKTestWorkflowRule.USE_VIRTUAL_THREADS

    /**
     * Create a new builder for KSDKTestWorkflowRule.
     */
    @JvmStatic
    public fun newBuilder(): Builder = Builder()

    /**
     * Wait for the first workflow task to complete by querying the stack trace.
     */
    @JvmStatic
    public fun waitForOKQuery(stub: Any) {
      SDKTestWorkflowRule.waitForOKQuery(stub)
    }

    /**
     * Assert that no history event of the given type exists in the history.
     */
    @JvmStatic
    public fun assertNoHistoryEvent(history: History, eventType: EventType) {
      SDKTestWorkflowRule.assertNoHistoryEvent(history, eventType)
    }
  }

  /**
   * Builder for [KSDKTestWorkflowRule] with idiomatic Kotlin DSL support.
   *
   * Uses properties and DSL functions instead of Java-style setXxx methods.
   */
  @TemporalDsl
  public class Builder internal constructor() {
    private val javaBuilder = SDKTestWorkflowRule.newBuilder()
    private var kWorkerInterceptors: List<KWorkerInterceptor> = emptyList()
    private var suspendActivityImplementations: List<Any> = emptyList()
    private var workerFactoryOptionsSet = false

    // ========== Simple Properties ==========

    /** The namespace for tests. */
    public var namespace: String? = null

    /** Whether to start the test environment automatically. Default is true (starts automatically). */
    public var doNotStart: Boolean = false

    /** Whether to use time skipping. */
    public var useTimeskipping: Boolean? = null

    /** Whether to use an external Temporal service. */
    public var useExternalService: Boolean? = null

    /** The target address for an external Temporal service. */
    public var target: String? = null

    /** The test timeout in seconds. */
    public var testTimeoutSeconds: Long? = null

    /** The initial time in milliseconds. */
    public var initialTimeMillis: Long? = null

    /** The metrics scope. */
    public var metricsScope: Scope? = null

    // ========== Options Objects ==========

    /** The WorkflowServiceStubsOptions. */
    public var workflowServiceStubsOptions: WorkflowServiceStubsOptions? = null

    /** The WorkflowClientOptions. */
    public var workflowClientOptions: WorkflowClientOptions? = null

    /** The WorkerOptions. */
    public var workerOptions: WorkerOptions? = null

    /** The WorkerFactoryOptions. If set, you may need to manually add KotlinPlugin for suspend workflow support. */
    public var workerFactoryOptions: WorkerFactoryOptions? = null

    /** Workflow implementation options for workflow types. */
    public var workflowImplementationOptions: WorkflowImplementationOptions? = null

    // ========== DSL Functions for Options ==========

    /**
     * Configure WorkflowClientOptions using DSL.
     *
     * Example:
     * ```kotlin
     * workflowClientOptions {
     *     setDataConverter(myConverter)
     * }
     * ```
     */
    public fun workflowClientOptions(block: WorkflowClientOptions.Builder.() -> Unit) {
      workflowClientOptions = WorkflowClientOptions.newBuilder().apply(block).build()
    }

    /**
     * Configure WorkerOptions using DSL.
     *
     * Example:
     * ```kotlin
     * workerOptions {
     *     setMaxConcurrentActivityExecutionSize(100)
     * }
     * ```
     */
    public fun workerOptions(block: WorkerOptions.Builder.() -> Unit) {
      workerOptions = WorkerOptions.newBuilder().apply(block).build()
    }

    /**
     * Configure WorkerFactoryOptions using DSL.
     *
     * Note: If you set this, you may need to manually add KotlinPlugin for suspend workflow support.
     *
     * Example:
     * ```kotlin
     * workerFactoryOptions {
     *     setWorkerInterceptors(myInterceptor)
     * }
     * ```
     */
    public fun workerFactoryOptions(block: WorkerFactoryOptions.Builder.() -> Unit) {
      workerFactoryOptions = WorkerFactoryOptions.newBuilder().apply(block).build()
    }

    /**
     * Configure WorkflowServiceStubsOptions using DSL.
     */
    public fun workflowServiceStubsOptions(block: WorkflowServiceStubsOptions.Builder.() -> Unit) {
      workflowServiceStubsOptions = WorkflowServiceStubsOptions.newBuilder().apply(block).build()
    }

    /**
     * Configure WorkflowImplementationOptions using DSL.
     */
    public fun workflowImplementationOptions(block: WorkflowImplementationOptions.Builder.() -> Unit) {
      workflowImplementationOptions = WorkflowImplementationOptions.newBuilder().apply(block).build()
    }

    // ========== Collections ==========

    private var _workflowTypes: MutableList<Class<*>> = mutableListOf()
    private var _activityImplementations: MutableList<Any> = mutableListOf()
    private var _suspendActivityImplementations: MutableList<Any> = mutableListOf()
    private var _nexusServiceImplementations: MutableList<Any> = mutableListOf()
    private var _searchAttributes: MutableList<Pair<String, IndexedValueType>> = mutableListOf()
    private var _searchAttributeKeys: MutableList<SearchAttributeKey<*>> = mutableListOf()

    /**
     * Add workflow implementation types using vararg KClass.
     *
     * Example:
     * ```kotlin
     * workflowTypes(MyWorkflowImpl::class, AnotherWorkflowImpl::class)
     * ```
     */
    public fun workflowTypes(vararg types: KClass<*>) {
      _workflowTypes.addAll(types.map { it.java })
    }

    /**
     * Add workflow implementation types using vararg Java Class.
     */
    public fun workflowTypes(vararg types: Class<*>) {
      _workflowTypes.addAll(types)
    }

    /**
     * Add activity implementations (regular Java-style activities).
     *
     * Example:
     * ```kotlin
     * activityImplementations(MyActivitiesImpl(), AnotherActivitiesImpl())
     * ```
     */
    public fun activityImplementations(vararg implementations: Any) {
      _activityImplementations.addAll(implementations)
    }

    /**
     * Add suspend activity implementations.
     * These will be registered with the worker as suspend activities after the rule starts.
     *
     * Example:
     * ```kotlin
     * suspendActivityImplementations(MySuspendActivitiesImpl())
     * ```
     */
    public fun suspendActivityImplementations(vararg implementations: Any) {
      _suspendActivityImplementations.addAll(implementations)
    }

    /**
     * Add Nexus service implementations.
     *
     * Example:
     * ```kotlin
     * nexusServiceImplementations(MyNexusServiceImpl())
     * ```
     */
    public fun nexusServiceImplementations(vararg implementations: Any) {
      _nexusServiceImplementations.addAll(implementations)
    }

    /**
     * Register a search attribute by name and type.
     *
     * Example:
     * ```kotlin
     * searchAttribute("CustomAttribute", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
     * ```
     */
    public fun searchAttribute(name: String, type: IndexedValueType) {
      _searchAttributes.add(name to type)
    }

    /**
     * Register a search attribute by key.
     *
     * Example:
     * ```kotlin
     * searchAttribute(SearchAttributeKey.forKeyword("CustomAttribute"))
     * ```
     */
    public fun searchAttribute(key: SearchAttributeKey<*>) {
      _searchAttributeKeys.add(key)
    }

    /**
     * Set Kotlin worker interceptors.
     * These interceptors are used for Kotlin workflow and activity interception.
     *
     * Example:
     * ```kotlin
     * kWorkerInterceptors(myInterceptor1, myInterceptor2)
     * ```
     */
    public fun kWorkerInterceptors(vararg interceptors: KWorkerInterceptor) {
      this.kWorkerInterceptors = interceptors.toList()
    }

    /**
     * Build the [KSDKTestWorkflowRule].
     */
    public fun build(): KSDKTestWorkflowRule {
      // Apply simple properties
      namespace?.let { javaBuilder.setNamespace(it) }
      if (doNotStart) javaBuilder.setDoNotStart(true)
      useTimeskipping?.let { javaBuilder.setUseTimeskipping(it) }
      useExternalService?.let { javaBuilder.setUseExternalService(it) }
      target?.let { javaBuilder.setTarget(it) }
      testTimeoutSeconds?.let { javaBuilder.setTestTimeoutSeconds(it) }
      initialTimeMillis?.let { javaBuilder.setInitialTimeMillis(it) }
      metricsScope?.let { javaBuilder.setMetricsScope(it) }

      // Apply options objects
      workflowServiceStubsOptions?.let { javaBuilder.setWorkflowServiceStubsOptions(it) }
      workflowClientOptions?.let { javaBuilder.setWorkflowClientOptions(it) }
      workerOptions?.let { javaBuilder.setWorkerOptions(it) }

      // Handle WorkerFactoryOptions with KotlinPlugin
      val explicitFactoryOptions = workerFactoryOptions
      if (explicitFactoryOptions != null) {
        javaBuilder.setWorkerFactoryOptions(explicitFactoryOptions)
        workerFactoryOptionsSet = true
      } else if (kWorkerInterceptors.isNotEmpty()) {
        // If worker factory options not explicitly set and we have Kotlin interceptors,
        // configure the KotlinPlugin with the interceptors
        val kotlinPlugin = KotlinPlugin.create(
          KotlinPluginOptions(workerInterceptors = kWorkerInterceptors)
        )
        javaBuilder.setWorkerFactoryOptions(
          WorkerFactoryOptions.newBuilder()
            .setWorkerInterceptors(
              TracingWorkerInterceptor(TracingWorkerInterceptor.FilteredTrace())
            )
            .addPlugin(kotlinPlugin)
            .build()
        )
      }

      // Apply collections
      if (_workflowTypes.isNotEmpty()) {
        val implOptions = workflowImplementationOptions
        if (implOptions != null) {
          javaBuilder.setWorkflowTypes(implOptions, *_workflowTypes.toTypedArray())
        } else {
          javaBuilder.setWorkflowTypes(*_workflowTypes.toTypedArray())
        }
      }
      if (_activityImplementations.isNotEmpty()) {
        javaBuilder.setActivityImplementations(*_activityImplementations.toTypedArray())
      }
      if (_nexusServiceImplementations.isNotEmpty()) {
        javaBuilder.setNexusServiceImplementation(*_nexusServiceImplementations.toTypedArray())
      }
      _searchAttributes.forEach { (name, type) ->
        javaBuilder.registerSearchAttribute(name, type)
      }
      _searchAttributeKeys.forEach { key ->
        javaBuilder.registerSearchAttribute(key)
      }

      val delegate = javaBuilder.build()
      return KSDKTestWorkflowRule(delegate, kWorkerInterceptors)
    }
  }
}

/**
 * Create a [KSDKTestWorkflowRule] using DSL syntax.
 *
 * Example:
 * ```kotlin
 * @Rule
 * @JvmField
 * val testRule = KSDKTestWorkflowRule {
 *     workflowTypes(MyWorkflowImpl::class)
 *     activityImplementations(MyActivitiesImpl())
 *     doNotStart = true
 *     workflowClientOptions {
 *         setDataConverter(myConverter)
 *     }
 * }
 * ```
 */
public fun KSDKTestWorkflowRule(
  block: @TemporalDsl KSDKTestWorkflowRule.Builder.() -> Unit
): KSDKTestWorkflowRule {
  return KSDKTestWorkflowRule.newBuilder().apply(block).build()
}
