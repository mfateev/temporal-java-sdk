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

@file:OptIn(InternalTemporalApi::class)

package io.temporal.kotlin.worker

import io.temporal.common.converter.DataConverter
import io.temporal.internal.worker.WorkflowImplementationFactory
import io.temporal.kotlin.TemporalDsl
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowDefinition
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.plugin.WorkerPlugin
import io.temporal.worker.WorkerOptions

/**
 * Plugin for enabling Kotlin coroutine support in Temporal workflows.
 *
 * This plugin provides configuration for Kotlin coroutine-based workflow execution,
 * enabling the use of suspend functions in workflow definitions.
 *
 * When registered with a WorkerFactory, this plugin automatically detects Kotlin
 * suspend workflows during registration and routes them to the Kotlin coroutine
 * execution runtime. Non-suspend workflows are handled by the default Java factory.
 *
 * Usage:
 * ```kotlin
 * val factory = WorkerFactory.newInstance(
 *     client,
 *     WorkerFactoryOptions.newBuilder()
 *         .addPlugin(KotlinPlugin { deadlockDetectionTimeout = 1500L })
 *         .build()
 * )
 * val worker = factory.newWorker("task-queue")
 *
 * // Suspend workflows auto-detected and routed to Kotlin factory
 * // Non-suspend workflows use default Java factory
 * worker.registerWorkflowImplementationTypes(
 *     MySuspendWorkflowImpl::class.java,
 *     MyJavaWorkflowImpl::class.java
 * )
 * ```
 *
 * @see KotlinPluginOptions
 * @see WorkerPlugin
 */
public class KotlinPlugin private constructor(
  private val options: KotlinPluginOptions
) : WorkerPlugin {

  /** Lazily created factory - shared across all workflow types handled by this plugin. */
  private var factory: KotlinWorkflowImplementationFactory? = null

  /**
   * Configures worker options before worker creation.
   * Sets deadlock detection timeout if configured.
   */
  override fun configureWorker(builder: WorkerOptions.Builder): WorkerOptions.Builder {
    if (options.configureDeadlockDetection) {
      builder.setDefaultDeadlockDetectionTimeout(options.deadlockDetectionTimeout)
    }
    return builder
  }

  /**
   * Called for each workflow type during registration.
   *
   * If the workflow uses Kotlin suspend functions, this plugin handles it by:
   * 1. Creating/reusing a KotlinWorkflowImplementationFactory
   * 2. Registering the type with that factory
   * 3. Returning the factory
   *
   * For non-suspend workflows, returns null to let the default POJO factory handle them.
   */
  override fun getFactoryForType(
    workflowImplementationType: Class<*>,
    dataConverter: DataConverter
  ): WorkflowImplementationFactory? {
    // Check if this is a Kotlin suspend workflow
    if (!KotlinWorkflowDefinition.isSuspendWorkflow(workflowImplementationType)) {
      return null // Not a suspend workflow, let default factory handle it
    }

    // Lazily create the factory
    if (factory == null) {
      factory = KotlinWorkflowImplementationFactory(
        dataConverter = dataConverter,
        deadlockDetectionTimeoutMs = options.deadlockDetectionTimeout
      )
    }

    // Register the workflow type with our factory
    factory!!.registerWorkflowImplementationType(workflowImplementationType)
    return factory
  }

  /**
   * Returns the deadlock detection timeout configured for this plugin.
   */
  public val deadlockDetectionTimeout: Long
    get() = options.deadlockDetectionTimeout

  public companion object {
    /**
     * Creates a KotlinPlugin with default options.
     */
    @JvmStatic
    public fun create(): KotlinPlugin = KotlinPlugin(KotlinPluginOptions())

    /**
     * Creates a KotlinPlugin with custom options.
     */
    @JvmStatic
    public fun create(options: KotlinPluginOptions): KotlinPlugin = KotlinPlugin(options)

    /**
     * Creates a KotlinPlugin using a builder DSL.
     */
    @JvmStatic
    public fun create(block: KotlinPluginOptions.Builder.() -> Unit): KotlinPlugin {
      return KotlinPlugin(KotlinPluginOptions.Builder().apply(block).build())
    }
  }
}

/**
 * Configuration options for the Kotlin plugin.
 */
public class KotlinPluginOptions(
  /**
   * Timeout in milliseconds for deadlock detection in workflow code.
   * Default is 1000ms.
   */
  public val deadlockDetectionTimeout: Long = DEFAULT_DEADLOCK_DETECTION_TIMEOUT,

  /**
   * Whether to configure worker's deadlock detection timeout from this plugin.
   * Set to false if you want to manage the timeout separately via WorkerOptions.
   * Default is true.
   */
  public val configureDeadlockDetection: Boolean = true
) {
  public companion object {
    public const val DEFAULT_DEADLOCK_DETECTION_TIMEOUT: Long = 1000L
  }

  /**
   * Builder for KotlinPluginOptions.
   */
  @TemporalDsl
  public class Builder {
    /**
     * Timeout in milliseconds for deadlock detection in workflow code.
     */
    public var deadlockDetectionTimeout: Long = DEFAULT_DEADLOCK_DETECTION_TIMEOUT

    /**
     * Whether to configure worker's deadlock detection timeout from this plugin.
     */
    public var configureDeadlockDetection: Boolean = true

    public fun build(): KotlinPluginOptions = KotlinPluginOptions(
      deadlockDetectionTimeout = deadlockDetectionTimeout,
      configureDeadlockDetection = configureDeadlockDetection
    )
  }
}

/**
 * Creates [KotlinPlugin] with the specified options using a builder DSL.
 *
 * Example:
 * ```kotlin
 * val plugin = KotlinPlugin {
 *   deadlockDetectionTimeout = 2000L
 * }
 * ```
 */
public fun KotlinPlugin(
  options: @TemporalDsl KotlinPluginOptions.Builder.() -> Unit
): KotlinPlugin {
  return KotlinPlugin.create(options)
}

/**
 * Creates [KotlinPlugin] with default options.
 */
public fun KotlinPlugin(): KotlinPlugin {
  return KotlinPlugin.create()
}
