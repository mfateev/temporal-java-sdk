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

package io.temporal.kotlin.worker

import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.TemporalDsl
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory

/**
 * Plugin for enabling Kotlin coroutine support in Temporal workflows.
 *
 * This plugin provides configuration for Kotlin coroutine-based workflow execution,
 * enabling the use of suspend functions in workflow definitions.
 *
 * Example usage:
 * ```kotlin
 * val plugin = KotlinPlugin {
 *   deadlockDetectionTimeout = 1500L
 * }
 *
 * // Use with worker
 * worker.registerKotlinWorkflowImplementationTypes(
 *   plugin,
 *   MyWorkflowImpl::class
 * )
 * ```
 *
 * @see KotlinPluginOptions
 */
public class KotlinPlugin private constructor(
  private val options: KotlinPluginOptions
) {

  /**
   * Creates a new KotlinWorkflowImplementationFactory configured with this plugin's options.
   *
   * @param dataConverter the data converter to use for serialization
   * @return a new factory instance
   */
  @InternalTemporalApi
  internal fun createFactory(dataConverter: DataConverter): KotlinWorkflowImplementationFactory {
    return KotlinWorkflowImplementationFactory(
      dataConverter = dataConverter,
      deadlockDetectionTimeoutMs = options.deadlockDetectionTimeout
    )
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
  public val deadlockDetectionTimeout: Long = DEFAULT_DEADLOCK_DETECTION_TIMEOUT
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

    public fun build(): KotlinPluginOptions = KotlinPluginOptions(
      deadlockDetectionTimeout = deadlockDetectionTimeout
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
