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

import io.temporal.kotlin.interceptor.KWorkerInterceptor

/**
 * DSL builder for configuring Kotlin worker factory options.
 *
 * This builder creates a [KWorkerFactoryOptions] data class with support for
 * Kotlin-specific interceptors.
 *
 * Example:
 * ```kotlin
 * val factory = KWorkerFactory(client) {
 *     workerInterceptors = listOf(
 *         LoggingInterceptor(),
 *         MetricsInterceptor()
 *     )
 *     maxWorkflowThreadCount = 800
 * }
 * ```
 */
public class KWorkerFactoryOptionsBuilder internal constructor() {

  /**
   * List of Kotlin worker interceptors to register.
   *
   * Interceptors are called in order for inbound operations and
   * in reverse order for outbound operations.
   */
  public var workerInterceptors: List<KWorkerInterceptor> = emptyList()

  /**
   * Maximum number of workflow threads.
   *
   * This defines the maximum number of concurrent workflow executions
   * that can run on this worker. Default is 600.
   */
  public var maxWorkflowThreadCount: Int? = null

  /**
   * Enable/disable logging in replay.
   * When enabled, logs during workflow replay will be shown.
   */
  public var enableLoggingInReplay: Boolean? = null

  /**
   * Workflow cache size. Default is 600.
   */
  public var workflowCacheSize: Int? = null

  /**
   * Builds the KWorkerFactoryOptions data class.
   *
   * @return configured KWorkerFactoryOptions
   */
  internal fun build(): KWorkerFactoryOptions {
    return KWorkerFactoryOptions(
      workerInterceptors = workerInterceptors,
      maxWorkflowThreadCount = maxWorkflowThreadCount,
      enableLoggingInReplay = enableLoggingInReplay,
      workflowCacheSize = workflowCacheSize
    )
  }
}
