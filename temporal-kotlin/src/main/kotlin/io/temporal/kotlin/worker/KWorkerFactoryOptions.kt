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
 * Options for configuring a Kotlin worker factory.
 *
 * Example:
 * ```kotlin
 * val options = KWorkerFactoryOptions(
 *     workerInterceptors = listOf(LoggingInterceptor()),
 *     maxWorkflowThreadCount = 800
 * )
 * val factory = KWorkerFactory(client, options)
 * ```
 *
 * @property workerInterceptors List of Kotlin worker interceptors to register.
 *           Interceptors are called in order for inbound operations and
 *           in reverse order for outbound operations.
 * @property maxWorkflowThreadCount Maximum number of workflow threads. Default is 600.
 * @property enableLoggingInReplay Enable/disable logging in replay.
 *           When enabled, logs during workflow replay will be shown.
 * @property workflowCacheSize Workflow cache size. Default is 600.
 */
public data class KWorkerFactoryOptions(
  val workerInterceptors: List<KWorkerInterceptor> = emptyList(),
  val maxWorkflowThreadCount: Int? = null,
  val enableLoggingInReplay: Boolean? = null,
  val workflowCacheSize: Int? = null
) {
  public companion object {
    /**
     * Default worker factory options.
     */
    @JvmStatic
    public val DEFAULT: KWorkerFactoryOptions = KWorkerFactoryOptions()
  }
}
