@file:OptIn(kotlin.time.ExperimentalTime::class)

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

package io.temporal.kotlin.common

import io.temporal.common.RetryOptions
import io.temporal.kotlin.toJava
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Kotlin-native retry options with Duration support.
 *
 * This data class provides a Kotlin-idiomatic way to configure retry behavior
 * using native `kotlin.time.Duration` values instead of `java.time.Duration`.
 *
 * Example:
 * ```kotlin
 * val retryOptions = KRetryOptions(
 *     initialInterval = 1.seconds,
 *     maximumInterval = 1.minutes,
 *     backoffCoefficient = 2.0,
 *     maximumAttempts = 5,
 *     doNotRetry = listOf("java.lang.IllegalArgumentException")
 * )
 * ```
 *
 * @property initialInterval Interval of the first retry. Default is 1 second.
 * @property backoffCoefficient Coefficient used to calculate the next retry interval.
 *           The next retry interval is previous interval multiplied by this coefficient.
 *           Default is 2.0.
 * @property maximumInterval Maximum interval between retries. Exponential backoff leads
 *           to interval increase. This value caps the increase. Default is 100x [initialInterval].
 * @property maximumAttempts Maximum number of attempts. When exceeded, retries stop even
 *           if not expired yet. Default is unlimited (0).
 * @property doNotRetry List of exception type names that should not be retried.
 */
public data class KRetryOptions(
  val initialInterval: Duration = 1.seconds,
  val backoffCoefficient: Double = 2.0,
  val maximumInterval: Duration? = null,
  val maximumAttempts: Int = 0,
  val doNotRetry: List<String> = emptyList()
) {
  /**
   * Converts this [KRetryOptions] to the Java SDK [RetryOptions].
   *
   * This conversion happens once when the activity/workflow is scheduled,
   * so there's no runtime overhead during workflow execution.
   */
  public fun toJavaOptions(): RetryOptions {
    return RetryOptions.newBuilder().apply {
      setInitialInterval(initialInterval.toJava())
      setBackoffCoefficient(backoffCoefficient)
      maximumInterval?.let { setMaximumInterval(it.toJava()) }
      if (maximumAttempts > 0) {
        setMaximumAttempts(maximumAttempts)
      }
      if (doNotRetry.isNotEmpty()) {
        setDoNotRetry(*doNotRetry.toTypedArray())
      }
    }.build()
  }
}
