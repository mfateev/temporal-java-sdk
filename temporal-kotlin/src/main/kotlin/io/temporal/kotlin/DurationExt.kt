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

package io.temporal.kotlin

import kotlin.time.Duration.Companion.nanoseconds
import java.time.Duration as JavaDuration
import kotlin.time.Duration as KotlinDuration

/**
 * Converts a Kotlin [Duration][KotlinDuration] to a Java [Duration][JavaDuration].
 *
 * This is useful when working with Temporal APIs that expect Java Duration
 * while using Kotlin's more expressive duration literals.
 *
 * Example:
 * ```kotlin
 * val timeout = 30.seconds.toJava()
 * ```
 */
public fun KotlinDuration.toJava(): JavaDuration {
  return JavaDuration.ofNanos(this.inWholeNanoseconds)
}

/**
 * Converts a Java [Duration][JavaDuration] to a Kotlin [Duration][KotlinDuration].
 *
 * This is useful when receiving durations from Temporal APIs and wanting
 * to work with them using Kotlin's duration operations.
 *
 * Example:
 * ```kotlin
 * val kotlinDuration = javaActivityTimeout.toKotlin()
 * ```
 */
public fun JavaDuration.toKotlin(): KotlinDuration {
  return this.toNanos().nanoseconds
}
