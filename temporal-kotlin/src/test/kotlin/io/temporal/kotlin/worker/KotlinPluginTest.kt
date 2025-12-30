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

import org.junit.Assert.assertEquals
import org.junit.Test

class KotlinPluginTest {

  @Test
  fun `create with default options`() {
    val plugin = KotlinPlugin()

    assertEquals(
      KotlinPluginOptions.DEFAULT_DEADLOCK_DETECTION_TIMEOUT,
      plugin.deadlockDetectionTimeout
    )
  }

  @Test
  fun `create with custom options using DSL`() {
    val plugin = KotlinPlugin {
      deadlockDetectionTimeout = 2000L
    }

    assertEquals(2000L, plugin.deadlockDetectionTimeout)
  }

  @Test
  fun `create with custom KotlinPluginOptions`() {
    val options = KotlinPluginOptions(deadlockDetectionTimeout = 3000L)
    val plugin = KotlinPlugin.create(options)

    assertEquals(3000L, plugin.deadlockDetectionTimeout)
  }

  @Test
  fun `KotlinPluginOptions builder works correctly`() {
    val options = KotlinPluginOptions.Builder().apply {
      deadlockDetectionTimeout = 5000L
    }.build()

    assertEquals(5000L, options.deadlockDetectionTimeout)
  }

  @Test
  fun `KotlinPluginOptions has correct default value`() {
    val options = KotlinPluginOptions()

    assertEquals(1000L, options.deadlockDetectionTimeout)
  }
}
