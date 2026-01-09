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

import io.temporal.kotlin.interceptor.KActivityInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkerInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
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

  // ========== Interceptor Tests ==========

  /**
   * Simple test interceptor for verifying wiring.
   */
  class TestInterceptor : KWorkerInterceptorBase() {
    var workflowInterceptCalled = false
    var activityInterceptCalled = false

    override fun interceptWorkflow(
      next: KWorkflowInboundCallsInterceptor
    ): KWorkflowInboundCallsInterceptor {
      workflowInterceptCalled = true
      return next
    }

    override fun interceptActivity(
      next: KActivityInboundCallsInterceptor
    ): KActivityInboundCallsInterceptor {
      activityInterceptCalled = true
      return next
    }
  }

  @Test
  fun `KotlinPluginOptions accepts worker interceptors`() {
    val interceptor1 = TestInterceptor()
    val interceptor2 = TestInterceptor()
    val interceptors = listOf(interceptor1, interceptor2)

    val options = KotlinPluginOptions(workerInterceptors = interceptors)

    assertEquals(2, options.workerInterceptors.size)
    assertTrue(options.workerInterceptors.contains(interceptor1))
    assertTrue(options.workerInterceptors.contains(interceptor2))
  }

  @Test
  fun `KotlinPluginOptions default has empty interceptors list`() {
    val options = KotlinPluginOptions()

    assertTrue(options.workerInterceptors.isEmpty())
  }

  @Test
  fun `KotlinPluginOptions builder accepts interceptors`() {
    val interceptor = TestInterceptor()

    val options = KotlinPluginOptions.Builder().apply {
      workerInterceptors = listOf(interceptor)
    }.build()

    assertEquals(1, options.workerInterceptors.size)
    assertTrue(options.workerInterceptors.contains(interceptor))
  }

  @Test
  fun `create KotlinPlugin with interceptors using DSL`() {
    val interceptor = TestInterceptor()

    val plugin = KotlinPlugin {
      workerInterceptors = listOf(interceptor)
    }

    // Plugin created successfully with interceptors
    assertEquals(1000L, plugin.deadlockDetectionTimeout)
  }

  @Test
  fun `create KotlinPlugin with interceptors using options`() {
    val interceptor = TestInterceptor()
    val options = KotlinPluginOptions(
      deadlockDetectionTimeout = 2000L,
      workerInterceptors = listOf(interceptor)
    )

    val plugin = KotlinPlugin.create(options)

    assertEquals(2000L, plugin.deadlockDetectionTimeout)
  }
}
