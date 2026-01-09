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

package io.temporal.kotlin.internal.interceptor

import io.temporal.activity.ActivityExecutionContext
import io.temporal.kotlin.interceptor.KActivityExecutionInput
import io.temporal.kotlin.interceptor.KActivityExecutionOutput
import io.temporal.kotlin.interceptor.KActivityInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KActivityInboundCallsInterceptorBase
import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalInput
import io.temporal.kotlin.interceptor.KUpdateInput
import io.temporal.kotlin.interceptor.KUpdateOutput
import io.temporal.kotlin.interceptor.KWorkerInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInput
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowOutput
import io.temporal.kotlin.internal.InternalTemporalApi
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Test

@OptIn(InternalTemporalApi::class)
class InterceptorChainTest {

  // Track call order for workflow interceptors
  private val workflowCallOrder = mutableListOf<String>()

  // Track call order for activity interceptors
  private val activityCallOrder = mutableListOf<String>()

  /**
   * Test interceptor that records when it's called.
   */
  inner class TrackingWorkerInterceptor(private val name: String) : KWorkerInterceptorBase() {
    override fun interceptWorkflow(
      next: KWorkflowInboundCallsInterceptor
    ): KWorkflowInboundCallsInterceptor {
      return object : KWorkflowInboundCallsInterceptorBase(next) {
        override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
          workflowCallOrder.add("$name-before")
          val result = next.execute(input)
          workflowCallOrder.add("$name-after")
          return result
        }
      }
    }

    override fun interceptActivity(
      next: KActivityInboundCallsInterceptor
    ): KActivityInboundCallsInterceptor {
      return object : KActivityInboundCallsInterceptorBase(next) {
        override suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput {
          activityCallOrder.add("$name-before")
          val result = next.execute(input)
          activityCallOrder.add("$name-after")
          return result
        }
      }
    }
  }

  /**
   * Mock root workflow interceptor that records execution.
   */
  inner class MockRootWorkflowInterceptor : KWorkflowInboundCallsInterceptor {
    var initCalled = false
    var executeCalled = false

    override suspend fun init(outboundCalls: KWorkflowOutboundCallsInterceptor) {
      initCalled = true
    }

    override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
      workflowCallOrder.add("root-execute")
      executeCalled = true
      return KWorkflowOutput(result = "root-result")
    }

    override suspend fun handleSignal(input: KSignalInput) {
      // No-op for test
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      return KQueryOutput(result = null)
    }

    override fun validateUpdate(input: KUpdateInput) {
      // No-op for test
    }

    override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
      return KUpdateOutput(result = null)
    }
  }

  /**
   * Mock root activity interceptor that records execution.
   */
  inner class MockRootActivityInterceptor : KActivityInboundCallsInterceptor {
    var initCalled = false
    var executeCalled = false

    override fun init(context: ActivityExecutionContext) {
      initCalled = true
    }

    override suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput {
      activityCallOrder.add("root-execute")
      executeCalled = true
      return KActivityExecutionOutput(result = "root-result")
    }
  }

  @Test
  fun `buildWorkflowInboundChain returns root when no interceptors`() {
    val root = MockRootWorkflowInterceptor()

    val result = InterceptorChain.buildWorkflowInboundChain(emptyList(), root)

    assertSame(root, result)
  }

  @Test
  fun `buildWorkflowInboundChain with single interceptor wraps root`() {
    val root = MockRootWorkflowInterceptor()
    val interceptor = TrackingWorkerInterceptor("A")

    val chain = InterceptorChain.buildWorkflowInboundChain(listOf(interceptor), root)

    // Chain should not be the root itself
    assert(chain !== root)
  }

  @Test
  fun `buildWorkflowInboundChain calls interceptors in correct order`() {
    workflowCallOrder.clear()
    val root = MockRootWorkflowInterceptor()
    val interceptors = listOf(
      TrackingWorkerInterceptor("first"),
      TrackingWorkerInterceptor("second"),
      TrackingWorkerInterceptor("third")
    )

    val chain = InterceptorChain.buildWorkflowInboundChain(interceptors, root)

    // Execute to trigger the chain
    kotlinx.coroutines.runBlocking {
      chain.execute(
        KWorkflowInput(
          header = io.temporal.common.interceptors.Header.empty(),
          arguments = emptyArray()
        )
      )
    }

    // First interceptor should be called first (before), then second, then third, then root
    // On the way back: third-after, second-after, first-after
    assertEquals(
      listOf(
        "first-before",
        "second-before",
        "third-before",
        "root-execute",
        "third-after",
        "second-after",
        "first-after"
      ),
      workflowCallOrder
    )
  }

  @Test
  fun `buildActivityInboundChain returns root when no interceptors`() {
    val root = MockRootActivityInterceptor()

    val result = InterceptorChain.buildActivityInboundChain(emptyList(), root)

    assertSame(root, result)
  }

  @Test
  fun `buildActivityInboundChain with single interceptor wraps root`() {
    val root = MockRootActivityInterceptor()
    val interceptor = TrackingWorkerInterceptor("A")

    val chain = InterceptorChain.buildActivityInboundChain(listOf(interceptor), root)

    // Chain should not be the root itself
    assert(chain !== root)
  }

  @Test
  fun `buildActivityInboundChain calls interceptors in correct order`() {
    activityCallOrder.clear()
    val root = MockRootActivityInterceptor()
    val interceptors = listOf(
      TrackingWorkerInterceptor("first"),
      TrackingWorkerInterceptor("second"),
      TrackingWorkerInterceptor("third")
    )

    val chain = InterceptorChain.buildActivityInboundChain(interceptors, root)

    // Execute to trigger the chain
    kotlinx.coroutines.runBlocking {
      chain.execute(
        KActivityExecutionInput(
          header = io.temporal.common.interceptors.Header.empty(),
          arguments = emptyArray()
        )
      )
    }

    // First interceptor should be called first (before), then second, then third, then root
    // On the way back: third-after, second-after, first-after
    assertEquals(
      listOf(
        "first-before",
        "second-before",
        "third-before",
        "root-execute",
        "third-after",
        "second-after",
        "first-after"
      ),
      activityCallOrder
    )
  }

  @Test
  fun `KWorkerInterceptorBase passes through workflow calls`() {
    val root = MockRootWorkflowInterceptor()
    val baseInterceptor = KWorkerInterceptorBase()

    val result = baseInterceptor.interceptWorkflow(root)

    assertSame(root, result)
  }

  @Test
  fun `KWorkerInterceptorBase passes through activity calls`() {
    val root = MockRootActivityInterceptor()
    val baseInterceptor = KWorkerInterceptorBase()

    val result = baseInterceptor.interceptActivity(root)

    assertSame(root, result)
  }
}
