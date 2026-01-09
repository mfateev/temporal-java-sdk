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

@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin.integration

import io.temporal.client.WorkflowOptions
import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalInput
import io.temporal.kotlin.interceptor.KUpdateInput
import io.temporal.kotlin.interceptor.KUpdateOutput
import io.temporal.kotlin.interceptor.KWorkerInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInput
import io.temporal.kotlin.interceptor.KWorkflowOutput
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.worker.KotlinPlugin
import io.temporal.kotlin.worker.KotlinPluginOptions
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Integration tests for Kotlin workflow interceptors.
 *
 * These tests verify that interceptors are properly invoked during workflow execution.
 */
class InterceptorIntegrationTest {

  // ==================== Test Tracking ====================

  companion object {
    // Thread-safe list to track interceptor calls across tests
    val interceptorCalls = CopyOnWriteArrayList<String>()

    fun clearCalls() {
      interceptorCalls.clear()
    }
  }

  // ==================== Test Interceptor ====================

  /**
   * A test interceptor that records all workflow operations.
   */
  class TrackingInterceptor : KWorkerInterceptorBase() {
    override fun interceptWorkflow(
      next: KWorkflowInboundCallsInterceptor
    ): KWorkflowInboundCallsInterceptor {
      return TrackingWorkflowInterceptor(next)
    }
  }

  class TrackingWorkflowInterceptor(
    next: KWorkflowInboundCallsInterceptor
  ) : KWorkflowInboundCallsInterceptorBase(next) {

    override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
      interceptorCalls.add("execute:before")
      val result = next.execute(input)
      interceptorCalls.add("execute:after")
      return result
    }

    override suspend fun handleSignal(input: KSignalInput) {
      interceptorCalls.add("signal:${input.signalName}")
      next.handleSignal(input)
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      interceptorCalls.add("query:${input.queryName}")
      return next.handleQuery(input)
    }

    override fun validateUpdate(input: KUpdateInput) {
      interceptorCalls.add("validateUpdate:${input.updateName}")
      next.validateUpdate(input)
    }

    override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
      interceptorCalls.add("executeUpdate:${input.updateName}")
      return next.executeUpdate(input)
    }
  }

  // ==================== Workflow Interfaces ====================

  @WorkflowInterface
  interface SimpleWorkflow {
    @WorkflowMethod
    suspend fun execute(input: String): String
  }

  class SimpleWorkflowImpl : SimpleWorkflow {
    override suspend fun execute(input: String): String {
      return "Hello, $input!"
    }
  }

  // ==================== Test Setup ====================

  private val trackingInterceptor = TrackingInterceptor()

  private val kotlinPlugin = KotlinPlugin.create(
    KotlinPluginOptions(
      workerInterceptors = listOf(trackingInterceptor)
    )
  )

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    setWorkerFactoryOptions(
      WorkerFactoryOptions.newBuilder()
        .addPlugin(kotlinPlugin)
        .build()
    )
    setWorkflowTypes(SimpleWorkflowImpl::class)
  }

  // ==================== Tests ====================

  @Test
  fun `interceptor is called during workflow execution`() {
    clearCalls()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SimpleWorkflow", options)
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)

    // Verify interceptor was called
    assertTrue(
      "Expected execute:before in calls: $interceptorCalls",
      interceptorCalls.contains("execute:before")
    )
    assertTrue(
      "Expected execute:after in calls: $interceptorCalls",
      interceptorCalls.contains("execute:after")
    )

    // Verify order: before should come before after
    val beforeIndex = interceptorCalls.indexOf("execute:before")
    val afterIndex = interceptorCalls.indexOf("execute:after")
    assertTrue("execute:before should come before execute:after", beforeIndex < afterIndex)
  }

  // TODO: Add tests for signal/query/update interceptor invocation once those are wired through
  // the interceptor chain. Currently only workflow execution goes through interceptors.
}
