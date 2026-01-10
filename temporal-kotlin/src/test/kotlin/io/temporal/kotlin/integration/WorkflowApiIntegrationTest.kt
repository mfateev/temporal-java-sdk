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
import io.temporal.common.SearchAttributeKey
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.UpdateMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.milliseconds

/**
 * Integration tests for Kotlin workflow APIs.
 *
 * Tests for:
 * - Search attributes (getTypedSearchAttributes, upsertTypedSearchAttributes)
 * - Memo (getMemo, upsertMemo)
 * - isReplaying()
 * - mutableSideEffect()
 * - getMetricsScope()
 * - getCurrentUpdateInfo()
 * - isEveryHandlerFinished()
 * - setCurrentDetails() / getCurrentDetails()
 */
class WorkflowApiIntegrationTest {

  // ==================== Workflow Interfaces ====================

  @WorkflowInterface
  interface SearchAttributeWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  @WorkflowInterface
  interface MemoWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  @WorkflowInterface
  interface ReplayingWorkflow {
    @WorkflowMethod
    suspend fun execute(): Boolean
  }

  @WorkflowInterface
  interface MutableSideEffectWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  @WorkflowInterface
  interface MetricsScopeWorkflow {
    @WorkflowMethod
    suspend fun execute(): Boolean
  }

  @WorkflowInterface
  interface WorkflowDetailsWorkflow {
    @WorkflowMethod
    suspend fun execute(): String

    @QueryMethod
    fun getDetails(): String?
  }

  @WorkflowInterface
  interface HandlerCompletionWorkflow {
    @WorkflowMethod
    suspend fun execute(): Boolean

    @SignalMethod
    suspend fun processSignal(value: String)

    @QueryMethod
    fun getHandlerStatus(): Boolean
  }

  @WorkflowInterface
  interface UpdateInfoWorkflow {
    @WorkflowMethod
    suspend fun execute(): String

    @UpdateMethod
    suspend fun myUpdate(input: String): String
  }

  @WorkflowInterface
  interface RetryWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  // ==================== Workflow Implementations ====================

  class SearchAttributeWorkflowImpl : SearchAttributeWorkflow {
    override suspend fun execute(): String {
      // Get initial search attributes (verifies API works)
      KWorkflow.typedSearchAttributes

      // Upsert search attributes
      val statusKey = SearchAttributeKey.forKeyword("CustomKeywordField")
      KWorkflow.upsertTypedSearchAttributes(
        statusKey.valueSet("Processing")
      )

      // Get updated search attributes
      val updatedAttrs = KWorkflow.typedSearchAttributes
      val status = updatedAttrs.get(statusKey)

      return "status=$status"
    }
  }

  class MemoWorkflowImpl : MemoWorkflow {
    override suspend fun execute(): String {
      // Get initial memo (should be null)
      val initialMemo = KWorkflow.getMemo("testKey", String::class.java)

      // Upsert memo
      KWorkflow.upsertMemo(mapOf("testKey" to "testValue", "count" to 42))

      // Get memo value
      val memoValue = KWorkflow.getMemo("testKey", String::class.java)
      val countValue = KWorkflow.getMemo("count", Int::class.java)

      return "initial=$initialMemo, memo=$memoValue, count=$countValue"
    }
  }

  class ReplayingWorkflowImpl : ReplayingWorkflow {
    override suspend fun execute(): Boolean {
      // isReplaying should be false on first execution
      return KWorkflow.isReplaying
    }
  }

  class MutableSideEffectWorkflowImpl : MutableSideEffectWorkflow {
    private var counter = 0

    override suspend fun execute(): String {
      // First call - should execute and store result
      val result1 = KWorkflow.mutableSideEffect("counter", Int::class.java) {
        counter++
        counter
      }

      // Second call with same id - should return cached value
      val result2 = KWorkflow.mutableSideEffect("counter", Int::class.java) {
        counter++
        counter
      }

      // Third call - value unchanged, should still return same
      val result3 = KWorkflow.mutableSideEffect("counter", Int::class.java) {
        counter // same value, no increment
      }

      return "results=$result1,$result2,$result3"
    }
  }

  class MetricsScopeWorkflowImpl : MetricsScopeWorkflow {
    override suspend fun execute(): Boolean {
      // metricsScope returns non-null Scope, verify it works
      val scope = KWorkflow.metricsScope
      // Try using the scope - if it doesn't throw, it works
      scope.counter("test_counter").inc(1)
      return true
    }
  }

  class WorkflowDetailsWorkflowImpl : WorkflowDetailsWorkflow {
    override suspend fun execute(): String {
      // Initially no details
      val initial = KWorkflow.currentDetails

      // Set details
      KWorkflow.currentDetails = "Processing step 1"

      // Get details
      val step1 = KWorkflow.currentDetails

      // Update details
      KWorkflow.currentDetails = "Processing step 2"
      val step2 = KWorkflow.currentDetails

      return "initial=$initial, step1=$step1, step2=$step2"
    }

    override fun getDetails(): String? {
      return KWorkflow.currentDetails
    }
  }

  class HandlerCompletionWorkflowImpl : HandlerCompletionWorkflow {
    private var signalReceived = false
    private var handlerFinishedAtQuery = false

    override suspend fun execute(): Boolean {
      // Wait for signal
      KWorkflow.awaitCondition { signalReceived }

      // Check if all handlers are finished
      return KWorkflow.isEveryHandlerFinished
    }

    override suspend fun processSignal(value: String) {
      // Simulate some work
      signalReceived = true
    }

    override fun getHandlerStatus(): Boolean {
      return KWorkflow.isEveryHandlerFinished
    }
  }

  class UpdateInfoWorkflowImpl : UpdateInfoWorkflow {
    private var updateResult: String? = null

    override suspend fun execute(): String {
      // Wait for update to complete
      KWorkflow.awaitCondition { updateResult != null }
      return updateResult!!
    }

    override suspend fun myUpdate(input: String): String {
      // Get current update info
      val updateInfo = KWorkflow.currentUpdateInfo
      val name = updateInfo?.updateName ?: "unknown"
      val id = updateInfo?.updateId ?: "unknown"

      updateResult = "update=$name, id=$id, input=$input"
      return updateResult!!
    }
  }

  class RetryWorkflowImpl : RetryWorkflow {
    override suspend fun execute(): String {
      var attempts = 0

      // Test retry with successful completion after failures
      val result = KWorkflow.retry(
        KRetryOptions(
          initialInterval = 10.milliseconds,
          maximumAttempts = 3
        )
      ) {
        attempts++
        if (attempts < 3) {
          throw RuntimeException("Attempt $attempts failed")
        }
        "success after $attempts attempts"
      }

      return result
    }
  }

  // ==================== Test Infrastructure ====================

  @Rule
  @JvmField
  val testWorkflowRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  private fun setupKotlinWorkflows(vararg workflowClasses: Class<*>) {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    workflowClasses.forEach { factory.registerWorkflowImplementationType(it) }
    testWorkflowRule.worker.registerWorkflowImplementationFactory(factory)
    testWorkflowRule.testEnvironment.start()
  }

  // ==================== Tests ====================

  @Test
  fun `search attributes can be read and updated`() {
    setupKotlinWorkflows(SearchAttributeWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SearchAttributeWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("status=Processing", result)
  }

  @Test
  fun `memo can be read and updated`() {
    setupKotlinWorkflows(MemoWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("MemoWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("initial=null, memo=testValue, count=42", result)
  }

  @Test
  fun `isReplaying returns false on first execution`() {
    setupKotlinWorkflows(ReplayingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ReplayingWorkflow", options)
    stub.start()
    val result = stub.getResult(Boolean::class.java)

    assertFalse(result)
  }

  @Test
  fun `mutableSideEffect caches results correctly`() {
    setupKotlinWorkflows(MutableSideEffectWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("MutableSideEffectWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    // First call returns 1, second call detects change and returns 2,
    // third call with same value should return cached 2
    assertEquals("results=1,2,2", result)
  }

  @Test
  fun `metricsScope is available`() {
    setupKotlinWorkflows(MetricsScopeWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("MetricsScopeWorkflow", options)
    stub.start()
    val result = stub.getResult(Boolean::class.java)

    assertTrue(result)
  }

  @Test
  fun `workflow details can be set and retrieved`() {
    setupKotlinWorkflows(WorkflowDetailsWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("WorkflowDetailsWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("initial=null, step1=Processing step 1, step2=Processing step 2", result)
  }

  @Test
  fun `isEveryHandlerFinished returns true when no handlers running`() {
    setupKotlinWorkflows(HandlerCompletionWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("HandlerCompletionWorkflow", options)
    stub.start()

    // Send signal
    stub.signal("processSignal", "test")

    val result = stub.getResult(Boolean::class.java)

    // After signal handler completes, isEveryHandlerFinished should be true
    assertTrue(result)
  }

  @Test
  fun `getCurrentUpdateInfo returns update info during update handler`() {
    setupKotlinWorkflows(UpdateInfoWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("UpdateInfoWorkflow", options)
    stub.start()

    // Send update
    val updateResult = stub.update("myUpdate", String::class.java, "testInput")

    // Get workflow result
    val result = stub.getResult(String::class.java)

    // Verify update info was available during update handler
    assertTrue(result.contains("update=myUpdate"))
    assertTrue(result.contains("input=testInput"))
  }

  @Test
  fun `retry succeeds after transient failures`() {
    setupKotlinWorkflows(RetryWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("RetryWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("success after 3 attempts", result)
  }
}
