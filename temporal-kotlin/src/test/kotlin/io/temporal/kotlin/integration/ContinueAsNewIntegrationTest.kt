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
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.internal.workflow.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.workflow.KContinueAsNewOptions
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.hours

/**
 * Integration tests for continue-as-new functionality in Kotlin workflows.
 */
class ContinueAsNewIntegrationTest {

  // ==================== Workflow Interfaces and Implementations ====================

  /**
   * Simple workflow that continues as new after processing some iterations.
   */
  @WorkflowInterface
  interface CountingWorkflow {
    @WorkflowMethod
    suspend fun count(iteration: Int, maxIterations: Int): String
  }

  class CountingWorkflowImpl : CountingWorkflow {
    override suspend fun count(iteration: Int, maxIterations: Int): String {
      if (iteration >= maxIterations) {
        return "Completed after $iteration iterations"
      }
      // Continue as new with incremented iteration
      KWorkflow.continueAsNew(iteration + 1, maxIterations)
    }
  }

  /**
   * Workflow that continues as new with custom options.
   */
  @WorkflowInterface
  interface ContinueWithOptionsWorkflow {
    @WorkflowMethod
    suspend fun process(value: Int): String
  }

  class ContinueWithOptionsWorkflowImpl : ContinueWithOptionsWorkflow {
    override suspend fun process(value: Int): String {
      if (value >= 3) {
        return "Done with value $value"
      }
      // Continue with custom options
      val options = KContinueAsNewOptions(
        workflowRunTimeout = 1.hours
      )
      KWorkflow.continueAsNew(options, value + 1)
    }
  }

  /**
   * Workflow that accumulates data across continue-as-new boundaries.
   */
  @WorkflowInterface
  interface AccumulatorWorkflow {
    @WorkflowMethod
    suspend fun accumulate(items: List<String>, processed: Int): String
  }

  class AccumulatorWorkflowImpl : AccumulatorWorkflow {
    override suspend fun accumulate(items: List<String>, processed: Int): String {
      if (items.isEmpty()) {
        return "Processed $processed items"
      }

      // Process one item and continue as new with remaining
      val remaining = items.drop(1)
      KWorkflow.continueAsNew(remaining, processed + 1)
    }
  }

  // ==================== Test Setup ====================

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
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
  fun `workflow can continue as new with basic parameters`() {
    setupKotlinWorkflows(CountingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("CountingWorkflow", options)
    stub.start(0, 3)
    val result = stub.getResult(String::class.java)

    assertEquals("Completed after 3 iterations", result)
  }

  @Test
  fun `workflow can continue as new with custom options`() {
    setupKotlinWorkflows(ContinueWithOptionsWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ContinueWithOptionsWorkflow", options)
    stub.start(0)
    val result = stub.getResult(String::class.java)

    assertEquals("Done with value 3", result)
  }

  @Test
  fun `workflow can pass complex data across continue as new boundaries`() {
    setupKotlinWorkflows(AccumulatorWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val items = listOf("a", "b", "c", "d", "e")
    val stub = client.newUntypedWorkflowStub("AccumulatorWorkflow", options)
    stub.start(items, 0)
    val result = stub.getResult(String::class.java)

    assertEquals("Processed 5 items", result)
  }

  @Test
  fun `workflow completes immediately if no continue as new needed`() {
    setupKotlinWorkflows(CountingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Start with iteration already at max
    val stub = client.newUntypedWorkflowStub("CountingWorkflow", options)
    stub.start(5, 5)
    val result = stub.getResult(String::class.java)

    assertEquals("Completed after 5 iterations", result)
  }

  @Test
  fun `single iteration continue as new works correctly`() {
    setupKotlinWorkflows(CountingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Just one continue-as-new iteration
    val stub = client.newUntypedWorkflowStub("CountingWorkflow", options)
    stub.start(0, 1)
    val result = stub.getResult(String::class.java)

    assertEquals("Completed after 1 iterations", result)
  }
}
