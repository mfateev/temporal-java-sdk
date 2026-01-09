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
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test

/**
 * End-to-end integration tests for Kotlin coroutine workflows.
 *
 * These tests verify the core workflow execution path for suspend workflows:
 * client → worker → workflow → result
 *
 * Note: Workflow interfaces have suspend functions for the worker-side execution.
 * Client-side invocation uses untyped stubs since Java proxies don't support
 * Kotlin suspend functions directly.
 *
 * Activity and timer integration requires additional context setup and will be
 * addressed in future phases.
 */
class KotlinWorkflowIntegrationTest {

  // ==================== Test Interfaces and Implementations ====================

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

  @WorkflowInterface
  interface WorkflowWithMultipleParams {
    @WorkflowMethod
    suspend fun execute(name: String, count: Int): String
  }

  class WorkflowWithMultipleParamsImpl : WorkflowWithMultipleParams {
    override suspend fun execute(name: String, count: Int): String {
      return "$name repeated $count times"
    }
  }

  @WorkflowInterface
  interface WorkflowReturningUnit {
    @WorkflowMethod
    suspend fun execute()
  }

  class WorkflowReturningUnitImpl : WorkflowReturningUnit {
    override suspend fun execute() {
      // No return value
    }
  }

  // Non-suspend workflow for mixed registration test
  @WorkflowInterface
  interface JavaStyleWorkflow {
    @WorkflowMethod
    fun execute(input: String): String
  }

  class JavaStyleWorkflowImpl : JavaStyleWorkflow {
    override fun execute(input: String): String {
      return "Java style: $input"
    }
  }

  // ==================== Test Setup ====================

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    setDoNotStart(true)
  }

  private fun setupKotlinWorkflows(vararg workflowClasses: Class<*>) {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    workflowClasses.forEach { factory.registerWorkflowImplementationType(it) }
    testWorkflowRule.worker.registerWorkflowImplementationFactory(factory)
    testWorkflowRule.testEnvironment.start()
  }

  // ==================== Tests ====================

  @Test
  fun `simple workflow execution`() {
    setupKotlinWorkflows(SimpleWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = client.newUntypedWorkflowStub("SimpleWorkflow", options)
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
  }

  @Test
  fun `workflow with multiple parameters`() {
    setupKotlinWorkflows(WorkflowWithMultipleParamsImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("WorkflowWithMultipleParams", options)
    stub.start("test", 5)
    val result = stub.getResult(String::class.java)

    assertEquals("test repeated 5 times", result)
  }

  @Test
  fun `workflow returning Unit`() {
    setupKotlinWorkflows(WorkflowReturningUnitImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("WorkflowReturningUnit", options)
    stub.start()
    val result = stub.getResult(Void::class.java)

    assertEquals(null, result)
  }

  @Test
  fun `mixed kotlin and java workflows on same worker`() {
    // Register Kotlin suspend workflow
    val kotlinFactory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    kotlinFactory.registerWorkflowImplementationType(SimpleWorkflowImpl::class.java)
    testWorkflowRule.worker.registerWorkflowImplementationFactory(kotlinFactory)

    // Register Java-style workflow using standard registration
    testWorkflowRule.worker.registerWorkflowImplementationTypes(JavaStyleWorkflowImpl::class.java)

    testWorkflowRule.testEnvironment.start()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Execute Kotlin suspend workflow
    val kotlinStub = client.newUntypedWorkflowStub("SimpleWorkflow", options)
    kotlinStub.start("Kotlin")
    val kotlinResult = kotlinStub.getResult(String::class.java)
    assertEquals("Hello, Kotlin!", kotlinResult)

    // Execute Java-style workflow (can use typed stub)
    val javaWorkflow = client.newWorkflowStub(JavaStyleWorkflow::class.java, options)
    val javaResult = javaWorkflow.execute("Java")
    assertEquals("Java style: Java", javaResult)
  }
}
