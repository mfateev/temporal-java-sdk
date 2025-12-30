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

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.client.WorkflowOptions
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.testing.internal.SDKTestWorkflowRule
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.time.Duration

/**
 * End-to-end integration tests for Kotlin coroutine workflow features.
 *
 * These tests verify that suspend workflows can:
 * - Execute activities and get results
 * - Execute activities in parallel
 * - Execute local activities
 * - Execute child workflows
 * - Use timers/delays
 */
class KotlinCoroutineFeaturesIntegrationTest {

  // ==================== Activity Interface ====================

  @ActivityInterface
  interface TestActivities {
    @ActivityMethod
    fun greet(name: String): String

    @ActivityMethod
    fun add(a: Int, b: Int): Int

    @ActivityMethod
    fun slowOperation(durationMs: Long): String
  }

  class TestActivitiesImpl : TestActivities {
    override fun greet(name: String): String {
      return "Hello, $name!"
    }

    override fun add(a: Int, b: Int): Int {
      return a + b
    }

    override fun slowOperation(durationMs: Long): String {
      Thread.sleep(durationMs)
      return "completed after ${durationMs}ms"
    }
  }

  // ==================== Workflow Interfaces and Implementations ====================

  @WorkflowInterface
  interface ActivityCallingWorkflow {
    @WorkflowMethod
    suspend fun execute(name: String): String
  }

  class ActivityCallingWorkflowImpl : ActivityCallingWorkflow {
    override suspend fun execute(name: String): String {
      val options = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(10))
        .build()

      // Note: Activity names are capitalized (greet -> Greet)
      val result: String = KWorkflow.executeActivity(
        "Greet",
        options,
        name
      )
      return result
    }
  }

  @WorkflowInterface
  interface ParallelActivitiesWorkflow {
    @WorkflowMethod
    suspend fun execute(): Int
  }

  class ParallelActivitiesWorkflowImpl : ParallelActivitiesWorkflow {
    override suspend fun execute(): Int {
      val options = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(10))
        .build()

      // Start activities in parallel (activity names are capitalized: add -> Add)
      val handle1 = KWorkflow.startActivity<Int>("Add", options, 10, 20)
      val handle2 = KWorkflow.startActivity<Int>("Add", options, 5, 15)
      val handle3 = KWorkflow.startActivity<Int>("Add", options, 100, 200)

      // Await all results
      val result1 = handle1.await()
      val result2 = handle2.await()
      val result3 = handle3.await()

      return result1 + result2 + result3 // 30 + 20 + 300 = 350
    }
  }

  @WorkflowInterface
  interface LocalActivityWorkflow {
    @WorkflowMethod
    suspend fun execute(a: Int, b: Int): Int
  }

  class LocalActivityWorkflowImpl : LocalActivityWorkflow {
    override suspend fun execute(a: Int, b: Int): Int {
      val options = LocalActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(5))
        .build()

      // Activity names are capitalized: add -> Add
      val result: Int = KWorkflow.executeLocalActivity(
        "Add",
        options,
        a,
        b
      )
      return result
    }
  }

  @WorkflowInterface
  interface ChildWorkflow {
    @WorkflowMethod
    suspend fun process(input: String): String
  }

  class ChildWorkflowImpl : ChildWorkflow {
    override suspend fun process(input: String): String {
      return "Child processed: $input"
    }
  }

  @WorkflowInterface
  interface ParentWorkflow {
    @WorkflowMethod
    suspend fun execute(input: String): String
  }

  class ParentWorkflowImpl : ParentWorkflow {
    override suspend fun execute(input: String): String {
      val options = ChildWorkflowOptions.newBuilder()
        .build()

      val childResult: String = KWorkflow.executeChildWorkflow(
        "ChildWorkflow",
        options,
        input
      )

      return "Parent received: $childResult"
    }
  }

  @WorkflowInterface
  interface ParallelChildWorkflowsWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class ParallelChildWorkflowsWorkflowImpl : ParallelChildWorkflowsWorkflow {
    override suspend fun execute(): String {
      val options = ChildWorkflowOptions.newBuilder().build()

      // Start child workflows in parallel
      val handle1 = KWorkflow.startChildWorkflow<String>("ChildWorkflow", options, "input1")
      val handle2 = KWorkflow.startChildWorkflow<String>("ChildWorkflow", options, "input2")

      // Await both
      val result1 = handle1.await()
      val result2 = handle2.await()

      return "$result1 | $result2"
    }
  }

  @WorkflowInterface
  interface TimerWorkflow {
    @WorkflowMethod
    suspend fun execute(): Long
  }

  class TimerWorkflowImpl : TimerWorkflow {
    override suspend fun execute(): Long {
      val startTime = KWorkflow.currentTimeMillis()

      // Sleep for 100ms (will be fast in test environment)
      KWorkflow.delay(100)

      val endTime = KWorkflow.currentTimeMillis()
      return endTime - startTime
    }
  }

  @WorkflowInterface
  interface ActivityWithDelayWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class ActivityWithDelayWorkflowImpl : ActivityWithDelayWorkflow {
    override suspend fun execute(): String {
      val options = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(10))
        .build()

      // Activity names are capitalized: greet -> Greet
      val result1: String = KWorkflow.executeActivity("Greet", options, "Step1")

      // Wait between activities
      KWorkflow.delay(Duration.ofMillis(50))

      val result2: String = KWorkflow.executeActivity("Greet", options, "Step2")

      return "$result1 -> $result2"
    }
  }

  // ==================== Test Setup ====================

  @Rule
  @JvmField
  var testWorkflowRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setDoNotStart(true)
    .setActivityImplementations(TestActivitiesImpl())
    .build()

  private fun setupKotlinWorkflows(vararg workflowClasses: Class<*>) {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    workflowClasses.forEach { factory.registerWorkflowImplementationType(it) }
    testWorkflowRule.worker.registerWorkflowImplementationFactory(factory)
    testWorkflowRule.testEnvironment.start()
  }

  // ==================== Tests ====================

  @Test
  fun `workflow can execute activity and get result`() {
    setupKotlinWorkflows(ActivityCallingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ActivityCallingWorkflow", options)
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
  }

  @Test
  fun `workflow can execute activities in parallel`() {
    setupKotlinWorkflows(ParallelActivitiesWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ParallelActivitiesWorkflow", options)
    stub.start()
    val result = stub.getResult(Int::class.java)

    assertEquals(350, result) // 30 + 20 + 300
  }

  @Test
  fun `workflow can execute local activity`() {
    setupKotlinWorkflows(LocalActivityWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("LocalActivityWorkflow", options)
    stub.start(7, 8)
    val result = stub.getResult(Int::class.java)

    assertEquals(15, result)
  }

  @Test
  fun `workflow can execute child workflow`() {
    setupKotlinWorkflows(
      ParentWorkflowImpl::class.java,
      ChildWorkflowImpl::class.java
    )

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ParentWorkflow", options)
    stub.start("test-input")
    val result = stub.getResult(String::class.java)

    assertEquals("Parent received: Child processed: test-input", result)
  }

  @Test
  fun `workflow can execute child workflows in parallel`() {
    setupKotlinWorkflows(
      ParallelChildWorkflowsWorkflowImpl::class.java,
      ChildWorkflowImpl::class.java
    )

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ParallelChildWorkflowsWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("Child processed: input1 | Child processed: input2", result)
  }

  @Test
  fun `workflow can use timer delay`() {
    setupKotlinWorkflows(TimerWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("TimerWorkflow", options)
    stub.start()
    val elapsedMs = stub.getResult(Long::class.java)

    // The test server may fast-forward time, but elapsed should be at least 100ms in workflow time
    assertTrue("Expected elapsed time >= 100ms, but was ${elapsedMs}ms", elapsedMs >= 100)
  }

  @Test
  fun `workflow can combine activities with delays`() {
    setupKotlinWorkflows(ActivityWithDelayWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("ActivityWithDelayWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, Step1! -> Hello, Step2!", result)
  }
}
