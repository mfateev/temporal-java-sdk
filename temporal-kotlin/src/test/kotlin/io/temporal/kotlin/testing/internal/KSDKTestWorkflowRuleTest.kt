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

package io.temporal.kotlin.testing.internal

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.client.WorkflowOptions
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Rule
import org.junit.Test

/**
 * Tests for [KSDKTestWorkflowRule] verifying the Kotlin DSL builder and
 * access to Kotlin-specific APIs like [io.temporal.kotlin.worker.KWorker]
 * and [io.temporal.kotlin.client.KWorkflowClient].
 */
class KSDKTestWorkflowRuleTest {

  // ==================== Test Interfaces and Implementations ====================

  @WorkflowInterface
  interface GreetingWorkflow {
    @WorkflowMethod
    fun greet(name: String): String
  }

  class GreetingWorkflowImpl : GreetingWorkflow {
    override fun greet(name: String): String {
      return "Hello, $name!"
    }
  }

  @WorkflowInterface
  interface CalculatorWorkflow {
    @WorkflowMethod
    fun add(a: Int, b: Int): Int
  }

  class CalculatorWorkflowImpl : CalculatorWorkflow {
    override fun add(a: Int, b: Int): Int {
      return a + b
    }
  }

  @ActivityInterface
  interface GreetingActivities {
    @ActivityMethod
    fun formatGreeting(name: String): String
  }

  class GreetingActivitiesImpl : GreetingActivities {
    override fun formatGreeting(name: String): String {
      return "Formatted: $name"
    }
  }

  // ==================== Tests ====================

  @Rule
  @JvmField
  val testRule = KSDKTestWorkflowRule {
    workflowTypes(GreetingWorkflowImpl::class)
  }

  @Test
  fun `DSL builder creates rule with workflow types`() {
    // Verify the rule is properly initialized
    assertNotNull(testRule.taskQueue)
    assertNotNull(testRule.worker)
    assertNotNull(testRule.workflowClient)
  }

  @Test
  fun `kWorker property provides KWorker instance`() {
    val kWorker = testRule.kWorker
    assertNotNull(kWorker)
    // KWorker should wrap the underlying worker
    assertEquals(testRule.worker, kWorker.worker)
  }

  @Test
  fun `kWorkflowClient property provides KWorkflowClient instance`() {
    val kClient = testRule.kWorkflowClient
    assertNotNull(kClient)
    assertNotNull(kClient.workflowClient)
    // Verify it's connected to the same namespace
    assertEquals(
      testRule.workflowClient.options.namespace,
      kClient.workflowClient.options.namespace
    )
  }

  @Test
  fun `workflow execution via typed stub`() {
    val workflow = testRule.newWorkflowStub<GreetingWorkflow>()
    val result = workflow.greet("World")
    assertEquals("Hello, World!", result)
  }

  @Test
  fun `workflow execution via KClass stub`() {
    val workflow = testRule.newWorkflowStub(GreetingWorkflow::class)
    val result = workflow.greet("Kotlin")
    assertEquals("Hello, Kotlin!", result)
  }

  @Test
  fun `newWorkflowStubTimeoutOptions creates stub with timeouts`() {
    val workflow = testRule.newWorkflowStubTimeoutOptions<GreetingWorkflow>()
    val result = workflow.greet("Timeout Test")
    assertEquals("Hello, Timeout Test!", result)
  }
}

/**
 * Test for multiple workflow types registration using KClass.
 */
class KSDKTestWorkflowRuleMultipleWorkflowsTest {

  @WorkflowInterface
  interface WorkflowA {
    @WorkflowMethod
    fun executeA(): String
  }

  class WorkflowAImpl : WorkflowA {
    override fun executeA(): String = "A"
  }

  @WorkflowInterface
  interface WorkflowB {
    @WorkflowMethod
    fun executeB(): String
  }

  class WorkflowBImpl : WorkflowB {
    override fun executeB(): String = "B"
  }

  @Rule
  @JvmField
  val testRule = KSDKTestWorkflowRule {
    workflowTypes(WorkflowAImpl::class, WorkflowBImpl::class)
  }

  @Test
  fun `multiple workflow types can be registered via KClass`() {
    val workflowA = testRule.newWorkflowStub<WorkflowA>()
    val workflowB = testRule.newWorkflowStub<WorkflowB>()

    assertEquals("A", workflowA.executeA())
    assertEquals("B", workflowB.executeB())
  }
}

/**
 * Test for workflow with activities registration.
 */
class KSDKTestWorkflowRuleWithActivitiesTest {

  @ActivityInterface
  interface SimpleActivities {
    @ActivityMethod
    fun echo(input: String): String
  }

  class SimpleActivitiesImpl : SimpleActivities {
    override fun echo(input: String): String = "Echo: $input"
  }

  @WorkflowInterface
  interface EchoWorkflow {
    @WorkflowMethod
    fun execute(input: String): String
  }

  // Note: This is a simple workflow that doesn't call activities,
  // just tests that activity registration works
  class EchoWorkflowImpl : EchoWorkflow {
    override fun execute(input: String): String = "Workflow: $input"
  }

  @Rule
  @JvmField
  val testRule = KSDKTestWorkflowRule {
    workflowTypes(EchoWorkflowImpl::class)
    activityImplementations(SimpleActivitiesImpl())
  }

  @Test
  fun `workflow and activity implementations can be registered together`() {
    val workflow = testRule.newWorkflowStub<EchoWorkflow>()
    val result = workflow.execute("test")
    assertEquals("Workflow: test", result)
  }
}

/**
 * Test for accessing test environment utilities.
 */
class KSDKTestWorkflowRuleUtilitiesTest {

  @WorkflowInterface
  interface SimpleWorkflow {
    @WorkflowMethod
    fun execute(): String
  }

  class SimpleWorkflowImpl : SimpleWorkflow {
    override fun execute(): String = "done"
  }

  @Rule
  @JvmField
  val testRule = KSDKTestWorkflowRule {
    workflowTypes(SimpleWorkflowImpl::class)
  }

  @Test
  fun `testEnvironment is accessible`() {
    assertNotNull(testRule.testEnvironment)
  }

  @Test
  fun `workflowServiceStubs is accessible`() {
    assertNotNull(testRule.workflowServiceStubs)
  }

  @Test
  fun `workerFactoryOptions is accessible`() {
    assertNotNull(testRule.workerFactoryOptions)
  }

  @Test
  fun `getExecutionHistory returns workflow history`() {
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testRule.taskQueue)
      .setWorkflowId("test-workflow-history")
      .build()

    val stub = testRule.workflowClient.newWorkflowStub(SimpleWorkflow::class.java, options)
    stub.execute()

    val history = testRule.getExecutionHistory("test-workflow-history")
    assertNotNull(history)
    assertNotNull(history.events)
  }
}
