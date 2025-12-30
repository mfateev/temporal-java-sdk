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
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
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

  // ==================== Signal and Query Workflow ====================

  @WorkflowInterface
  interface SignalQueryWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class SignalQueryWorkflowImpl : SignalQueryWorkflow {
    private var status = "waiting"
    private var approved = false
    private var messages = mutableListOf<String>()

    override suspend fun execute(): String {
      // Register signal handlers
      KWorkflow.registerSignalHandler("approve") { args ->
        approved = args.get(0, Boolean::class.java)
        status = if (approved) "approved" else "rejected"
      }

      KWorkflow.registerSignalHandler("addMessage") { args ->
        val message = args.get(0, String::class.java)
        messages.add(message)
      }

      // Register query handlers (explicitly use no-arg form)
      KWorkflow.registerQueryHandler("getStatus") { -> status }

      KWorkflow.registerQueryHandler("getMessageCount") { -> messages.size }

      KWorkflow.registerQueryHandler("getMessages") { -> messages.toList() }

      // Wait for approval
      KWorkflow.condition { approved }

      return "Workflow completed with ${messages.size} messages"
    }
  }

  @WorkflowInterface
  interface DynamicSignalWorkflow {
    @WorkflowMethod
    suspend fun execute(): Map<String, Int>
  }

  class DynamicSignalWorkflowImpl : DynamicSignalWorkflow {
    private val signalCounts = mutableMapOf<String, Int>()
    private var done = false

    override suspend fun execute(): Map<String, Int> {
      // Register dynamic signal handler for any signal
      KWorkflow.registerDynamicSignalHandler { signalName, args ->
        if (signalName == "done") {
          done = true
        } else {
          val count = signalCounts.getOrDefault(signalName, 0)
          signalCounts[signalName] = count + 1
        }
      }

      // Register dynamic query handler
      KWorkflow.registerDynamicQueryHandler { queryName, args ->
        when (queryName) {
          "getCount" -> {
            val signalName = args.get(0, String::class.java)
            signalCounts.getOrDefault(signalName, 0)
          }
          "getAllCounts" -> signalCounts.toMap()
          else -> null
        }
      }

      // Wait for done signal
      KWorkflow.condition { done }

      return signalCounts.toMap()
    }
  }

  // ==================== Annotation-Based Signal and Query Workflow ====================

  /**
   * Workflow interface with annotation-based signal and query methods.
   * This demonstrates the declarative approach to defining handlers.
   */
  @WorkflowInterface
  interface AnnotatedSignalQueryWorkflow {
    @WorkflowMethod
    suspend fun execute(): String

    @SignalMethod
    suspend fun approve(value: Boolean)

    @SignalMethod(name = "addMessage")
    suspend fun addMessageSignal(message: String)

    @QueryMethod
    fun getStatus(): String

    @QueryMethod(name = "getMessageCount")
    fun messageCount(): Int

    @QueryMethod
    fun getMessages(): List<String>
  }

  class AnnotatedSignalQueryWorkflowImpl : AnnotatedSignalQueryWorkflow {
    private var status = "waiting"
    private var approved = false
    private val messages = mutableListOf<String>()

    override suspend fun execute(): String {
      // Wait for approval using condition
      KWorkflow.condition { approved }

      return "Workflow completed with ${messages.size} messages"
    }

    override suspend fun approve(value: Boolean) {
      approved = value
      status = if (value) "approved" else "rejected"
    }

    override suspend fun addMessageSignal(message: String) {
      messages.add(message)
    }

    override fun getStatus(): String = status

    override fun messageCount(): Int = messages.size

    override fun getMessages(): List<String> = messages.toList()
  }

  // ==================== Signal and Query Tests ====================

  @Test(timeout = 10000)
  fun `workflow can register and handle signals`() {
    setupKotlinWorkflows(SignalQueryWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SignalQueryWorkflow", options)
    stub.start()

    // Send signals
    stub.signal("addMessage", "Hello")
    stub.signal("addMessage", "World")
    stub.signal("approve", true)

    val result = stub.getResult(String::class.java)
    assertEquals("Workflow completed with 2 messages", result)
  }

  @Test(timeout = 10000)
  fun `workflow can register and handle queries`() {
    setupKotlinWorkflows(SignalQueryWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SignalQueryWorkflow", options)
    stub.start()

    // Wait a bit for workflow to start and register handlers
    Thread.sleep(500)

    // Query initial status
    val initialStatus = stub.query("getStatus", String::class.java)
    assertEquals("waiting", initialStatus)

    // Send signals
    stub.signal("addMessage", "Test1")
    stub.signal("addMessage", "Test2")
    stub.signal("addMessage", "Test3")

    // Give time for signals to be processed
    Thread.sleep(500)

    // Query message count
    val messageCount = stub.query("getMessageCount", Int::class.java)
    assertEquals(3, messageCount)

    // Approve to complete workflow
    stub.signal("approve", true)

    // Query final status
    val finalStatus = stub.query("getStatus", String::class.java)
    assertEquals("approved", finalStatus)

    val result = stub.getResult(String::class.java)
    assertEquals("Workflow completed with 3 messages", result)
  }

  @Test(timeout = 10000)
  fun `workflow can use dynamic signal and query handlers`() {
    setupKotlinWorkflows(DynamicSignalWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("DynamicSignalWorkflow", options)
    stub.start()

    // Wait for workflow to start
    Thread.sleep(500)

    // Send various signals
    stub.signal("eventA")
    stub.signal("eventA")
    stub.signal("eventB")
    stub.signal("eventA")
    stub.signal("eventC")

    // Give time for signals to be processed
    Thread.sleep(500)

    // Query specific counts
    val countA = stub.query("getCount", Int::class.java, "eventA")
    assertEquals(3, countA)

    val countB = stub.query("getCount", Int::class.java, "eventB")
    assertEquals(1, countB)

    // Complete workflow
    stub.signal("done")

    @Suppress("UNCHECKED_CAST")
    val result = stub.getResult(Map::class.java) as Map<String, Int>
    assertEquals(3, result["eventA"])
    assertEquals(1, result["eventB"])
    assertEquals(1, result["eventC"])
  }

  // ==================== Annotation-Based Signal and Query Tests ====================

  @Test(timeout = 10000)
  fun `annotation-based workflow can handle signals`() {
    setupKotlinWorkflows(AnnotatedSignalQueryWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AnnotatedSignalQueryWorkflow", options)
    stub.start()

    // Send signals using annotation-defined names
    stub.signal("addMessage", "Hello")
    stub.signal("addMessage", "World")
    stub.signal("approve", true)

    val result = stub.getResult(String::class.java)
    assertEquals("Workflow completed with 2 messages", result)
  }

  @Test(timeout = 10000)
  fun `annotation-based workflow can handle queries`() {
    setupKotlinWorkflows(AnnotatedSignalQueryWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AnnotatedSignalQueryWorkflow", options)
    stub.start()

    // Wait for workflow to start
    Thread.sleep(500)

    // Query initial status
    val initialStatus = stub.query("getStatus", String::class.java)
    assertEquals("waiting", initialStatus)

    // Send signals
    stub.signal("addMessage", "Test1")
    stub.signal("addMessage", "Test2")
    stub.signal("addMessage", "Test3")

    // Give time for signals to be processed
    Thread.sleep(500)

    // Query using custom name
    val messageCount = stub.query("getMessageCount", Int::class.java)
    assertEquals(3, messageCount)

    // Query messages (order may vary due to signal batching)
    @Suppress("UNCHECKED_CAST")
    val messages = stub.query("getMessages", List::class.java) as List<String>
    assertEquals(setOf("Test1", "Test2", "Test3"), messages.toSet())

    // Approve to complete workflow
    stub.signal("approve", true)

    // Query final status
    val finalStatus = stub.query("getStatus", String::class.java)
    assertEquals("approved", finalStatus)

    val result = stub.getResult(String::class.java)
    assertEquals("Workflow completed with 3 messages", result)
  }
}
