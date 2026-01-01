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
import io.temporal.client.WorkflowOptions
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.workflow.KChildWorkflowOptions
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.testing.internal.SDKTestWorkflowRule
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.seconds

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
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

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
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start activities in parallel using standard coroutines
      return coroutineScope {
        val d1 = async { KWorkflow.executeActivity<Int>("Add", options, 10, 20) }
        val d2 = async { KWorkflow.executeActivity<Int>("Add", options, 5, 15) }
        val d3 = async { KWorkflow.executeActivity<Int>("Add", options, 100, 200) }

        // Await all results using standard awaitAll
        val results = awaitAll(d1, d2, d3)
        results.sum() // 30 + 20 + 300 = 350
      }
    }
  }

  @WorkflowInterface
  interface LocalActivityWorkflow {
    @WorkflowMethod
    suspend fun execute(a: Int, b: Int): Int
  }

  class LocalActivityWorkflowImpl : LocalActivityWorkflow {
    override suspend fun execute(a: Int, b: Int): Int {
      val options = KLocalActivityOptions(startToCloseTimeout = 5.seconds)

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
      val options = KChildWorkflowOptions()

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
      val options = KChildWorkflowOptions()

      // Start child workflows in parallel using standard coroutines
      return coroutineScope {
        val d1 = async { KWorkflow.executeChildWorkflow<String>("ChildWorkflow", options, "input1") }
        val d2 = async { KWorkflow.executeChildWorkflow<String>("ChildWorkflow", options, "input2") }

        // Await both using standard awaitAll
        val (result1, result2) = awaitAll(d1, d2)
        "$result1 | $result2"
      }
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

      // Sleep for 100ms using standard delay (intercepted via Delay interface)
      delay(100)

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
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Activity names are capitalized: greet -> Greet
      val result1: String = KWorkflow.executeActivity("Greet", options, "Step1")

      // Wait between activities using standard delay
      delay(50)

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
      KWorkflow.awaitCondition { approved }

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
      KWorkflow.awaitCondition { done }

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
      // Wait for approval using awaitCondition
      KWorkflow.awaitCondition { approved }

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

  // ==================== Async Workflow Tests ====================

  /**
   * Workflow demonstrating eager async execution with parallel activities.
   */
  @WorkflowInterface
  interface AsyncParallelActivitiesWorkflow {
    @WorkflowMethod
    suspend fun execute(): Int
  }

  class AsyncParallelActivitiesWorkflowImpl : AsyncParallelActivitiesWorkflow {
    override suspend fun execute(): Int {
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start activities in parallel using standard coroutines
      return coroutineScope {
        val d1 = async { KWorkflow.executeActivity<Int>("Add", options, 10, 20) }
        val d2 = async { KWorkflow.executeActivity<Int>("Add", options, 5, 15) }
        val d3 = async { KWorkflow.executeActivity<Int>("Add", options, 100, 200) }

        // All three activities are now running in parallel
        // Await results using standard awaitAll
        val results = awaitAll(d1, d2, d3)
        results.sum() // 30 + 20 + 300 = 350
      }
    }
  }

  /**
   * Workflow demonstrating async with condition waiting on isCompleted.
   */
  @WorkflowInterface
  interface AsyncConditionWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class AsyncConditionWorkflowImpl : AsyncConditionWorkflow {
    override suspend fun execute(): String {
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start activity asynchronously using standard coroutineScope/async
      return coroutineScope {
        val deferred = async {
          KWorkflow.executeActivity<String>("Greet", options, "AsyncWorld")
        }

        // With standard Deferred, can check isCompleted or just await
        KWorkflow.awaitCondition { deferred.isCompleted }

        "Got: ${deferred.await()}"
      }
    }
  }

  /**
   * Workflow demonstrating mixed async and sequential execution.
   */
  @WorkflowInterface
  interface AsyncMixedExecutionWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class AsyncMixedExecutionWorkflowImpl : AsyncMixedExecutionWorkflow {
    override suspend fun execute(): String {
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start slow activity in background using standard async
      return coroutineScope {
        val backgroundTask = async {
          KWorkflow.executeActivity<String>("SlowOperation", options, 100L)
        }

        // Do quick work while background task runs
        val quickResult = KWorkflow.executeActivity<String>("Greet", options, "Quick")

        // Now wait for background task
        val slowResult = backgroundTask.await()

        "$quickResult + $slowResult"
      }
    }
  }

  /**
   * Workflow demonstrating async error handling.
   */
  @WorkflowInterface
  interface AsyncErrorHandlingWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class AsyncErrorHandlingWorkflowImpl : AsyncErrorHandlingWorkflow {
    override suspend fun execute(): String {
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start async operation using standard coroutines
      return coroutineScope {
        val successDeferred = async {
          KWorkflow.executeActivity<String>("Greet", options, "Success")
        }

        // Check isCompleted before waiting
        val wasCompletedImmediately = successDeferred.isCompleted

        // Wait for result
        val result = successDeferred.await()

        if (!wasCompletedImmediately) {
          "Async completed: $result"
        } else {
          "Unexpectedly completed immediately: $result"
        }
      }
    }
  }

  // ==================== Async Tests ====================

  @Test
  fun `async enables parallel activity execution`() {
    setupKotlinWorkflows(AsyncParallelActivitiesWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AsyncParallelActivitiesWorkflow", options)
    stub.start()
    val result = stub.getResult(Int::class.java)

    assertEquals(350, result) // 30 + 20 + 300
  }

  /**
   * Workflow demonstrating awaitAll for multiple deferred values.
   */
  @WorkflowInterface
  interface AwaitAllWorkflow {
    @WorkflowMethod
    suspend fun execute(): Int
  }

  class AwaitAllWorkflowImpl : AwaitAllWorkflow {
    override suspend fun execute(): Int {
      val options = KActivityOptions(startToCloseTimeout = 10.seconds)

      // Start activities in parallel using standard coroutines
      return coroutineScope {
        val deferreds = listOf(
          async { KWorkflow.executeActivity<Int>("Add", options, 10, 20) },
          async { KWorkflow.executeActivity<Int>("Add", options, 5, 15) },
          async { KWorkflow.executeActivity<Int>("Add", options, 100, 200) }
        )

        // Await all using standard awaitAll
        val results = deferreds.awaitAll()
        results.sum() // 30 + 20 + 300 = 350
      }
    }
  }

  @Test
  fun `awaitAll waits for all deferreds`() {
    setupKotlinWorkflows(AwaitAllWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AwaitAllWorkflow", options)
    stub.start()
    val result = stub.getResult(Int::class.java)

    assertEquals(350, result)
  }

  @Test
  fun `async works with condition waiting on isCompleted`() {
    setupKotlinWorkflows(AsyncConditionWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AsyncConditionWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("Got: Hello, AsyncWorld!", result)
  }

  @Test
  fun `async supports mixed execution with background tasks`() {
    setupKotlinWorkflows(AsyncMixedExecutionWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AsyncMixedExecutionWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, Quick! + completed after 100ms", result)
  }

  @Test
  fun `async deferred tracks completion state correctly`() {
    setupKotlinWorkflows(AsyncErrorHandlingWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("AsyncErrorHandlingWorkflow", options)
    stub.start()
    val result = stub.getResult(String::class.java)

    // The async should not complete immediately (activity needs to run)
    assertEquals("Async completed: Hello, Success!", result)
  }
}
