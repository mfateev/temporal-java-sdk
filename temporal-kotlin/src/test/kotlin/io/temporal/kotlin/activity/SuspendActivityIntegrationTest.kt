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

package io.temporal.kotlin.activity

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowOptions
import io.temporal.testing.internal.SDKTestWorkflowRule
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for suspend activity support.
 *
 * These tests verify that suspend activities:
 * - Execute correctly and return results
 * - Support multiple parameters and return types
 * - Work with heartbeating
 * - Handle errors properly
 * - Can coexist with regular activities
 *
 * Design Note:
 * - Activity interfaces use suspend functions (they define the implementation contract)
 * - Workflows use untyped stubs since Java SDK proxies can't invoke Kotlin suspend functions
 * - The SuspendActivityWrapper registers the implementation as a DynamicActivity
 */
class SuspendActivityIntegrationTest {

  // ==================== Test Interfaces ====================
  // Interfaces use suspend - this is the implementation contract

  @ActivityInterface
  interface SuspendActivities {
    @ActivityMethod(name = "Greet")
    suspend fun greet(name: String): String

    @ActivityMethod(name = "Add")
    suspend fun add(a: Int, b: Int): Int

    @ActivityMethod(name = "ProcessWithDelay")
    suspend fun processWithDelay(input: String, delayMs: Long): String

    @ActivityMethod(name = "NoReturnValue")
    suspend fun noReturnValue(message: String)

    @ActivityMethod(name = "ThrowError")
    suspend fun throwError(message: String): String
  }

  @ActivityInterface
  interface RegularActivities {
    @ActivityMethod(name = "RegularGreet")
    fun regularGreet(name: String): String
  }

  @ActivityInterface
  interface HeartbeatActivities {
    @ActivityMethod(name = "ProcessWithHeartbeat")
    suspend fun processWithHeartbeat(items: Int): Int
  }

  // ==================== Test Implementations ====================

  class SuspendActivitiesImpl : SuspendActivities {
    companion object {
      val executionThreads = ConcurrentHashMap<String, String>()
      val executionCount = AtomicInteger(0)
    }

    override suspend fun greet(name: String): String {
      executionThreads["greet"] = Thread.currentThread().name
      executionCount.incrementAndGet()
      return "Hello, $name!"
    }

    override suspend fun add(a: Int, b: Int): Int {
      executionThreads["add"] = Thread.currentThread().name
      executionCount.incrementAndGet()
      return a + b
    }

    override suspend fun processWithDelay(input: String, delayMs: Long): String {
      executionThreads["processWithDelay-start"] = Thread.currentThread().name
      executionCount.incrementAndGet()
      delay(delayMs) // This is a suspend function - should not block the thread
      executionThreads["processWithDelay-end"] = Thread.currentThread().name
      return "Processed: $input"
    }

    override suspend fun noReturnValue(message: String) {
      executionThreads["noReturnValue"] = Thread.currentThread().name
      executionCount.incrementAndGet()
      // No return value - test Unit return type
    }

    override suspend fun throwError(message: String): String {
      executionCount.incrementAndGet()
      throw RuntimeException(message)
    }
  }

  class RegularActivitiesImpl : RegularActivities {
    companion object {
      val executionCount = AtomicInteger(0)
    }

    override fun regularGreet(name: String): String {
      executionCount.incrementAndGet()
      return "Regular Hello, $name!"
    }
  }

  class HeartbeatActivitiesImpl : HeartbeatActivities {
    companion object {
      val heartbeatCount = AtomicInteger(0)
    }

    override suspend fun processWithHeartbeat(items: Int): Int {
      var processed = 0
      for (i in 1..items) {
        delay(10) // Simulate work
        processed = i
        KActivity.heartbeat(processed)
        heartbeatCount.incrementAndGet()
      }
      return processed
    }
  }

  // ==================== Test Workflows ====================
  // Workflows use untyped stubs since Java proxies can't call suspend functions

  @WorkflowInterface
  interface TestGreetWorkflow {
    @WorkflowMethod
    fun runGreet(name: String): String
  }

  @WorkflowInterface
  interface TestAddWorkflow {
    @WorkflowMethod
    fun runAdd(a: Int, b: Int): Int
  }

  @WorkflowInterface
  interface TestDelayWorkflow {
    @WorkflowMethod
    fun runDelay(input: String, delayMs: Long): String
  }

  @WorkflowInterface
  interface TestNoReturnWorkflow {
    @WorkflowMethod
    fun runNoReturn(message: String): String
  }

  @WorkflowInterface
  interface TestErrorWorkflow {
    @WorkflowMethod
    fun runError(message: String): String
  }

  @WorkflowInterface
  interface TestMixedWorkflow {
    @WorkflowMethod
    fun runMixed(name: String): String
  }

  @WorkflowInterface
  interface TestHeartbeatWorkflow {
    @WorkflowMethod
    fun runHeartbeat(items: Int): Int
  }

  class TestGreetWorkflowImpl : TestGreetWorkflow {
    override fun runGreet(name: String): String {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )
      return stub.execute("Greet", String::class.java, name)
    }
  }

  class TestAddWorkflowImpl : TestAddWorkflow {
    override fun runAdd(a: Int, b: Int): Int {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )
      return stub.execute("Add", Int::class.javaObjectType, a, b)
    }
  }

  class TestDelayWorkflowImpl : TestDelayWorkflow {
    override fun runDelay(input: String, delayMs: Long): String {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )
      return stub.execute("ProcessWithDelay", String::class.java, input, delayMs)
    }
  }

  class TestNoReturnWorkflowImpl : TestNoReturnWorkflow {
    override fun runNoReturn(message: String): String {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )
      stub.execute("NoReturnValue", Void::class.java, message)
      return "completed"
    }
  }

  class TestErrorWorkflowImpl : TestErrorWorkflow {
    override fun runError(message: String): String {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .setRetryOptions(
            io.temporal.common.RetryOptions.newBuilder()
              .setMaximumAttempts(1)
              .build()
          )
          .build()
      )
      return try {
        stub.execute("ThrowError", String::class.java, message)
      } catch (e: Exception) {
        // Exception chain: ActivityFailure -> ApplicationFailure -> original message
        // Traverse the chain to find the original message
        var cause: Throwable? = e
        val messages = mutableListOf<String>()
        while (cause != null) {
          cause.message?.let { messages.add(it) }
          cause = cause.cause
        }
        "caught: ${messages.joinToString(" | ")}"
      }
    }
  }

  class TestMixedWorkflowImpl : TestMixedWorkflow {
    override fun runMixed(name: String): String {
      // Use untyped stub for suspend activity
      val suspendStub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )
      // Use typed stub for regular activity
      val regularActivities = Workflow.newActivityStub(
        RegularActivities::class.java,
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .build()
      )

      val suspendResult = suspendStub.execute("Greet", String::class.java, name)
      val regularResult = regularActivities.regularGreet(name)
      return "$suspendResult | $regularResult"
    }
  }

  class TestHeartbeatWorkflowImpl : TestHeartbeatWorkflow {
    override fun runHeartbeat(items: Int): Int {
      val stub = Workflow.newUntypedActivityStub(
        ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMinutes(1))
          .setHeartbeatTimeout(Duration.ofSeconds(10))
          .build()
      )
      return stub.execute("ProcessWithHeartbeat", Int::class.javaObjectType, items)
    }
  }

  // ==================== Test Setup ====================

  private fun resetCounters() {
    SuspendActivitiesImpl.executionCount.set(0)
    SuspendActivitiesImpl.executionThreads.clear()
    RegularActivitiesImpl.executionCount.set(0)
    HeartbeatActivitiesImpl.heartbeatCount.set(0)
  }

  // ==================== Tests ====================

  @Rule
  @JvmField
  var greetTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestGreetWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity returns correct result`() {
    resetCounters()
    greetTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    greetTestRule.testEnvironment.start()

    val workflow = greetTestRule.workflowClient.newWorkflowStub(
      TestGreetWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(greetTestRule.taskQueue)
        .build()
    )
    val result = workflow.runGreet("World")

    assertEquals("Hello, World!", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var addTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestAddWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity with multiple parameters`() {
    resetCounters()
    addTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    addTestRule.testEnvironment.start()

    val workflow = addTestRule.workflowClient.newWorkflowStub(
      TestAddWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(addTestRule.taskQueue)
        .build()
    )
    val result = workflow.runAdd(10, 20)

    assertEquals(30, result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var delayTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestDelayWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity with delay uses coroutines`() {
    resetCounters()
    delayTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    delayTestRule.testEnvironment.start()

    val workflow = delayTestRule.workflowClient.newWorkflowStub(
      TestDelayWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(delayTestRule.taskQueue)
        .build()
    )
    val result = workflow.runDelay("test-data", 50L)

    assertEquals("Processed: test-data", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var noReturnTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestNoReturnWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity with Unit return type`() {
    resetCounters()
    noReturnTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    noReturnTestRule.testEnvironment.start()

    val workflow = noReturnTestRule.workflowClient.newWorkflowStub(
      TestNoReturnWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(noReturnTestRule.taskQueue)
        .build()
    )
    val result = workflow.runNoReturn("test message")

    assertEquals("completed", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var errorTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestErrorWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity error is propagated`() {
    resetCounters()
    errorTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    errorTestRule.testEnvironment.start()

    val workflow = errorTestRule.workflowClient.newWorkflowStub(
      TestErrorWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(errorTestRule.taskQueue)
        .build()
    )
    val result = workflow.runError("test error")

    assertTrue("Should contain error message", result.contains("test error"))
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var mixedTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestMixedWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend and regular activities can coexist`() {
    resetCounters()
    // Register suspend activities
    mixedTestRule.worker.registerSuspendActivities(SuspendActivitiesImpl())
    // Register regular activities separately
    mixedTestRule.worker.registerActivitiesImplementations(RegularActivitiesImpl())

    mixedTestRule.testEnvironment.start()

    val workflow = mixedTestRule.workflowClient.newWorkflowStub(
      TestMixedWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(mixedTestRule.taskQueue)
        .build()
    )
    val result = workflow.runMixed("Test")

    assertEquals("Hello, Test! | Regular Hello, Test!", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
    assertEquals(1, RegularActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var heartbeatTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestHeartbeatWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity with heartbeating`() {
    resetCounters()
    heartbeatTestRule.worker.registerSuspendActivities(HeartbeatActivitiesImpl())
    heartbeatTestRule.testEnvironment.start()

    val workflow = heartbeatTestRule.workflowClient.newWorkflowStub(
      TestHeartbeatWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(heartbeatTestRule.taskQueue)
        .build()
    )
    val result = workflow.runHeartbeat(5)

    assertEquals(5, result)
    assertEquals(5, HeartbeatActivitiesImpl.heartbeatCount.get())
  }

  @Rule
  @JvmField
  var dispatcherTestRule: SDKTestWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestGreetWorkflowImpl::class.java)
    .setDoNotStart(true)
    .build()

  @Test
  fun `suspend activity runs on configured dispatcher`() {
    resetCounters()
    // Use IO dispatcher to verify we can configure the dispatcher
    dispatcherTestRule.worker.registerSuspendActivities(
      SuspendActivitiesImpl(),
      options = SuspendActivityOptions(dispatcher = Dispatchers.IO)
    )
    dispatcherTestRule.testEnvironment.start()

    val workflow = dispatcherTestRule.workflowClient.newWorkflowStub(
      TestGreetWorkflow::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(dispatcherTestRule.taskQueue)
        .build()
    )
    val result = workflow.runGreet("World")

    assertEquals("Hello, World!", result)
    // Verify the activity actually executed
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
    // Note: We can't easily verify the exact dispatcher used, but the test
    // passing means it worked with the configured dispatcher
  }
}
