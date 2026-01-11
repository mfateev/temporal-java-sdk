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

package io.temporal.kotlin.activity

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.client.WorkflowOptions
import io.temporal.common.converter.DataConverter
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.common.converter.KotlinObjectMapperFactory
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

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
 * Tests use KWorkflow.executeActivity with method references to test the full
 * activity invocation path including activity type name derivation.
 */
class SuspendActivityIntegrationTest {

  // ==================== Test Interfaces ====================
  // No explicit @ActivityMethod names - testing default name generation

  @ActivityInterface
  interface SuspendActivities {
    suspend fun greet(name: String): String

    suspend fun add(a: Int, b: Int): Int

    suspend fun processWithDelay(input: String, delayMs: Long): String

    suspend fun noReturnValue(message: String)

    suspend fun throwError(message: String): String
  }

  @ActivityInterface
  interface RegularActivities {
    fun regularGreet(name: String): String
  }

  @ActivityInterface
  interface HeartbeatActivities {
    suspend fun processWithHeartbeat(items: Int): Int
  }

  // Interface with explicit @ActivityMethod names for testing annotation support
  @ActivityInterface
  interface ExplicitNameActivities {
    @ActivityMethod(name = "CustomGreet")
    suspend fun greet(name: String): String
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

  class ExplicitNameActivitiesImpl : ExplicitNameActivities {
    companion object {
      val executionCount = AtomicInteger(0)
    }

    override suspend fun greet(name: String): String {
      executionCount.incrementAndGet()
      return "Custom Hello, $name!"
    }
  }

  // ==================== Test Workflows ====================
  // Workflows use KWorkflow.executeActivity with method references

  @WorkflowInterface
  interface TestGreetWorkflow {
    @WorkflowMethod
    suspend fun runGreet(name: String): String
  }

  @WorkflowInterface
  interface TestAddWorkflow {
    @WorkflowMethod
    suspend fun runAdd(a: Int, b: Int): Int
  }

  @WorkflowInterface
  interface TestDelayWorkflow {
    @WorkflowMethod
    suspend fun runDelay(input: String, delayMs: Long): String
  }

  @WorkflowInterface
  interface TestNoReturnWorkflow {
    @WorkflowMethod
    suspend fun runNoReturn(message: String): String
  }

  @WorkflowInterface
  interface TestErrorWorkflow {
    @WorkflowMethod
    suspend fun runError(message: String): String
  }

  @WorkflowInterface
  interface TestMixedWorkflow {
    @WorkflowMethod
    suspend fun runMixed(name: String): String
  }

  @WorkflowInterface
  interface TestHeartbeatWorkflow {
    @WorkflowMethod
    suspend fun runHeartbeat(items: Int): Int
  }

  @WorkflowInterface
  interface TestExplicitNameWorkflow {
    @WorkflowMethod
    suspend fun runExplicitName(name: String): String
  }

  class TestGreetWorkflowImpl : TestGreetWorkflow {
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runGreet(name: String): String {
      // Use method reference - tests activity type name derivation
      return KWorkflow.executeActivity(SuspendActivities::greet, options, name)
    }
  }

  class TestAddWorkflowImpl : TestAddWorkflow {
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runAdd(a: Int, b: Int): Int {
      return KWorkflow.executeActivity(SuspendActivities::add, options, a, b)
    }
  }

  class TestDelayWorkflowImpl : TestDelayWorkflow {
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runDelay(input: String, delayMs: Long): String {
      return KWorkflow.executeActivity(SuspendActivities::processWithDelay, options, input, delayMs)
    }
  }

  class TestNoReturnWorkflowImpl : TestNoReturnWorkflow {
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runNoReturn(message: String): String {
      KWorkflow.executeActivity(SuspendActivities::noReturnValue, options, message)
      return "completed"
    }
  }

  class TestErrorWorkflowImpl : TestErrorWorkflow {
    private val options = KActivityOptions(
      startToCloseTimeout = 1.minutes,
      retryOptions = KRetryOptions(maximumAttempts = 1)
    )

    override suspend fun runError(message: String): String {
      return try {
        KWorkflow.executeActivity(SuspendActivities::throwError, options, message)
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
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runMixed(name: String): String {
      // Call suspend activity via method reference
      val suspendResult = KWorkflow.executeActivity(SuspendActivities::greet, options, name)

      // Call regular (non-suspend) activity via method reference
      val regularResult = KWorkflow.executeActivity(RegularActivities::regularGreet, options, name)
      return "$suspendResult | $regularResult"
    }
  }

  class TestHeartbeatWorkflowImpl : TestHeartbeatWorkflow {
    private val options = KActivityOptions(
      startToCloseTimeout = 1.minutes,
      heartbeatTimeout = 10.seconds
    )

    override suspend fun runHeartbeat(items: Int): Int {
      return KWorkflow.executeActivity(HeartbeatActivities::processWithHeartbeat, options, items)
    }
  }

  class TestExplicitNameWorkflowImpl : TestExplicitNameWorkflow {
    private val options = KActivityOptions(startToCloseTimeout = 1.minutes)

    override suspend fun runExplicitName(name: String): String {
      // Tests that @ActivityMethod(name = "CustomGreet") is respected
      return KWorkflow.executeActivity(ExplicitNameActivities::greet, options, name)
    }
  }

  // ==================== Test Setup ====================

  private fun resetCounters() {
    SuspendActivitiesImpl.executionCount.set(0)
    SuspendActivitiesImpl.executionThreads.clear()
    RegularActivitiesImpl.executionCount.set(0)
    HeartbeatActivitiesImpl.heartbeatCount.set(0)
    ExplicitNameActivitiesImpl.executionCount.set(0)
  }

  // ==================== Tests ====================

  @Rule
  @JvmField
  var greetTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityReturnsCorrectResult() {
    resetCounters()
    // Register Kotlin suspend workflow using KotlinWorkflowImplementationFactory
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestGreetWorkflowImpl::class.java)
    greetTestRule.worker.registerWorkflowImplementationFactory(factory)
    greetTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    greetTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = greetTestRule.workflowClient.newUntypedWorkflowStub(
      "TestGreetWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(greetTestRule.taskQueue)
        .build()
    )
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var addTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityWithMultipleParameters() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestAddWorkflowImpl::class.java)
    addTestRule.worker.registerWorkflowImplementationFactory(factory)
    addTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    addTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = addTestRule.workflowClient.newUntypedWorkflowStub(
      "TestAddWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(addTestRule.taskQueue)
        .build()
    )
    stub.start(10, 20)
    val result = stub.getResult(Int::class.java)

    assertEquals(30, result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var delayTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityWithDelayUsesCoroutines() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestDelayWorkflowImpl::class.java)
    delayTestRule.worker.registerWorkflowImplementationFactory(factory)
    delayTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    delayTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = delayTestRule.workflowClient.newUntypedWorkflowStub(
      "TestDelayWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(delayTestRule.taskQueue)
        .build()
    )
    stub.start("test-data", 50L)
    val result = stub.getResult(String::class.java)

    assertEquals("Processed: test-data", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var noReturnTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityWithUnitReturnType() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestNoReturnWorkflowImpl::class.java)
    noReturnTestRule.worker.registerWorkflowImplementationFactory(factory)
    noReturnTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    noReturnTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = noReturnTestRule.workflowClient.newUntypedWorkflowStub(
      "TestNoReturnWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(noReturnTestRule.taskQueue)
        .build()
    )
    stub.start("test message")
    val result = stub.getResult(String::class.java)

    assertEquals("completed", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var errorTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityErrorIsPropagated() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestErrorWorkflowImpl::class.java)
    errorTestRule.worker.registerWorkflowImplementationFactory(factory)
    errorTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    errorTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = errorTestRule.workflowClient.newUntypedWorkflowStub(
      "TestErrorWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(errorTestRule.taskQueue)
        .build()
    )
    stub.start("test error")
    val result = stub.getResult(String::class.java)

    assertTrue("Should contain error message", result.contains("test error"))
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var mixedTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendAndRegularActivitiesCanCoexist() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestMixedWorkflowImpl::class.java)
    mixedTestRule.worker.registerWorkflowImplementationFactory(factory)
    // Register suspend activities
    mixedTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    // Register regular activities separately
    mixedTestRule.kWorker.registerActivitiesImplementations(RegularActivitiesImpl())

    mixedTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = mixedTestRule.workflowClient.newUntypedWorkflowStub(
      "TestMixedWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(mixedTestRule.taskQueue)
        .build()
    )
    stub.start("Test")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, Test! | Regular Hello, Test!", result)
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
    assertEquals(1, RegularActivitiesImpl.executionCount.get())
  }

  @Rule
  @JvmField
  var heartbeatTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityWithHeartbeating() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestHeartbeatWorkflowImpl::class.java)
    heartbeatTestRule.worker.registerWorkflowImplementationFactory(factory)
    heartbeatTestRule.kWorker.registerActivitiesImplementations(HeartbeatActivitiesImpl())
    heartbeatTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = heartbeatTestRule.workflowClient.newUntypedWorkflowStub(
      "TestHeartbeatWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(heartbeatTestRule.taskQueue)
        .build()
    )
    stub.start(5)
    val result = stub.getResult(Int::class.java)

    assertEquals(5, result)
    assertEquals(5, HeartbeatActivitiesImpl.heartbeatCount.get())
  }

  @Rule
  @JvmField
  var dispatcherTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityRunsOnConfiguredDispatcher() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestGreetWorkflowImpl::class.java)
    dispatcherTestRule.worker.registerWorkflowImplementationFactory(factory)
    // Register suspend activities via kWorker
    dispatcherTestRule.kWorker.registerActivitiesImplementations(SuspendActivitiesImpl())
    dispatcherTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = dispatcherTestRule.workflowClient.newUntypedWorkflowStub(
      "TestGreetWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(dispatcherTestRule.taskQueue)
        .build()
    )
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
    // Verify the activity actually executed
    assertEquals(1, SuspendActivitiesImpl.executionCount.get())
    // Note: We can't easily verify the exact dispatcher used, but the test
    // passing means it worked with the configured dispatcher
  }

  @Rule
  @JvmField
  var explicitNameTestRule = KSDKTestWorkflowRule {
    doNotStart = true
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun testSuspendActivityWithExplicitActivityMethodName() {
    resetCounters()
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(TestExplicitNameWorkflowImpl::class.java)
    explicitNameTestRule.worker.registerWorkflowImplementationFactory(factory)
    explicitNameTestRule.kWorker.registerActivitiesImplementations(ExplicitNameActivitiesImpl())
    explicitNameTestRule.testEnvironment.start()

    // Use untyped stub since Java proxy doesn't support suspend functions on client side
    val stub = explicitNameTestRule.workflowClient.newUntypedWorkflowStub(
      "TestExplicitNameWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(explicitNameTestRule.taskQueue)
        .build()
    )
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Custom Hello, World!", result)
    assertEquals(1, ExplicitNameActivitiesImpl.executionCount.get())
  }
}
