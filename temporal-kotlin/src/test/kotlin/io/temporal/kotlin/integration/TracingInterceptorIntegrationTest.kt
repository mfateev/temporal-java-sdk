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

@file:OptIn(
  io.temporal.kotlin.internal.InternalTemporalApi::class,
  kotlin.time.ExperimentalTime::class
)

package io.temporal.kotlin.integration

import io.temporal.client.WorkflowOptions
import io.temporal.kotlin.interceptor.KActivityInvocationInput
import io.temporal.kotlin.interceptor.KCancelWorkflowInput
import io.temporal.kotlin.interceptor.KChildWorkflowInvocationInput
import io.temporal.kotlin.interceptor.KContinueAsNewInput
import io.temporal.kotlin.interceptor.KLocalActivityInvocationInput
import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalExternalInput
import io.temporal.kotlin.interceptor.KSignalInput
import io.temporal.kotlin.interceptor.KUpdateInput
import io.temporal.kotlin.interceptor.KUpdateOutput
import io.temporal.kotlin.interceptor.KWorkerInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowInput
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptorBase
import io.temporal.kotlin.interceptor.KWorkflowOutput
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.worker.KotlinPlugin
import io.temporal.kotlin.worker.KotlinPluginOptions
import io.temporal.kotlin.workflow.KChildWorkflowHandle
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.util.Random
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration

/**
 * Comprehensive integration tests for Kotlin workflow interceptors.
 *
 * This test verifies that all inbound and outbound interceptor methods are properly
 * invoked during workflow execution, similar to Java SDK's TracingWorkerInterceptor.
 */
class TracingInterceptorIntegrationTest {

  // ==================== Trace Collection ====================

  /**
   * Thread-safe trace collector for interceptor calls.
   */
  class FilteredTrace {
    private val entries = CopyOnWriteArrayList<String>()

    fun add(entry: String) {
      entries.add(entry)
    }

    fun getEntries(): List<String> = entries.toList()

    fun clear() {
      entries.clear()
    }

    fun getTrace(): String = entries.joinToString("\n")
  }

  // ==================== Tracing Interceptor ====================

  /**
   * A comprehensive tracing interceptor that records all workflow operations.
   */
  class TracingWorkerInterceptor(private val trace: FilteredTrace) : KWorkerInterceptorBase() {

    override fun interceptWorkflow(
      next: KWorkflowInboundCallsInterceptor
    ): KWorkflowInboundCallsInterceptor {
      return TracingWorkflowInboundInterceptor(trace, next)
    }
  }

  /**
   * Inbound interceptor that traces workflow method calls.
   */
  class TracingWorkflowInboundInterceptor(
    private val trace: FilteredTrace,
    next: KWorkflowInboundCallsInterceptor
  ) : KWorkflowInboundCallsInterceptorBase(next) {

    override suspend fun init(outboundCalls: KWorkflowOutboundCallsInterceptor) {
      trace.add("init")
      // Wrap outbound calls with tracing interceptor
      val tracingOutbound = TracingWorkflowOutboundInterceptor(trace, outboundCalls)
      next.init(tracingOutbound)
    }

    override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
      trace.add("execute:start")
      val result = next.execute(input)
      trace.add("execute:end")
      return result
    }

    override suspend fun handleSignal(input: KSignalInput) {
      trace.add("handleSignal:${input.signalName}")
      next.handleSignal(input)
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      trace.add("handleQuery:${input.queryName}")
      return next.handleQuery(input)
    }

    override fun validateUpdate(input: KUpdateInput) {
      trace.add("validateUpdate:${input.updateName}")
      next.validateUpdate(input)
    }

    override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
      trace.add("executeUpdate:${input.updateName}")
      return next.executeUpdate(input)
    }
  }

  /**
   * Outbound interceptor that traces workflow operations.
   */
  class TracingWorkflowOutboundInterceptor(
    private val trace: FilteredTrace,
    next: KWorkflowOutboundCallsInterceptor
  ) : KWorkflowOutboundCallsInterceptorBase(next) {

    override suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R {
      trace.add("executeActivity:${input.activityName}")
      return next.executeActivity(input)
    }

    override suspend fun <R> executeLocalActivity(input: KLocalActivityInvocationInput<R>): R {
      trace.add("executeLocalActivity:${input.activityName}")
      return next.executeLocalActivity(input)
    }

    override suspend fun <T, R> startChildWorkflow(
      input: KChildWorkflowInvocationInput<R>
    ): KChildWorkflowHandle<T, R> {
      trace.add("startChildWorkflow:${input.workflowType}")
      return next.startChildWorkflow(input)
    }

    override suspend fun delay(duration: Duration) {
      trace.add("delay:${duration.inWholeMilliseconds}ms")
      next.delay(duration)
    }

    override suspend fun awaitCondition(timeout: Duration, reason: String, condition: () -> Boolean): Boolean {
      trace.add("awaitCondition:$reason")
      return next.awaitCondition(timeout, reason, condition)
    }

    override suspend fun awaitCondition(reason: String, condition: () -> Boolean) {
      trace.add("awaitCondition:$reason")
      next.awaitCondition(reason, condition)
    }

    override fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
      trace.add("sideEffect")
      return next.sideEffect(resultClass, func)
    }

    override fun <R> mutableSideEffect(
      id: String,
      resultClass: Class<R>,
      updated: (R?, R?) -> Boolean,
      func: () -> R
    ): R {
      trace.add("mutableSideEffect:$id")
      return next.mutableSideEffect(id, resultClass, updated, func)
    }

    override fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
      trace.add("getVersion:$changeId")
      return next.getVersion(changeId, minSupported, maxSupported)
    }

    override fun continueAsNew(input: KContinueAsNewInput): Nothing {
      trace.add("continueAsNew:${input.workflowType}")
      next.continueAsNew(input)
    }

    override suspend fun signalExternalWorkflow(input: KSignalExternalInput) {
      trace.add("signalExternalWorkflow:${input.signalName}")
      next.signalExternalWorkflow(input)
    }

    override suspend fun cancelWorkflow(input: KCancelWorkflowInput) {
      trace.add("cancelWorkflow:${input.execution.workflowId}")
      next.cancelWorkflow(input)
    }

    override fun newRandom(): Random {
      trace.add("newRandom")
      return next.newRandom()
    }

    override fun randomUUID(): UUID {
      trace.add("randomUUID")
      return next.randomUUID()
    }

    override fun currentTimeMillis(): Long {
      trace.add("currentTimeMillis")
      return next.currentTimeMillis()
    }
  }

  // ==================== Workflow Interfaces ====================

  @WorkflowInterface
  interface SimpleTracingWorkflow {
    @WorkflowMethod
    suspend fun execute(input: String): String
  }

  @WorkflowInterface
  interface WorkflowWithSignalAndQuery {
    @WorkflowMethod
    suspend fun execute(): String

    @SignalMethod
    suspend fun receiveSignal(message: String)

    @QueryMethod
    fun getState(): String
  }

  // ==================== Workflow Implementations ====================

  class SimpleTracingWorkflowImpl : SimpleTracingWorkflow {
    override suspend fun execute(input: String): String {
      // Test various outbound calls that go through the interceptor
      val random = KWorkflow.newRandom()
      val uuid = KWorkflow.randomUUID()
      val time = KWorkflow.currentTimeMillis()

      return "Result: $input, random=${random.nextInt(100)}, uuid=$uuid"
    }
  }

  class WorkflowWithSignalAndQueryImpl : WorkflowWithSignalAndQuery {
    private var state = "initial"
    private var receivedSignal = false

    override suspend fun execute(): String {
      // Wait for signal using awaitCondition
      KWorkflow.awaitCondition { receivedSignal }
      return "Completed with state: $state"
    }

    override suspend fun receiveSignal(message: String) {
      state = message
      receivedSignal = true
    }

    override fun getState(): String {
      return state
    }
  }

  // ==================== Test Setup ====================

  private val trace = FilteredTrace()
  private val tracingInterceptor = TracingWorkerInterceptor(trace)

  private val kotlinPlugin = KotlinPlugin.create(
    KotlinPluginOptions(
      workerInterceptors = listOf(tracingInterceptor)
    )
  )

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    workerFactoryOptions {
      addPlugin(kotlinPlugin)
    }
    workflowTypes(
      SimpleTracingWorkflowImpl::class,
      WorkflowWithSignalAndQueryImpl::class
    )
  }

  // ==================== Tests ====================

  @Test
  fun `interceptor traces workflow execution lifecycle`() {
    trace.clear()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SimpleTracingWorkflow", options)
    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertTrue("Result should contain 'World'", result.contains("World"))

    // Verify interceptor calls
    val entries = trace.getEntries()
    println("Trace entries:\n${entries.joinToString("\n")}")

    // Verify inbound interceptor init was called
    assertTrue("Should have init call", entries.contains("init"))

    // Verify execute lifecycle
    assertTrue("Should have execute:start", entries.contains("execute:start"))
    assertTrue("Should have execute:end", entries.contains("execute:end"))

    // Verify outbound calls were traced
    assertTrue("Should have newRandom call", entries.contains("newRandom"))
    assertTrue("Should have randomUUID call", entries.contains("randomUUID"))
    assertTrue("Should have currentTimeMillis call", entries.contains("currentTimeMillis"))
    // Note: kotlinx.coroutines.delay() bypasses interceptors - it's handled by workflow dispatcher directly
    // To trace delays, use the explicit KWorkflow.delay() method or awaitCondition with timeout

    // Verify order: init should come before execute
    val initIndex = entries.indexOf("init")
    val executeStartIndex = entries.indexOf("execute:start")
    val executeEndIndex = entries.indexOf("execute:end")

    assertTrue("init should come before execute:start", initIndex < executeStartIndex)
    assertTrue("execute:start should come before execute:end", executeStartIndex < executeEndIndex)
  }

  @Test
  fun `interceptor traces signal handling`() {
    trace.clear()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("WorkflowWithSignalAndQuery", options)
    stub.start()

    // Wait a bit then send signal
    Thread.sleep(500)
    stub.signal("receiveSignal", "new-state")

    val result = stub.getResult(String::class.java)

    assertEquals("Completed with state: new-state", result)

    // Verify signal handling was traced
    val entries = trace.getEntries()
    println("Trace entries:\n${entries.joinToString("\n")}")

    assertTrue("Should have handleSignal:receiveSignal", entries.contains("handleSignal:receiveSignal"))
  }

  @Test
  fun `interceptor traces query handling`() {
    trace.clear()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("WorkflowWithSignalAndQuery", options)
    stub.start()

    // Wait a bit then query
    Thread.sleep(500)
    val state = stub.query("getState", String::class.java)

    assertEquals("initial", state)

    // Send signal to complete workflow
    stub.signal("receiveSignal", "done")
    stub.getResult(String::class.java)

    // Verify query handling was traced
    val entries = trace.getEntries()
    println("Trace entries:\n${entries.joinToString("\n")}")

    assertTrue("Should have handleQuery:getState", entries.contains("handleQuery:getState"))
  }

  @Test
  fun `interceptor trace order is correct`() {
    trace.clear()

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    val stub = client.newUntypedWorkflowStub("SimpleTracingWorkflow", options)
    stub.start("Test")
    stub.getResult(String::class.java)

    val entries = trace.getEntries()
    println("Full trace:\n${trace.getTrace()}")

    // Verify the basic interceptor chain order
    // 1. init (outbound interceptor setup)
    // 2. execute:start (workflow begins)
    // 3. outbound calls (random, uuid, time, delay)
    // 4. execute:end (workflow completes)

    val initIdx = entries.indexOf("init")
    val executeStartIdx = entries.indexOf("execute:start")
    val executeEndIdx = entries.indexOf("execute:end")
    val newRandomIdx = entries.indexOf("newRandom")
    val randomUUIDIdx = entries.indexOf("randomUUID")

    assertTrue("init should be first", initIdx == 0)
    assertTrue("execute:start should follow init", executeStartIdx > initIdx)
    assertTrue("newRandom should be during execution", newRandomIdx > executeStartIdx && newRandomIdx < executeEndIdx)
    assertTrue("randomUUID should be during execution", randomUUIDIdx > executeStartIdx && randomUUIDIdx < executeEndIdx)
    assertTrue("execute:end should be last in execute block", executeEndIdx > newRandomIdx)
  }
}
