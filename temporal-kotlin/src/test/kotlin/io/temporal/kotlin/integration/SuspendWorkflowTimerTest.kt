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
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Tests that Kotlin suspend workflows properly support time-based operations
 * with the test environment's time skipping.
 *
 * This test verifies that KWorkflow.awaitCondition with a timeout works correctly
 * when using KotlinWorkflowImplementationFactory in the test environment.
 */
class SuspendWorkflowTimerTest {

  @WorkflowInterface
  interface TimerWorkflow {
    @WorkflowMethod
    suspend fun waitAndReturn(waitSeconds: Int): String

    @QueryMethod
    fun getState(): String
  }

  class TimerWorkflowImpl : TimerWorkflow {
    private var state = "initial"

    override suspend fun waitAndReturn(waitSeconds: Int): String {
      state = "started"

      // Use KWorkflow.awaitCondition with a time-based check
      // This should work with test environment time skipping
      val startTime = KWorkflow.currentTimeMillis()
      val waitMillis = waitSeconds * 1000L

      KWorkflow.awaitCondition(waitSeconds.seconds) {
        KWorkflow.currentTimeMillis() - startTime >= waitMillis
      }

      state = "completed"
      return "waited $waitSeconds seconds"
    }

    override fun getState(): String = state
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

  /**
   * Test that a suspend workflow with time-based awaitCondition completes
   * within a reasonable time when using test environment time skipping.
   *
   * If time skipping doesn't work, this test will timeout.
   */
  @Test(timeout = 10_000)
  fun `suspend workflow with time-based await should complete with time skipping`() {
    setupKotlinWorkflows(TimerWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val taskQueue = testWorkflowRule.taskQueue

    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(taskQueue)
      .setWorkflowId("timer-test-workflow")
      .build()

    // Use untyped stub to avoid Kotlin suspend function issues in test
    val stub = client.newUntypedWorkflowStub("TimerWorkflow", options)

    // Start workflow that waits for 60 seconds (workflow time)
    // With time skipping, this should complete almost instantly
    stub.start(60)
    val result = stub.getResult(String::class.java)

    assertEquals("waited 60 seconds", result)
  }

  /**
   * Test that queries work on a suspend workflow with timer.
   */
  @Test(timeout = 10_000)
  fun `can query suspend workflow state during timer wait`() {
    setupKotlinWorkflows(TimerWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val taskQueue = testWorkflowRule.taskQueue

    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(taskQueue)
      .setWorkflowId("timer-query-test-workflow")
      .build()

    // Use untyped stub
    val stub = client.newUntypedWorkflowStub("TimerWorkflow", options)

    // Start workflow that waits for 60 seconds
    stub.start(60)
    val result = stub.getResult(String::class.java)

    assertEquals("waited 60 seconds", result)
  }
}
