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
import io.temporal.client.WorkflowUpdateException
import io.temporal.common.converter.DataConverter
import io.temporal.failure.ApplicationFailure
import io.temporal.kotlin.internal.workflow.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.UpdateMethod
import io.temporal.workflow.UpdateValidatorMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test

/**
 * Tests for suspend update methods with validators.
 *
 * This test verifies that Kotlin suspend update methods work correctly with
 * non-suspend validator methods. The challenge is that suspend functions have
 * an extra Continuation parameter at bytecode level, which needs to be handled
 * when comparing update method and validator method parameters.
 */
class SuspendUpdateValidatorTest {

  // ==================== Workflow Interface ====================

  @WorkflowInterface
  interface UpdateWithValidatorWorkflow {
    @WorkflowMethod
    suspend fun execute(): List<String>

    /**
     * Suspend update method - can call activities and other workflow operations.
     */
    @UpdateMethod
    suspend fun addGreeting(name: String): Int

    /**
     * Validator for the update method. Validators must NOT be suspend functions
     * because they need to return synchronously to accept/reject the update.
     */
    @UpdateValidatorMethod(updateName = "addGreeting")
    fun addGreetingValidator(name: String)

    @SignalMethod
    fun exit()
  }

  // ==================== Workflow Implementation ====================

  class UpdateWithValidatorWorkflowImpl : UpdateWithValidatorWorkflow {
    private val greetings = mutableListOf<String>()
    private var exit = false

    override suspend fun execute(): List<String> {
      KWorkflow.awaitCondition { exit }
      return greetings.toList()
    }

    override suspend fun addGreeting(name: String): Int {
      greetings.add("Hello $name")
      return greetings.size
    }

    override fun addGreetingValidator(name: String) {
      if (name.isBlank()) {
        throw ApplicationFailure.newFailure("Name cannot be blank", "ValidationError")
      }
      if (greetings.size >= 10) {
        throw ApplicationFailure.newFailure("Maximum greetings reached", "LimitExceeded")
      }
    }

    override fun exit() {
      exit = true
    }
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

  @Test
  fun `suspend update method with validator can be registered and executed`() {
    // This test verifies that a workflow with a suspend update method and
    // a non-suspend validator can be registered and executed successfully.
    // The SDK should handle the Continuation parameter difference.
    setupKotlinWorkflows(UpdateWithValidatorWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Use untyped stub to avoid Kotlin suspend function issues
    val stub = client.newUntypedWorkflowStub("UpdateWithValidatorWorkflow", options)
    stub.start()

    // Send update - this should work
    val count = stub.update("addGreeting", Int::class.java, "World")
    assertEquals(1, count)

    // Signal exit
    stub.signal("exit")

    // Get result
    @Suppress("UNCHECKED_CAST")
    val result = stub.getResult(List::class.java) as List<String>

    assertTrue(result.contains("Hello World"))
  }

  @Test
  fun `validator rejects invalid update`() {
    setupKotlinWorkflows(UpdateWithValidatorWorkflowImpl::class.java)

    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder()
      .setTaskQueue(testWorkflowRule.taskQueue)
      .build()

    // Use untyped stub
    val stub = client.newUntypedWorkflowStub("UpdateWithValidatorWorkflow", options)
    stub.start()

    // Try to send update with blank name - should be rejected by validator
    try {
      stub.update("addGreeting", Int::class.java, "")
      fail("Expected WorkflowUpdateException")
    } catch (e: WorkflowUpdateException) {
      // Expected - validator rejected the update
      // Check that the error message contains the validation error somewhere in the chain
      val messageChain = generateSequence<Throwable>(e) { it.cause }
        .mapNotNull { it.message }
        .joinToString(" | ")
      assertTrue(
        "Expected 'Name cannot be blank' in exception chain but got: $messageChain",
        messageChain.contains("Name cannot be blank")
      )
    }

    // Signal exit to complete workflow
    stub.signal("exit")
    stub.getResult(List::class.java)
  }
}
