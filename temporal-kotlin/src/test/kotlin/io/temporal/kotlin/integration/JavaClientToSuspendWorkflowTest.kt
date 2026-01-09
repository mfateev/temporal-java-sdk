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
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test

/**
 * Tests that Java clients can call Kotlin workflows with suspend functions.
 *
 * Key findings:
 * - Java cannot directly use Kotlin suspend interfaces (suspend functions compile to
 *   methods with an extra Continuation parameter and Object return type)
 * - Java clients CAN invoke Kotlin suspend workflows using untyped workflow stubs
 * - The workflow type name defaults to the interface name (e.g., "SuspendGreetingWorkflow")
 * - Java clients could also define their own Java interface with the SAME name as the
 *   Kotlin interface to use typed stubs
 */
class JavaClientToSuspendWorkflowTest {

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    setDoNotStart(true)
  }

  private fun setupWorkflow() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerWorkflowImplementationType(SuspendGreetingWorkflowImpl::class.java)
    testWorkflowRule.worker.registerWorkflowImplementationFactory(factory)
    testWorkflowRule.testEnvironment.start()
  }

  // Kotlin suspend workflow interface
  @WorkflowInterface
  interface SuspendGreetingWorkflow {
    @WorkflowMethod
    suspend fun greet(name: String): String
  }

  // Kotlin implementation using suspend
  class SuspendGreetingWorkflowImpl : SuspendGreetingWorkflow {
    override suspend fun greet(name: String): String {
      delay(100) // Use coroutine delay
      return "Hello, $name!"
    }
  }

  @Test
  fun javaClientCanCallSuspendWorkflowUsingUntypedStub() {
    setupWorkflow()
    val client = testWorkflowRule.workflowClient

    // Java would use untyped stub - workflow type defaults to interface name
    val stub = client.newUntypedWorkflowStub(
      "SuspendGreetingWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(testWorkflowRule.taskQueue)
        .build()
    )

    // Start the workflow and get result - this is how Java clients would call it
    val execution = stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
  }

  @Test
  fun kotlinClientCanCallSuspendWorkflowUsingUntypedStub() {
    setupWorkflow()
    val client = testWorkflowRule.workflowClient

    // Kotlin code should also use untyped stubs for suspend workflows
    // because Java dynamic proxies don't support Kotlin suspend functions on the client side.
    // The suspend interface is for the WORKER side, not the client side.
    val stub = client.newUntypedWorkflowStub(
      "SuspendGreetingWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(testWorkflowRule.taskQueue)
        .build()
    )

    stub.start("World")
    val result = stub.getResult(String::class.java)

    assertEquals("Hello, World!", result)
  }
}
