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

package io.temporal.kotlin.internal

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.common.v1.WorkflowType
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(InternalTemporalApi::class)
class KotlinWorkflowImplementationFactoryTest {

  @WorkflowInterface
  interface TestSuspendWorkflow {
    @WorkflowMethod
    suspend fun execute(input: String): String
  }

  class TestSuspendWorkflowImpl : TestSuspendWorkflow {
    override suspend fun execute(input: String): String {
      return "Result: $input"
    }
  }

  @WorkflowInterface
  interface AnotherSuspendWorkflow {
    @WorkflowMethod
    suspend fun run(): Unit
  }

  class AnotherSuspendWorkflowImpl : AnotherSuspendWorkflow {
    override suspend fun run() {}
  }

  @WorkflowInterface
  interface NonSuspendWorkflow {
    @WorkflowMethod
    fun execute(): String
  }

  class NonSuspendWorkflowImpl : NonSuspendWorkflow {
    override fun execute(): String = "done"
  }

  @Test
  fun `registerWorkflowImplementationType registers suspend workflow`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    factory.registerWorkflowImplementationType(TestSuspendWorkflowImpl::class.java)

    assertTrue(factory.isAnyTypeSupported())
    assertTrue(factory.getRegisteredWorkflowTypes().contains("TestSuspendWorkflow"))
  }

  @Test
  fun `registerWorkflowImplementationTypes registers multiple workflows`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    factory.registerWorkflowImplementationTypes(
      TestSuspendWorkflowImpl::class.java,
      AnotherSuspendWorkflowImpl::class.java
    )

    assertEquals(2, factory.getRegisteredWorkflowTypes().size)
    assertTrue(factory.getRegisteredWorkflowTypes().contains("TestSuspendWorkflow"))
    assertTrue(factory.getRegisteredWorkflowTypes().contains("AnotherSuspendWorkflow"))
  }

  @Test(expected = IllegalArgumentException::class)
  fun `registerWorkflowImplementationType throws for non-suspend workflow`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    factory.registerWorkflowImplementationType(NonSuspendWorkflowImpl::class.java)
  }

  @Test(expected = IllegalStateException::class)
  fun `registerWorkflowImplementationType throws for duplicate registration`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    factory.registerWorkflowImplementationType(TestSuspendWorkflowImpl::class.java)
    factory.registerWorkflowImplementationType(TestSuspendWorkflowImpl::class.java)
  }

  @Test
  fun `getWorkflow returns null for unregistered workflow type`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)
    factory.registerWorkflowImplementationType(TestSuspendWorkflowImpl::class.java)

    val workflowType = WorkflowType.newBuilder().setName("UnknownWorkflow").build()
    val workflowExecution = WorkflowExecution.newBuilder()
      .setWorkflowId("test-id")
      .setRunId("test-run-id")
      .build()

    val result = factory.getWorkflow(workflowType, workflowExecution)

    assertNull(result)
  }

  @Test
  fun `getWorkflow returns ReplayWorkflow for registered workflow type`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)
    factory.registerWorkflowImplementationType(TestSuspendWorkflowImpl::class.java)

    val workflowType = WorkflowType.newBuilder().setName("TestSuspendWorkflow").build()
    val workflowExecution = WorkflowExecution.newBuilder()
      .setWorkflowId("test-id")
      .setRunId("test-run-id")
      .build()

    val result = factory.getWorkflow(workflowType, workflowExecution)

    assertNotNull(result)
    assertTrue(result is KotlinReplayWorkflow)
  }

  @Test
  fun `isAnyTypeSupported returns false when no workflows registered`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    assertFalse(factory.isAnyTypeSupported())
  }

  @Test
  fun `getRegisteredWorkflowTypes returns empty set when no workflows registered`() {
    val factory = KotlinWorkflowImplementationFactory(DefaultDataConverter.STANDARD_INSTANCE)

    assertTrue(factory.getRegisteredWorkflowTypes().isEmpty())
  }
}
