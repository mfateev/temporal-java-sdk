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

import io.temporal.kotlin.internal.workflow.KotlinWorkflowDefinition
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.UpdateMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(InternalTemporalApi::class)
class KotlinWorkflowDefinitionTest {

  @WorkflowInterface
  interface SuspendWorkflow {
    @WorkflowMethod
    suspend fun run(input: String): String

    @SignalMethod
    suspend fun signal(value: Int)

    @QueryMethod
    fun query(): String

    @UpdateMethod
    suspend fun update(value: String): String
  }

  class SuspendWorkflowImpl : SuspendWorkflow {
    private var state = ""

    override suspend fun run(input: String): String {
      state = input
      return "result: $input"
    }

    override suspend fun signal(value: Int) {
      state = "signaled: $value"
    }

    override fun query(): String {
      return state
    }

    override suspend fun update(value: String): String {
      state = value
      return "updated: $value"
    }
  }

  @WorkflowInterface
  interface NonSuspendWorkflow {
    @WorkflowMethod
    fun run(input: String): String
  }

  class NonSuspendWorkflowImpl : NonSuspendWorkflow {
    override fun run(input: String): String {
      return input
    }
  }

  @WorkflowInterface
  interface NamedWorkflow {
    @WorkflowMethod(name = "CustomWorkflowName")
    suspend fun execute(): Unit
  }

  class NamedWorkflowImpl : NamedWorkflow {
    override suspend fun execute() {}
  }

  @Test
  fun `isSuspendWorkflow returns true for suspend workflow`() {
    assertTrue(KotlinWorkflowDefinition.isSuspendWorkflow(SuspendWorkflowImpl::class.java))
  }

  @Test
  fun `isSuspendWorkflow returns false for non-suspend workflow`() {
    assertFalse(KotlinWorkflowDefinition.isSuspendWorkflow(NonSuspendWorkflowImpl::class.java))
  }

  @Test
  fun `fromImplementationClass extracts workflow type name from interface`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    assertEquals("SuspendWorkflow", definition.workflowTypeName)
  }

  @Test
  fun `fromImplementationClass uses custom name from annotation`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(NamedWorkflowImpl::class)

    assertEquals("CustomWorkflowName", definition.workflowTypeName)
  }

  @Test
  fun `fromImplementationClass detects suspend function`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    assertTrue(definition.isSuspendFunction)
  }

  @Test
  fun `fromImplementationClass extracts signal methods`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    assertEquals(1, definition.signalMethods.size)
    assertTrue(definition.signalMethods.containsKey("signal"))
  }

  @Test
  fun `fromImplementationClass extracts query methods`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    assertEquals(1, definition.queryMethods.size)
    assertTrue(definition.queryMethods.containsKey("query"))
  }

  @Test
  fun `fromImplementationClass extracts update methods`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    assertEquals(1, definition.updateMethods.size)
    assertTrue(definition.updateMethods.containsKey("update"))
  }

  @Test
  fun `createInstance creates new workflow instance`() {
    val definition = KotlinWorkflowDefinition.fromImplementationClass(SuspendWorkflowImpl::class)

    val instance = definition.createInstance()

    assertNotNull(instance)
    assertTrue(instance is SuspendWorkflowImpl)
  }

  @Test(expected = IllegalArgumentException::class)
  fun `fromImplementationClass throws for class without WorkflowInterface`() {
    KotlinWorkflowDefinition.fromImplementationClass(String::class)
  }
}
