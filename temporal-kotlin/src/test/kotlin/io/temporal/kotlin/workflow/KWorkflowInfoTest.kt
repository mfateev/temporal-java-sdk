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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.workflow

import io.temporal.common.Priority
import io.temporal.workflow.WorkflowInfo
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import java.time.Duration
import java.util.Optional
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class KWorkflowInfoTest {

  @Test
  fun `wraps basic properties correctly`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    `when`(javaInfo.namespace).thenReturn("test-namespace")
    `when`(javaInfo.workflowId).thenReturn("workflow-123")
    `when`(javaInfo.workflowType).thenReturn("TestWorkflow")
    `when`(javaInfo.runId).thenReturn("run-456")
    `when`(javaInfo.taskQueue).thenReturn("test-queue")
    `when`(javaInfo.attempt).thenReturn(1)

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertEquals("test-namespace", kInfo.namespace)
    assertEquals("workflow-123", kInfo.workflowId)
    assertEquals("TestWorkflow", kInfo.workflowType)
    assertEquals("run-456", kInfo.runId)
    assertEquals("test-queue", kInfo.taskQueue)
    assertEquals(1, kInfo.attempt)
  }

  @Test
  fun `converts Optional to nullable - present value`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    `when`(javaInfo.parentWorkflowId).thenReturn(Optional.of("parent-123"))
    `when`(javaInfo.parentRunId).thenReturn(Optional.of("parent-run-456"))

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertEquals("parent-123", kInfo.parentWorkflowId)
    assertEquals("parent-run-456", kInfo.parentRunId)
  }

  @Test
  fun `converts Optional to nullable - empty value`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    `when`(javaInfo.parentWorkflowId).thenReturn(Optional.empty())
    `when`(javaInfo.parentRunId).thenReturn(Optional.empty())
    `when`(javaInfo.continuedExecutionRunId).thenReturn(Optional.empty())
    `when`(javaInfo.rootWorkflowId).thenReturn(Optional.empty())
    `when`(javaInfo.rootRunId).thenReturn(Optional.empty())
    `when`(javaInfo.currentBuildId).thenReturn(Optional.empty())

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertNull(kInfo.parentWorkflowId)
    assertNull(kInfo.parentRunId)
    assertNull(kInfo.continuedExecutionRunId)
    assertNull(kInfo.rootWorkflowId)
    assertNull(kInfo.rootRunId)
    assertNull(kInfo.currentBuildId)
  }

  @Test
  fun `converts Java Duration to Kotlin Duration`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    `when`(javaInfo.workflowRunTimeout).thenReturn(Duration.ofMinutes(5))
    `when`(javaInfo.workflowExecutionTimeout).thenReturn(Duration.ofSeconds(30))

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertEquals(5.minutes, kInfo.workflowRunTimeout)
    assertEquals(30.seconds, kInfo.workflowExecutionTimeout)
  }

  @Test
  fun `exposes history metrics`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    `when`(javaInfo.historyLength).thenReturn(100L)
    `when`(javaInfo.historySize).thenReturn(5000L)
    `when`(javaInfo.isContinueAsNewSuggested).thenReturn(true)

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertEquals(100L, kInfo.historyLength)
    assertEquals(5000L, kInfo.historySize)
    assertEquals(true, kInfo.isContinueAsNewSuggested)
  }

  @Test
  fun `exposes priority`() {
    val javaInfo = mock(WorkflowInfo::class.java)
    val priority = Priority.newBuilder().setPriorityKey(5).build()
    `when`(javaInfo.priority).thenReturn(priority)

    val kInfo = KWorkflowInfoImpl(javaInfo)

    assertEquals(5, kInfo.priority.priorityKey)
  }
}
