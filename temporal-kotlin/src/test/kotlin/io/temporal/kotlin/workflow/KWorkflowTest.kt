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

package io.temporal.kotlin.workflow

import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.kotlin.internal.workflow.KUpdateInfo
import io.temporal.kotlin.internal.workflow.KotlinWorkflowContext
import io.temporal.workflow.Workflow
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Test
import org.mockito.Mockito.mock

/**
 * Unit tests for [KWorkflow].
 *
 * Note: Most KWorkflow methods delegate to the Java SDK's Workflow class,
 * which requires a workflow execution context. These tests verify the API
 * structure and basic functionality that can be tested without a full context.
 *
 * Full integration tests should be added to test KWorkflow within actual
 * workflow executions.
 */
class KWorkflowTest {

  @After
  fun cleanup() {
    // Reset the context after each test
    KWorkflow.currentContext.remove()
  }

  @Test
  fun `DEFAULT_VERSION matches Java SDK constant`() {
    assertEquals(Workflow.DEFAULT_VERSION, KWorkflow.DEFAULT_VERSION)
  }

  @Test
  fun `isCancelRequested returns false when no context is set`() {
    // When called outside of workflow context, should return false gracefully
    assertFalse(KWorkflow.isCancelRequested())
  }

  @Test
  fun `currentContext ThreadLocal is initially null`() {
    // Verify the thread local starts as null
    assertEquals(null, KWorkflow.currentContext.get())
  }

  // ==================== getCurrentUpdateInfo Tests ====================

  @Test
  fun `currentUpdateInfo throws when no context is set`() {
    // Ensure no context is set
    KWorkflow.currentContext.remove()

    val exception = assertThrows(IllegalStateException::class.java) {
      KWorkflow.currentUpdateInfo
    }
    assertEquals(
      "KWorkflow.currentUpdateInfo must be accessed from within workflow code",
      exception.message
    )
  }

  @Test
  fun `currentUpdateInfo returns null when not in update handler`() {
    // Set up a context with mocked ReplayWorkflowContext
    val mockReplayContext = mock(ReplayWorkflowContext::class.java)
    val context = KotlinWorkflowContext(mockReplayContext)
    KWorkflow.currentContext.set(context)

    // When not in an update handler, should return null
    assertNull(KWorkflow.currentUpdateInfo)
  }

  @Test
  fun `currentUpdateInfo returns update info when in update handler`() {
    // Set up a context with mocked ReplayWorkflowContext
    val mockReplayContext = mock(ReplayWorkflowContext::class.java)
    val context = KotlinWorkflowContext(mockReplayContext)
    KWorkflow.currentContext.set(context)

    // Simulate being in an update handler by setting the update info
    val updateInfo = KUpdateInfo("myUpdate", "update-id-123")
    context.currentUpdateInfo.set(updateInfo)

    // Should return the update info
    val result = KWorkflow.currentUpdateInfo
    assertEquals("myUpdate", result?.updateName)
    assertEquals("update-id-123", result?.updateId)
  }

  // Note: The following methods require a workflow execution context and
  // should be tested via integration tests:
  // - getInfo()
  // - currentTime()
  // - currentTimeMillis()
  // - randomUUID()
  // - newRandom()
  // - getVersion()
  // - sideEffect()
}
