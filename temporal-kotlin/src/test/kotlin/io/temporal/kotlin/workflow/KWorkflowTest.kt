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

import io.temporal.workflow.Workflow
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

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
