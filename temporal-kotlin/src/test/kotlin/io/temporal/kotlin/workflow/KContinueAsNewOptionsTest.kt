@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

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

package io.temporal.kotlin.workflow

import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.internal.KOptionsConverters
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class KContinueAsNewOptionsTest {

  @Test
  fun `default options should produce empty builder result`() {
    val options = KContinueAsNewOptions()
    val javaOptions = KOptionsConverters.toJava(options)

    // All optional fields should remain unset
    assertNull(javaOptions.workflowRunTimeout)
    assertNull(javaOptions.taskQueue)
    assertNull(javaOptions.retryOptions)
    assertNull(javaOptions.workflowTaskTimeout)
    assertNull(javaOptions.memo)
    assertNull(javaOptions.typedSearchAttributes)
    assertNull(javaOptions.contextPropagators)
  }

  @Test
  fun `DEFAULT companion value should have all null fields`() {
    val options = KContinueAsNewOptions.DEFAULT

    assertNull(options.workflowRunTimeout)
    assertNull(options.taskQueue)
    assertNull(options.retryOptions)
    assertNull(options.workflowTaskTimeout)
    assertNull(options.memo)
    assertNull(options.typedSearchAttributes)
    assertNull(options.contextPropagators)
  }

  @Test
  fun `workflowRunTimeout should be converted correctly`() {
    val options = KContinueAsNewOptions(
      workflowRunTimeout = 1.hours
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertNotNull(javaOptions.workflowRunTimeout)
    assertEquals(java.time.Duration.ofHours(1), javaOptions.workflowRunTimeout)
  }

  @Test
  fun `taskQueue should be set correctly`() {
    val options = KContinueAsNewOptions(
      taskQueue = "new-task-queue"
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertEquals("new-task-queue", javaOptions.taskQueue)
  }

  @Test
  fun `retryOptions should be converted correctly`() {
    val retryOptions = KRetryOptions(
      maximumAttempts = 5,
      initialInterval = 1.seconds,
      backoffCoefficient = 2.0
    )
    val options = KContinueAsNewOptions(
      retryOptions = retryOptions
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertNotNull(javaOptions.retryOptions)
    assertEquals(5, javaOptions.retryOptions!!.maximumAttempts)
    assertEquals(java.time.Duration.ofSeconds(1), javaOptions.retryOptions!!.initialInterval)
    assertEquals(2.0, javaOptions.retryOptions!!.backoffCoefficient, 0.001)
  }

  @Test
  fun `workflowTaskTimeout should be converted correctly`() {
    val options = KContinueAsNewOptions(
      workflowTaskTimeout = 30.seconds
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertNotNull(javaOptions.workflowTaskTimeout)
    assertEquals(java.time.Duration.ofSeconds(30), javaOptions.workflowTaskTimeout)
  }

  @Test
  fun `memo should be set correctly`() {
    val memoData = mapOf(
      "key1" to "value1",
      "key2" to 42
    )
    val options = KContinueAsNewOptions(
      memo = memoData
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertNotNull(javaOptions.memo)
    assertEquals(memoData, javaOptions.memo)
  }

  @Test
  fun `all options should be set correctly when provided`() {
    val options = KContinueAsNewOptions(
      workflowRunTimeout = 2.hours,
      taskQueue = "my-queue",
      retryOptions = KRetryOptions(maximumAttempts = 3),
      workflowTaskTimeout = 15.minutes,
      memo = mapOf("key" to "value")
    )
    val javaOptions = KOptionsConverters.toJava(options)

    assertEquals(java.time.Duration.ofHours(2), javaOptions.workflowRunTimeout)
    assertEquals("my-queue", javaOptions.taskQueue)
    assertEquals(3, javaOptions.retryOptions!!.maximumAttempts)
    assertEquals(java.time.Duration.ofMinutes(15), javaOptions.workflowTaskTimeout)
    assertEquals(mapOf("key" to "value"), javaOptions.memo)
  }

  @Test
  fun `data class copy should work correctly`() {
    val original = KContinueAsNewOptions(
      workflowRunTimeout = 1.hours,
      taskQueue = "original-queue"
    )

    val modified = original.copy(
      taskQueue = "modified-queue",
      workflowTaskTimeout = 30.seconds
    )

    // Original unchanged
    assertEquals(1.hours, original.workflowRunTimeout)
    assertEquals("original-queue", original.taskQueue)
    assertNull(original.workflowTaskTimeout)

    // Modified has updates
    assertEquals(1.hours, modified.workflowRunTimeout) // inherited
    assertEquals("modified-queue", modified.taskQueue) // changed
    assertEquals(30.seconds, modified.workflowTaskTimeout) // added
  }
}
