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

package io.temporal.kotlin.client

import io.temporal.client.ActivityCompletionClient
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import java.util.Optional

/**
 * Unit tests for [KActivityCompletionClient] and [KActivityCompletionHandle].
 */
class KActivityCompletionClientTest {

  private lateinit var mockJavaClient: ActivityCompletionClient
  private lateinit var client: KActivityCompletionClient

  @Before
  fun setup() {
    mockJavaClient = mock(ActivityCompletionClient::class.java)
    client = KActivityCompletionClient(mockJavaClient)
  }

  @Test
  fun `forTaskToken returns ByTaskToken handle`() {
    val taskToken = "test-token".toByteArray()
    val handle = client.forTaskToken(taskToken)

    assertNotNull(handle)
    assertTrue(handle is KActivityCompletionHandle.ByTaskToken)
  }

  @Test
  fun `forActivity returns ByActivityId handle`() {
    val handle = client.forActivity(
      workflowId = "workflow-123",
      activityId = "activity-456"
    )

    assertNotNull(handle)
    assertTrue(handle is KActivityCompletionHandle.ByActivityId)
  }

  @Test
  fun `forActivity with runId returns ByActivityId handle`() {
    val handle = client.forActivity(
      workflowId = "workflow-123",
      activityId = "activity-456",
      runId = "run-789"
    )

    assertNotNull(handle)
    assertTrue(handle is KActivityCompletionHandle.ByActivityId)
  }

  @Test
  fun `complete by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val result = "test-result"

    client.complete(taskToken, result)

    verify(mockJavaClient).complete(taskToken, result)
  }

  @Test
  fun `completeExceptionally by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val exception = RuntimeException("test error")

    client.completeExceptionally(taskToken, exception)

    verify(mockJavaClient).completeExceptionally(taskToken, exception)
  }

  @Test
  fun `heartbeat by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val details = "progress-50%"

    client.heartbeat(taskToken, details)

    verify(mockJavaClient).heartbeat(taskToken, details)
  }

  @Test
  fun `reportCancellation by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val details = "cancelled by user"

    client.reportCancellation(taskToken, details)

    verify(mockJavaClient).reportCancellation(taskToken, details)
  }

  @Test
  fun `handle complete by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val result = "test-result"

    val handle = client.forTaskToken(taskToken)
    handle.complete(result)

    verify(mockJavaClient).complete(taskToken, result)
  }

  @Test
  fun `handle completeExceptionally by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val exception = RuntimeException("test error")

    val handle = client.forTaskToken(taskToken)
    handle.completeExceptionally(exception)

    verify(mockJavaClient).completeExceptionally(taskToken, exception)
  }

  @Test
  fun `handle heartbeat by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val details = mapOf("progress" to 50)

    val handle = client.forTaskToken(taskToken)
    handle.heartbeat(details)

    verify(mockJavaClient).heartbeat(taskToken, details)
  }

  @Test
  fun `handle reportCancellation by task token delegates to Java client`() = runBlocking {
    val taskToken = "test-token".toByteArray()
    val details = "cleanup complete"

    val handle = client.forTaskToken(taskToken)
    handle.reportCancellation(details)

    verify(mockJavaClient).reportCancellation(taskToken, details)
  }

  @Test
  fun `handle complete by activity id delegates to Java client`() = runBlocking {
    val workflowId = "workflow-123"
    val activityId = "activity-456"
    val result = "test-result"

    val handle = client.forActivity(workflowId, activityId)
    handle.complete(result)

    verify(mockJavaClient).complete(
      workflowId,
      Optional.empty(),
      activityId,
      result
    )
  }

  @Test
  fun `handle complete by activity id with runId delegates to Java client`() = runBlocking {
    val workflowId = "workflow-123"
    val runId = "run-789"
    val activityId = "activity-456"
    val result = "test-result"

    val handle = client.forActivity(workflowId, activityId, runId)
    handle.complete(result)

    verify(mockJavaClient).complete(
      workflowId,
      Optional.of(runId),
      activityId,
      result
    )
  }

  @Test
  fun `handle completeExceptionally by activity id delegates to Java client`() = runBlocking {
    val workflowId = "workflow-123"
    val activityId = "activity-456"
    val exception = RuntimeException("test error")

    val handle = client.forActivity(workflowId, activityId)
    handle.completeExceptionally(exception)

    verify(mockJavaClient).completeExceptionally(
      workflowId,
      Optional.empty(),
      activityId,
      exception
    )
  }

  @Test
  fun `handle heartbeat by activity id delegates to Java client`() = runBlocking {
    val workflowId = "workflow-123"
    val activityId = "activity-456"
    val details = "heartbeat-details"

    val handle = client.forActivity(workflowId, activityId)
    handle.heartbeat(details)

    verify(mockJavaClient).heartbeat(
      workflowId,
      Optional.empty(),
      activityId,
      details
    )
  }

  @Test
  fun `handle reportCancellation by activity id delegates to Java client`() = runBlocking {
    val workflowId = "workflow-123"
    val runId = "run-789"
    val activityId = "activity-456"
    val details = "cancellation-details"

    val handle = client.forActivity(workflowId, activityId, runId)
    handle.reportCancellation(details)

    verify(mockJavaClient).reportCancellation(
      workflowId,
      Optional.of(runId),
      activityId,
      details
    )
  }
}
