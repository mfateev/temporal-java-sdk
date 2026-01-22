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

package io.temporal.kotlin.worker

import io.temporal.kotlin.client.KClient
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean

class KWorkerTest {

  @WorkflowInterface
  interface SimpleWorkflow {
    @WorkflowMethod
    suspend fun execute(): String
  }

  class SimpleWorkflowImpl : SimpleWorkflow {
    override suspend fun execute(): String = "done"
  }

  private lateinit var testEnv: TestWorkflowEnvironment

  @Before
  fun setUp() {
    testEnv = TestWorkflowEnvironment.newInstance()
  }

  @After
  fun tearDown() {
    testEnv.close()
  }

  @Test
  fun `run blocks until cancelled`() = runBlocking {
    val client = KClient(testEnv.workflowClient)
    val taskQueue = "test-task-queue-${System.currentTimeMillis()}"

    val worker = KWorker(
      client,
      KWorkerOptions(
        taskQueue = taskQueue,
        workflows = listOf(SimpleWorkflowImpl::class)
      )
    )

    val runStarted = AtomicBoolean(false)
    val runCompleted = AtomicBoolean(false)

    val job = async {
      runStarted.set(true)
      try {
        worker.run()
      } catch (e: CancellationException) {
        // Expected
        throw e
      } finally {
        runCompleted.set(true)
      }
    }

    // Wait for run to start
    while (!runStarted.get()) {
      delay(10)
    }

    // Give it a moment to actually start blocking
    delay(100)

    // run() should still be blocking
    assertFalse("run() should not have completed yet", runCompleted.get())

    // Cancel the job
    job.cancel()

    // Wait for completion
    try {
      job.await()
    } catch (e: CancellationException) {
      // Expected
    }

    // Verify run completed after cancellation
    assertTrue("run() should have completed after cancellation", runCompleted.get())
  }

  @Test
  fun `run returns when shutdown is called externally`() = runBlocking {
    val client = KClient(testEnv.workflowClient)
    val taskQueue = "test-task-queue-shutdown-${System.currentTimeMillis()}"

    val worker = KWorker(
      client,
      KWorkerOptions(
        taskQueue = taskQueue,
        workflows = listOf(SimpleWorkflowImpl::class)
      )
    )

    val runCompleted = AtomicBoolean(false)

    val job = async {
      try {
        worker.run()
      } finally {
        runCompleted.set(true)
      }
    }

    // Give worker time to start
    delay(100)

    // Shutdown externally
    worker.shutdown()

    // Wait for completion with timeout
    job.await()

    assertTrue("run() should have completed after shutdown", runCompleted.get())
  }
}
