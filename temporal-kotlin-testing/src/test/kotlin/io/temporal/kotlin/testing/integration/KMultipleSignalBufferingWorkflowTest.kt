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

package io.temporal.kotlin.testing.integration

import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.common.KEncodedValues
import io.temporal.kotlin.testing.kTestWorkflowExtension
import io.temporal.kotlin.workflow.KDynamicWorkflow
import io.temporal.kotlin.workflow.KWorkflow
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * Integration test for [KDynamicWorkflow] with multiple signal buffering via signalWithStart.
 */
class KMultipleSignalBufferingWorkflowTest {

    /**
     * Dynamic workflow that tests multiple signals being buffered via signalWithStart.
     * The first signal is sent atomically with start, guaranteeing it arrives before
     * the workflow has a chance to register its handler.
     */
    class MultipleSignalBufferingWorkflow : KDynamicWorkflow {
        private val receivedSignals = mutableListOf<String>()

        override suspend fun execute(args: KEncodedValues): Any? {
            val expectedCount = args.get<Int>(0)

            // Register dynamic signal handler - signals received before this should be buffered
            KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
                val value = signalArgs.get<String>(0)
                receivedSignals.add("$signalName:$value")
            }

            // Wait until we have all expected signals
            KWorkflow.awaitCondition { receivedSignals.size >= expectedCount }

            // Return the count of signals received (order is not guaranteed for signals after the first)
            return "count=${receivedSignals.size}, first=${receivedSignals.firstOrNull()}"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val testExtension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(MultipleSignalBufferingWorkflow::class)
        }
    }

    @Test
    fun testSignalBufferedViaSignalWithStart(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        // Start workflow expecting 1 signal, sent atomically via signalWithStart
        // This signal MUST be buffered since it arrives before workflow code runs
        val handle = client.signalWithStart(
            workflowType = "MultiSignalWorkflow",
            signalName = "bufferedSignal",
            signalArgs = arrayOf("buffered-value"),
            workflowArgs = arrayOf(1),
            options = options.copy(workflowId = "test-buffered-signal")
        )
        val result = handle.getResult<String>()

        // Verify the signal was received (it was buffered and replayed)
        assertEquals("count=1, first=bufferedSignal:buffered-value", result)
    }
}
