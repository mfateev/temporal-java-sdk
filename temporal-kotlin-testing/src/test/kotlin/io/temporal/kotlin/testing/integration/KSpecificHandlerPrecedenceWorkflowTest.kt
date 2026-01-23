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
 * Integration test for [KDynamicWorkflow] with specific handler taking precedence over buffering.
 */
class KSpecificHandlerPrecedenceWorkflowTest {

    /**
     * Dynamic workflow that tests specific handler taking precedence over buffering.
     * A specific handler registered BEFORE a signal arrives should handle the signal
     * immediately, not buffer it.
     */
    class SpecificHandlerPrecedenceWorkflow : KDynamicWorkflow {
        private var specificSignalValue: String = ""
        private val dynamicSignals = mutableListOf<String>()

        override suspend fun execute(args: KEncodedValues): Any? {
            // Register a SPECIFIC signal handler BEFORE any signals arrive
            // This handler should take precedence - signals to "specificSignal" should NOT be buffered
            KWorkflow.registerSignalHandler("specificSignal") { signalArgs ->
                specificSignalValue = signalArgs.get<String>(0)
            }

            // Now register dynamic handler for any other signals
            KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
                val value = signalArgs.get<String>(0)
                dynamicSignals.add("$signalName:$value")
            }

            // Wait for specific signal
            KWorkflow.awaitCondition { specificSignalValue.isNotEmpty() }

            return "specific=$specificSignalValue"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val testExtension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(SpecificHandlerPrecedenceWorkflow::class)
        }
    }

    @Test
    fun testSpecificHandlerTakesPrecedence(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        // Start workflow normally
        val handle = client.startWorkflow(
            "SpecificHandlerWorkflow",
            options.copy(workflowId = "test-specific-handler")
        )

        // Send signal that has a specific handler
        handle.signal("specificSignal", "specific-value")

        val result = handle.getResult<String>()

        // Verify specific signal was handled by specific handler
        assertEquals("specific=specific-value", result)
    }
}
