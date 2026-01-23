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
 * Integration test for [KDynamicWorkflow] verifying signals sent AFTER dynamic handler
 * registration are handled directly.
 */
class KSignalAfterRegistrationWorkflowTest {

    /**
     * Dynamic workflow that verifies signals sent AFTER dynamic handler registration
     * are handled directly (not going through buffer).
     */
    class SignalAfterRegistrationWorkflow : KDynamicWorkflow {
        private val receivedSignals = mutableListOf<String>()

        override suspend fun execute(args: KEncodedValues): Any? {
            // Register dynamic handler immediately
            KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
                val value = signalArgs.get<String>(0)
                receivedSignals.add("$signalName:$value")
            }

            // Wait for signal
            KWorkflow.awaitCondition { receivedSignals.isNotEmpty() }

            return "received=${receivedSignals.first()}"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val testExtension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(SignalAfterRegistrationWorkflow::class)
        }
    }

    @Test
    fun testSignalAfterHandlerRegistration(
        client: KClient,
        options: KWorkflowOptions
    ) = runTest {
        // Start workflow normally (no signal with start)
        val handle = client.startWorkflow(
            "SignalAfterRegWorkflow",
            options.copy(workflowId = "test-signal-after-reg")
        )

        // Send signal after workflow has started
        handle.signal("testSignal", "test-value")

        val result = handle.getResult<String>()

        // Verify signal was received
        assertEquals("received=testSignal:test-value", result)
    }
}
