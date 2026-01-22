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

package io.temporal.kotlin.testing.integration

import io.temporal.activity.Activity
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KDynamicActivity
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
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test for [KDynamicWorkflow] with signals and activities.
 */
class KSignalDynamicWorkflowTest {

    /**
     * Dynamic workflow that uses signals and activities.
     */
    class SignalDynamicWorkflow : KDynamicWorkflow {
        private var signalValue: String = ""

        override suspend fun execute(args: KEncodedValues): Any? {
            val greeting = args.get<String>(0)
            val workflowType = KWorkflow.info.workflowType

            // Register dynamic signal handler
            KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
                if (signalName == "testSignal") {
                    signalValue = signalArgs.get<String>(0)
                }
            }

            // Wait for signal using coroutine
            KWorkflow.awaitCondition { signalValue.isNotEmpty() }

            // Execute activity using KWorkflow API
            val activityResult = KWorkflow.executeActivity<String>(
                "testActivity",
                listOf(greeting, signalValue),
                KActivityOptions(startToCloseTimeout = 10.seconds),
            )

            return "workflowType=$workflowType, activity=$activityResult"
        }
    }

    /**
     * Dynamic activity for testing.
     */
    class TestDynamicActivity : KDynamicActivity {
        override fun execute(args: KEncodedValues): Any? {
            val activityType = Activity.getExecutionContext().info.activityType
            val greeting = args.get<String>(0)
            val name = args.get<String>(1)
            return "$activityType: $greeting $name"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val testExtension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(SignalDynamicWorkflow::class)
            activityImplementations = listOf(TestDynamicActivity())
        }
    }

    @Test
    fun testDynamicWorkflowWithSignalAndActivity(
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        val handle = client.signalWithStart(
            workflowType = "MyDynamicType",
            signalName = "testSignal",
            signalArgs = arrayOf("World"),
            workflowArgs = arrayOf("Hello"),
            options = options.copy(workflowId = "test-signal-dynamic"),
        )
        val result = handle.getResult<String>()

        assertEquals("workflowType=MyDynamicType, activity=testActivity: Hello World", result)
    }
}
