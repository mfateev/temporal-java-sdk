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
 * Integration test for simple [KDynamicWorkflow].
 */
class KSimpleDynamicWorkflowTest {

    /**
     * Simple dynamic workflow that returns a formatted string.
     */
    class SimpleDynamicWorkflow : KDynamicWorkflow {
        override suspend fun execute(args: KEncodedValues): Any? {
            val input = args.get<String>(0)
            val workflowType = KWorkflow.info.workflowType
            return "$workflowType received: $input"
        }
    }

    companion object {
        @JvmField
        @RegisterExtension
        val testExtension = kTestWorkflowExtension {
            workflowImplementationTypes = listOf(SimpleDynamicWorkflow::class)
        }
    }

    @Test
    fun testSimpleDynamicWorkflow(
        client: KClient,
        options: KWorkflowOptions,
    ) = runTest {
        val result = client.executeWorkflow<String>(
            "AnyWorkflowType",
            options.copy(workflowId = "test-simple-dynamic"),
            "test input",
        )

        assertEquals("AnyWorkflowType received: test input", result)
    }
}
