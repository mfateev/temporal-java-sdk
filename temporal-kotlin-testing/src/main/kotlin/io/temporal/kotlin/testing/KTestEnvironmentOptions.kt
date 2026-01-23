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

package io.temporal.kotlin.testing

import com.uber.m3.tally.Scope
import io.temporal.api.enums.v1.IndexedValueType
import io.temporal.client.WorkflowClientOptions
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.worker.WorkerFactoryOptions
import java.time.Instant

/**
 * Kotlin-native test environment options.
 *
 * This data class provides a Kotlin-idiomatic way to configure the test environment.
 *
 * Example:
 * ```kotlin
 * val testEnv = KTestWorkflowEnvironment.newInstance(
 *     KTestEnvironmentOptions(
 *         namespace = "test-namespace",
 *         initialTime = Instant.parse("2024-01-01T00:00:00Z"),
 *         useTimeskipping = true,
 *         searchAttributes = mapOf(
 *             "CustomKeyword" to IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
 *             "CustomInt" to IndexedValueType.INDEXED_VALUE_TYPE_INT
 *         )
 *     )
 * )
 * ```
 *
 * @property namespace Namespace to use for testing. Default: null (uses Java SDK default "default").
 *           When using an in-memory test server, the namespace is automatically created.
 * @property initialTime Initial time for the workflow virtual clock. Default: current time.
 * @property useTimeskipping Whether to enable time skipping. Default: true.
 * @property useExternalService Whether to use external Temporal service. Default: false (in-memory).
 * @property target Target endpoint for external service.
 * @property metricsScope Metrics scope for reporting.
 * @property workerFactoryOptions Optional WorkerFactoryOptions for advanced configuration.
 *           KotlinPlugin is automatically added to support suspend workflows.
 * @property workflowClientOptions Optional WorkflowClientOptions for advanced configuration.
 * @property workflowServiceStubsOptions Optional WorkflowServiceStubsOptions for advanced configuration.
 * @property searchAttributes Search attributes to register with the test server.
 */
public data class KTestEnvironmentOptions(
    val namespace: String? = null,
    val initialTime: Instant? = null,
    val useTimeskipping: Boolean = true,
    val useExternalService: Boolean = false,
    val target: String? = null,
    val metricsScope: Scope? = null,
    val workerFactoryOptions: WorkerFactoryOptions? = null,
    val workflowClientOptions: WorkflowClientOptions? = null,
    val workflowServiceStubsOptions: WorkflowServiceStubsOptions? = null,
    val searchAttributes: Map<String, IndexedValueType> = emptyMap(),
)
