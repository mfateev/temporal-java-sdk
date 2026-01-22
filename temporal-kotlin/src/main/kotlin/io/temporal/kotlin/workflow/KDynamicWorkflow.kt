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

import io.temporal.kotlin.common.KEncodedValues

/**
 * Interface for implementing dynamic workflows that can handle any workflow type.
 *
 * Dynamic workflows receive all arguments as [KEncodedValues] and can return any result.
 * The workflow type is available via [KWorkflow.info].workflowType.
 *
 * Dynamic workflows support Kotlin coroutines and can use all [KWorkflow] APIs:
 * - [KWorkflow.awaitCondition] for waiting on conditions
 * - [KWorkflow.executeActivity] for activity execution
 * - [KWorkflow.registerDynamicSignalHandler] for dynamic signals
 *
 * Dynamic workflows are useful when:
 * - Workflow types are determined at runtime
 * - You need a single implementation to handle multiple workflow types
 * - Building generic workflow routing/dispatching systems
 *
 * Example:
 * ```kotlin
 * class RouterWorkflow : KDynamicWorkflow {
 *     private var name: String = ""
 *
 *     override suspend fun execute(args: KEncodedValues): Any? {
 *         val workflowType = KWorkflow.info.workflowType
 *         val greeting = args.get<String>(0)
 *
 *         // Register dynamic signal handler
 *         KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
 *             if (signalName == "greetingSignal") {
 *                 name = signalArgs.get<String>(0)
 *             }
 *         }
 *
 *         // Wait for signal using coroutine
 *         KWorkflow.awaitCondition { name.isNotEmpty() }
 *
 *         // Execute activity using coroutine
 *         val result = KWorkflow.executeActivity<String>(
 *             "MyActivity",
 *             KActivityOptions(startToCloseTimeout = 10.seconds),
 *             greeting, name
 *         )
 *         return result
 *     }
 * }
 * ```
 *
 * To register a dynamic workflow:
 * ```kotlin
 * val worker = KWorker(
 *     client,
 *     KWorkerOptions(
 *         taskQueue = "my-task-queue",
 *         dynamicWorkflow = RouterWorkflow::class
 *     )
 * )
 * ```
 */
public interface KDynamicWorkflow {
  /**
   * Execute the workflow with the provided arguments.
   *
   * This is a suspend function that supports Kotlin coroutines.
   * Use [KWorkflow] APIs for workflow operations like timers,
   * activities, child workflows, and signals.
   *
   * @param args The workflow arguments as [KEncodedValues]
   * @return The workflow result (must be serializable)
   */
  suspend fun execute(args: KEncodedValues): Any?
}
