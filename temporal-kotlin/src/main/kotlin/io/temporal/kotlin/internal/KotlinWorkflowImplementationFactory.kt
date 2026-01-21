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

package io.temporal.kotlin.internal

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.common.v1.WorkflowType
import io.temporal.common.converter.DataConverter
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.internal.worker.WorkflowImplementationFactory
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory for creating Kotlin coroutine-based workflow implementations.
 *
 * This factory integrates with the Temporal worker infrastructure to provide
 * suspend function support for workflow implementations. It detects whether
 * a workflow class uses Kotlin suspend functions and creates the appropriate
 * coroutine-based execution runtime.
 *
 * Usage:
 * ```kotlin
 * val factory = KotlinWorkflowImplementationFactory(dataConverter)
 * factory.registerWorkflowImplementationType(MyWorkflowImpl::class.java)
 * worker.registerWorkflowImplementationFactory(factory)
 * ```
 */
@InternalTemporalApi
class KotlinWorkflowImplementationFactory(
  private val dataConverter: DataConverter,
  private val deadlockDetectionTimeoutMs: Long = DEFAULT_DEADLOCK_DETECTION_TIMEOUT_MS,
  private val workerInterceptors: List<KWorkerInterceptor> = emptyList()
) : WorkflowImplementationFactory {

  companion object {
    const val DEFAULT_DEADLOCK_DETECTION_TIMEOUT_MS = 1000L

    /**
     * Checks if a class is a suspend-based Kotlin workflow.
     *
     * A class is considered a suspend-based workflow if:
     * 1. It implements an interface annotated with @WorkflowInterface
     * 2. The workflow method is a Kotlin suspend function
     *
     * @param implementationClass the workflow implementation class to check
     * @return true if the class is a suspend-based workflow
     */
    fun isSuspendWorkflow(implementationClass: Class<*>): Boolean {
      return KotlinWorkflowDefinition.isSuspendWorkflow(implementationClass)
    }
  }

  private val workflowDefinitions = ConcurrentHashMap<String, KotlinWorkflowDefinition>()

  /**
   * Registers a workflow implementation type with this factory.
   *
   * The implementation class must:
   * - Implement an interface annotated with @WorkflowInterface
   * - Have a workflow method that is a Kotlin suspend function
   *
   * @param implementationClass the workflow implementation class
   * @throws IllegalArgumentException if the class is not a valid suspend workflow
   */
  fun registerWorkflowImplementationType(implementationClass: Class<*>) {
    if (!KotlinWorkflowDefinition.isSuspendWorkflow(implementationClass)) {
      throw IllegalArgumentException(
        "Class ${implementationClass.name} is not a suspend-based Kotlin workflow. " +
          "The workflow method must be a suspend function."
      )
    }

    val definition = KotlinWorkflowDefinition.fromImplementationClass(implementationClass.kotlin)
    val existing = workflowDefinitions.putIfAbsent(definition.workflowTypeName, definition)
    if (existing != null) {
      throw IllegalStateException(
        "Workflow type '${definition.workflowTypeName}' is already registered " +
          "with implementation ${existing.workflowImplementationClass.qualifiedName}"
      )
    }
  }

  /**
   * Registers multiple workflow implementation types.
   *
   * @param implementationClasses the workflow implementation classes
   */
  fun registerWorkflowImplementationTypes(vararg implementationClasses: Class<*>) {
    implementationClasses.forEach { registerWorkflowImplementationType(it) }
  }

  override fun getWorkflow(
    workflowType: WorkflowType,
    workflowExecution: WorkflowExecution
  ): ReplayWorkflow? {
    val definition = workflowDefinitions[workflowType.name] ?: return null

    return KotlinReplayWorkflow(
      workflowDefinition = definition,
      dataConverter = dataConverter,
      deadlockDetectionTimeoutMs = deadlockDetectionTimeoutMs,
      workerInterceptors = workerInterceptors
    )
  }

  override fun getRegisteredWorkflowTypes(): Set<String> {
    return workflowDefinitions.keys.toSet()
  }

  override fun isAnyTypeSupported(): Boolean {
    return workflowDefinitions.isNotEmpty()
  }
}
