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
import io.temporal.kotlin.workflow.KDynamicWorkflow
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

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
  private var dynamicWorkflowClass: KClass<out KDynamicWorkflow>? = null

  /**
   * Registers a workflow implementation type with this factory.
   *
   * This method handles both regular workflows and dynamic workflows:
   * - If the class implements [KDynamicWorkflow], it's registered as the dynamic workflow
   *   (handles any workflow type without a specific registration)
   * - Otherwise, the class must implement an interface annotated with @WorkflowInterface
   *   and have a workflow method that is a Kotlin suspend function
   *
   * @param implementationClass the workflow implementation class
   * @throws IllegalArgumentException if the class is not a valid suspend workflow
   * @throws IllegalStateException if a dynamic workflow is already registered
   */
  fun registerWorkflowImplementationType(implementationClass: Class<*>) {
    // Check if this is a dynamic workflow
    if (KDynamicWorkflow::class.java.isAssignableFrom(implementationClass)) {
      @Suppress("UNCHECKED_CAST")
      val dynamicClass = implementationClass.kotlin as KClass<out KDynamicWorkflow>
      if (this.dynamicWorkflowClass != null) {
        throw IllegalStateException(
          "Dynamic workflow is already registered: ${this.dynamicWorkflowClass!!.qualifiedName}"
        )
      }
      this.dynamicWorkflowClass = dynamicClass
      return
    }

    // Regular workflow registration
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
   * This method handles both regular workflows and dynamic workflows.
   * See [registerWorkflowImplementationType] for details.
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
    // First try to find a registered workflow definition
    val definition = workflowDefinitions[workflowType.name]
    if (definition != null) {
      return KotlinReplayWorkflow(
        workflowDefinition = definition,
        dataConverter = dataConverter,
        deadlockDetectionTimeoutMs = deadlockDetectionTimeoutMs,
        workerInterceptors = workerInterceptors
      )
    }

    // Fall back to dynamic workflow if registered
    val dynamicClass = dynamicWorkflowClass
    if (dynamicClass != null) {
      return KotlinDynamicReplayWorkflow(
        dynamicWorkflowClass = dynamicClass,
        dataConverter = dataConverter,
        deadlockDetectionTimeoutMs = deadlockDetectionTimeoutMs,
        workerInterceptors = workerInterceptors
      )
    }

    return null
  }

  override fun getRegisteredWorkflowTypes(): Set<String> {
    return workflowDefinitions.keys.toSet()
  }

  override fun isAnyTypeSupported(): Boolean {
    return workflowDefinitions.isNotEmpty() || dynamicWorkflowClass != null
  }
}
