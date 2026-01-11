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

import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation

/**
 * Metadata class for Kotlin workflow types.
 *
 * This class extracts workflow type information from Kotlin classes annotated with
 * Temporal workflow annotations, including support for suspend functions.
 */
@InternalTemporalApi
internal class KotlinWorkflowDefinition(
  /**
   * The workflow interface class.
   */
  val workflowInterface: KClass<*>,

  /**
   * The workflow implementation class.
   */
  val workflowImplementationClass: KClass<*>,

  /**
   * The workflow type name as registered with Temporal.
   */
  val workflowTypeName: String,

  /**
   * The workflow method (the main entry point).
   */
  val workflowMethod: KFunction<*>,

  /**
   * True if the workflow method is a suspend function.
   */
  val isSuspendFunction: Boolean,

  /**
   * Signal method handlers mapped by signal name.
   */
  val signalMethods: Map<String, KFunction<*>>,

  /**
   * Query method handlers mapped by query name.
   */
  val queryMethods: Map<String, KFunction<*>>,

  /**
   * Update method handlers mapped by update name.
   */
  val updateMethods: Map<String, KFunction<*>>
) {
  companion object {
    /**
     * Creates a workflow definition from a workflow implementation class.
     *
     * @param implementationClass the workflow implementation class
     * @return the workflow definition
     * @throws IllegalArgumentException if the class is not a valid workflow implementation
     */
    fun fromImplementationClass(implementationClass: KClass<*>): KotlinWorkflowDefinition {
      // Find the workflow interface
      val workflowInterface = findWorkflowInterface(implementationClass)
        ?: throw IllegalArgumentException(
          "Class ${implementationClass.qualifiedName} does not implement a @WorkflowInterface"
        )

      // Find the workflow method
      val workflowMethod = findWorkflowMethod(workflowInterface)
        ?: throw IllegalArgumentException(
          "Interface ${workflowInterface.qualifiedName} does not have a @WorkflowMethod"
        )

      // Determine the workflow type name
      val workflowTypeName = extractWorkflowTypeName(workflowInterface, workflowMethod)

      // Check if it's a suspend function
      val isSuspendFunction = workflowMethod.isSuspend

      // Find signal, query, and update methods
      val signalMethods = findSignalMethods(workflowInterface)
      val queryMethods = findQueryMethods(workflowInterface)
      val updateMethods = findUpdateMethods(workflowInterface)

      return KotlinWorkflowDefinition(
        workflowInterface = workflowInterface,
        workflowImplementationClass = implementationClass,
        workflowTypeName = workflowTypeName,
        workflowMethod = workflowMethod,
        isSuspendFunction = isSuspendFunction,
        signalMethods = signalMethods,
        queryMethods = queryMethods,
        updateMethods = updateMethods
      )
    }

    /**
     * Checks if a class is a suspend-based Kotlin workflow.
     *
     * A class is considered a suspend-based workflow if:
     * 1. It implements an interface annotated with @WorkflowInterface
     * 2. The workflow method is a Kotlin suspend function
     */
    fun isSuspendWorkflow(implementationClass: Class<*>): Boolean {
      val kClass = implementationClass.kotlin
      val workflowInterface = findWorkflowInterface(kClass) ?: return false
      val workflowMethod = findWorkflowMethod(workflowInterface) ?: return false
      return workflowMethod.isSuspend
    }

    private fun findWorkflowInterface(implementationClass: KClass<*>): KClass<*>? {
      // Check all interfaces implemented by the class
      return implementationClass.java.interfaces
        .map { it.kotlin }
        .firstOrNull { iface ->
          iface.hasAnnotation<WorkflowInterface>()
        }
    }

    private fun findWorkflowMethod(workflowInterface: KClass<*>): KFunction<*>? {
      return workflowInterface.declaredFunctions.firstOrNull { func ->
        func.findAnnotation<WorkflowMethod>() != null
      }
    }

    private fun extractWorkflowTypeName(
      workflowInterface: KClass<*>,
      workflowMethod: KFunction<*>
    ): String {
      // Check for explicit name in @WorkflowMethod annotation
      val methodAnnotation = workflowMethod.findAnnotation<WorkflowMethod>()
      if (methodAnnotation != null && methodAnnotation.name.isNotEmpty()) {
        return methodAnnotation.name
      }

      // Default to the interface simple name
      return workflowInterface.simpleName
        ?: throw IllegalArgumentException("Workflow interface must have a name")
    }

    private fun findSignalMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
      return workflowInterface.declaredFunctions
        .filter { it.findAnnotation<io.temporal.workflow.SignalMethod>() != null }
        .associateBy { func ->
          val annotation = func.findAnnotation<io.temporal.workflow.SignalMethod>()!!
          if (annotation.name.isNotEmpty()) annotation.name else func.name
        }
    }

    private fun findQueryMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
      // Find query methods from declared functions
      val functionQueries = workflowInterface.declaredFunctions
        .filter { it.findAnnotation<io.temporal.workflow.QueryMethod>() != null }
        .associateBy { func ->
          val annotation = func.findAnnotation<io.temporal.workflow.QueryMethod>()!!
          if (annotation.name.isNotEmpty()) annotation.name else func.name
        }

      // Find query methods from property getters (supports @get:QueryMethod on val properties)
      val propertyQueries = workflowInterface.declaredMemberProperties
        .mapNotNull { prop ->
          val getter = prop.getter
          val annotation = getter.findAnnotation<io.temporal.workflow.QueryMethod>()
          if (annotation != null) {
            val name = if (annotation.name.isNotEmpty()) annotation.name else prop.name
            name to getter
          } else {
            null
          }
        }
        .toMap()

      return functionQueries + propertyQueries
    }

    private fun findUpdateMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
      return workflowInterface.declaredFunctions
        .filter { it.findAnnotation<io.temporal.workflow.UpdateMethod>() != null }
        .associateBy { func ->
          val annotation = func.findAnnotation<io.temporal.workflow.UpdateMethod>()!!
          if (annotation.name.isNotEmpty()) annotation.name else func.name
        }
    }
  }

  /**
   * Creates a new instance of the workflow implementation.
   */
  fun createInstance(): Any {
    return workflowImplementationClass.java.getDeclaredConstructor().newInstance()
  }
}
