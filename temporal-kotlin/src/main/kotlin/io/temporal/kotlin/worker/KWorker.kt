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

package io.temporal.kotlin.worker

import io.temporal.activity.ActivityInterface
import io.temporal.common.metadata.POJOActivityInterfaceMetadata
import io.temporal.kotlin.activity.KotlinActivityWrapper
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.worker.Worker
import io.temporal.worker.WorkflowImplementationOptions
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlin.reflect.KClass

/**
 * Kotlin worker that provides idiomatic APIs for registering
 * Kotlin workflows and activities (including suspend activities).
 *
 * Use [worker] property for direct access to the underlying Java Worker
 * when interoperating with Java workflows/activities.
 *
 * ## Activity Registration
 *
 * This worker uses [TypedDynamicActivity][io.temporal.activity.TypedDynamicActivity] wrappers
 * to register Kotlin activities directly with the Java SDK. Each activity method is wrapped
 * in a [KotlinActivityWrapper] that handles both suspend and non-suspend methods.
 *
 * This design ensures:
 * - Proper handling of Kotlin suspend functions
 * - No conflicts with Java DynamicActivity registrations
 * - Compatibility with test mocking frameworks
 * - Full support for activity interceptors
 *
 * Example:
 * ```kotlin
 * val factory = KWorkerFactory(client)
 * val kWorker = factory.newWorker("task-queue")
 *
 * // Register workflow using reified generics
 * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
 *
 * // Register workflow with options
 * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl> {
 *     setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
 * }
 *
 * // Register activities (works with both suspend and non-suspend)
 * kWorker.registerActivitiesImplementations(MyActivitiesImpl())
 *
 * // Register Nexus services
 * kWorker.registerNexusServiceImplementations(MyNexusServiceImpl())
 * ```
 */
public class KWorker(
  /** The underlying Java Worker for interop scenarios */
  public val worker: Worker,
  /** Kotlin worker interceptors for activity interception */
  internal val workerInterceptors: List<KWorkerInterceptor> = emptyList(),
  /** Coroutine dispatcher for suspend activities */
  private val activityDispatcher: CoroutineDispatcher = Dispatchers.Default
) {
  // ========== Workflow Registration ==========

  /**
   * Register Kotlin workflow implementation types using reified generics.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
   * ```
   */
  public inline fun <reified T : Any> registerWorkflowImplementationTypes() {
    worker.registerWorkflowImplementationTypes(T::class.java)
  }

  /**
   * Register Kotlin workflow implementation types using KClass.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes(
   *     MyWorkflowImpl::class,
   *     AnotherWorkflowImpl::class
   * )
   * ```
   *
   * @param workflowClasses Workflow implementation classes to register
   */
  public fun registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>) {
    worker.registerWorkflowImplementationTypes(
      *workflowClasses.map { it.java }.toTypedArray()
    )
  }

  /**
   * Register Kotlin workflow implementation types with options using KClass.
   *
   * Example:
   * ```kotlin
   * val options = WorkflowImplementationOptions.newBuilder()
   *     .setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
   *     .build()
   * kWorker.registerWorkflowImplementationTypes(options, MyWorkflowImpl::class)
   * ```
   *
   * @param options WorkflowImplementationOptions instance
   * @param workflowClasses Workflow implementation classes to register
   */
  public fun registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions,
    vararg workflowClasses: KClass<*>
  ) {
    worker.registerWorkflowImplementationTypes(
      options,
      *workflowClasses.map { it.java }.toTypedArray()
    )
  }

  /**
   * Register Kotlin workflow implementation types with options DSL.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl> {
   *     setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
   * }
   * ```
   *
   * @param options DSL builder for WorkflowImplementationOptions
   */
  public inline fun <reified T : Any> registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions.Builder.() -> Unit
  ) {
    val opts = WorkflowImplementationOptions.newBuilder().apply(options).build()
    worker.registerWorkflowImplementationTypes(opts, T::class.java)
  }

  // ========== Activity Registration ==========

  /**
   * Register activity implementations.
   *
   * This method handles both suspend and non-suspend activity methods by wrapping
   * each method in a [KotlinActivityWrapper] and registering it as a
   * [TypedDynamicActivity][io.temporal.activity.TypedDynamicActivity] with the Java SDK.
   *
   * Example:
   * ```kotlin
   * @ActivityInterface
   * interface MyActivities {
   *     fun syncOperation(): String           // Regular method
   *     suspend fun asyncOperation(): Data    // Suspend method
   * }
   *
   * kWorker.registerActivitiesImplementations(MyActivitiesImpl())
   * ```
   *
   * @param activities Activity implementation instances to register
   */
  public fun registerActivitiesImplementations(vararg activities: Any) {
    for (activity in activities) {
      registerActivityImplementation(activity)
    }
  }

  /**
   * Register a single activity implementation.
   *
   * Extracts activity interfaces, creates a [KotlinActivityWrapper] for each method,
   * and registers them with the Java worker.
   */
  private fun registerActivityImplementation(activity: Any) {
    val implClass = activity::class.java

    // Find all activity interfaces implemented by this class
    val activityInterfaces = findActivityInterfaces(implClass)
    if (activityInterfaces.isEmpty()) {
      throw IllegalArgumentException(
        "Implementation does not implement any @ActivityInterface annotated interfaces: ${implClass.name}"
      )
    }

    // Create wrappers for all activity methods and register them
    val wrappers = mutableListOf<KotlinActivityWrapper>()

    for (activityInterface in activityInterfaces) {
      val metadata = POJOActivityInterfaceMetadata.newInstance(activityInterface)
      for (methodMetadata in metadata.methodsMetadata) {
        val activityTypeName = methodMetadata.activityTypeName
        val interfaceMethod = methodMetadata.method

        // Find the implementation method (may be different for suspend functions)
        val implMethod = findImplementationMethod(implClass, interfaceMethod)

        val wrapper = KotlinActivityWrapper(
          activityTypeName = activityTypeName,
          implementation = activity,
          method = implMethod,
          dispatcher = activityDispatcher
        )
        wrappers.add(wrapper)
      }
    }

    // Register all wrappers with the Java worker
    worker.registerActivitiesImplementations(*wrappers.toTypedArray())
  }

  /**
   * Find the implementation method for an interface method.
   *
   * For suspend functions, the implementation method will have a Continuation parameter.
   */
  private fun findImplementationMethod(
    implClass: Class<*>,
    interfaceMethod: java.lang.reflect.Method
  ): java.lang.reflect.Method {
    val methodName = interfaceMethod.name
    val interfaceParams = interfaceMethod.parameterTypes

    // First try exact match (for non-suspend methods)
    try {
      return implClass.getMethod(methodName, *interfaceParams)
    } catch (_: NoSuchMethodException) {
      // Not found, continue to search for suspend variant
    }

    // For suspend methods, the implementation has an extra Continuation parameter
    // Look for a method with the same name and compatible parameter count
    val continuationClass = kotlin.coroutines.Continuation::class.java
    for (method in implClass.methods) {
      if (method.name == methodName) {
        val params = method.parameterTypes
        // Suspend method: same params + Continuation at the end
        if (params.size == interfaceParams.size + 1 &&
          continuationClass.isAssignableFrom(params.last())
        ) {
          // Verify the other params match
          var matches = true
          for (i in interfaceParams.indices) {
            if (interfaceParams[i] != params[i]) {
              matches = false
              break
            }
          }
          if (matches) {
            return method
          }
        }
      }
    }

    throw IllegalStateException(
      "Could not find implementation method for ${interfaceMethod.name} in ${implClass.name}"
    )
  }

  /**
   * Find all activity interfaces implemented by a class.
   */
  private fun findActivityInterfaces(clazz: Class<*>): List<Class<*>> {
    val result = mutableListOf<Class<*>>()

    fun collectInterfaces(cls: Class<*>) {
      for (iface in cls.interfaces) {
        if (iface.isAnnotationPresent(ActivityInterface::class.java)) {
          result.add(iface)
        }
        collectInterfaces(iface)
      }
      cls.superclass?.let { collectInterfaces(it) }
    }

    collectInterfaces(clazz)
    return result.distinct()
  }

  // ========== Nexus Registration ==========

  /**
   * Register Nexus service implementations.
   *
   * Example:
   * ```kotlin
   * @NexusServiceInterface
   * interface MyNexusService {
   *     @NexusOperationInterface
   *     fun doSomething(input: String): String
   * }
   *
   * class MyNexusServiceImpl : MyNexusService {
   *     override fun doSomething(input: String): String = "result"
   * }
   *
   * kWorker.registerNexusServiceImplementations(MyNexusServiceImpl())
   * ```
   *
   * @param services Nexus service implementation instances to register
   */
  public fun registerNexusServiceImplementations(vararg services: Any) {
    worker.registerNexusServiceImplementation(*services)
  }
}
