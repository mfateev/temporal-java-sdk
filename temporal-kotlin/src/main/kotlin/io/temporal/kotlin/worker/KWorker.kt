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

import io.temporal.kotlin.activity.registerSuspendActivities
import io.temporal.worker.Worker
import io.temporal.worker.WorkflowImplementationOptions
import kotlin.reflect.KClass

/**
 * Kotlin worker that provides idiomatic APIs for registering
 * Kotlin workflows and suspend activities.
 *
 * Use [worker] property for direct access to the underlying Java Worker
 * when interoperating with Java workflows/activities.
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
 * // Register activities
 * kWorker.registerActivitiesImplementations(MyActivitiesImpl())
 *
 * // Register suspend activities
 * kWorker.registerSuspendActivities(MySuspendActivitiesImpl())
 *
 * // Register Nexus services
 * kWorker.registerNexusServiceImplementations(MyNexusServiceImpl())
 * ```
 */
public class KWorker(
  /** The underlying Java Worker for interop scenarios */
  public val worker: Worker
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
   * Works with both regular and suspend activity implementations.
   *
   * Example:
   * ```kotlin
   * kWorker.registerActivitiesImplementations(
   *     MyActivitiesImpl(),
   *     AnotherActivitiesImpl()
   * )
   * ```
   *
   * @param activities Activity implementation instances to register
   */
  public fun registerActivitiesImplementations(vararg activities: Any) {
    worker.registerActivitiesImplementations(*activities)
  }

  /**
   * Register suspend activity implementations.
   * Wraps suspend functions for execution in the Temporal activity context.
   *
   * This method uses [io.temporal.kotlin.activity.registerSuspendActivities] extension
   * which automatically detects suspend functions and wraps them appropriately.
   *
   * Example:
   * ```kotlin
   * @ActivityInterface
   * interface MySuspendActivities {
   *     suspend fun fetchData(url: String): Data
   * }
   *
   * class MySuspendActivitiesImpl : MySuspendActivities {
   *     override suspend fun fetchData(url: String): Data {
   *         // Suspend function implementation
   *     }
   * }
   *
   * kWorker.registerSuspendActivities(MySuspendActivitiesImpl())
   * ```
   *
   * @param activities Activity implementation objects containing suspend functions
   */
  public fun registerSuspendActivities(vararg activities: Any) {
    worker.registerSuspendActivities(*activities)
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
