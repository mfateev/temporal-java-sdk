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

import io.temporal.kotlin.client.KWorkflowClient
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

/**
 * Kotlin worker factory that automatically enables coroutine support.
 *
 * Wraps [WorkerFactory] with [KotlinPlugin] pre-configured, providing
 * seamless support for Kotlin coroutine-based workflows.
 *
 * Example:
 * ```kotlin
 * val service = WorkflowServiceStubs.newLocalServiceStubs()
 * val client = KWorkflowClient(service) { setNamespace("default") }
 *
 * // KWorkerFactory automatically enables Kotlin coroutine support
 * val factory = KWorkerFactory(client) {
 *     maxWorkflowThreadCount = 800
 * }
 *
 * val worker = factory.newWorker("task-queue") {
 *     maxConcurrentActivityExecutionSize = 100
 * }
 *
 * // Register Kotlin coroutine workflows
 * worker.registerWorkflowImplementationTypes(
 *     GreetingWorkflowImpl::class,
 *     OrderWorkflowImpl::class
 * )
 *
 * // Register activities - suspend functions handled automatically
 * worker.registerActivitiesImplementations(
 *     GreetingActivitiesImpl(),
 *     JavaActivitiesImpl()
 * )
 *
 * // Start the worker
 * factory.start()
 * ```
 *
 * @param client The KWorkflowClient to use for workflow interactions
 * @param options DSL builder for WorkerFactoryOptions
 */
public class KWorkerFactory(
  client: KWorkflowClient,
  options: WorkerFactoryOptions.Builder.() -> Unit = {}
) {

  /**
   * The underlying WorkerFactory for advanced use cases.
   */
  public val workerFactory: WorkerFactory

  init {
    val factoryOptions = WorkerFactoryOptions.newBuilder()
      .apply(options)
      // Ensure KotlinPlugin is added
      .also { builder ->
        // Add KotlinPlugin to support Kotlin coroutine workflows
        val existingOptions = builder.build()
        val plugins = existingOptions.workerInterceptors.toMutableList()
        // Note: KotlinPlugin is added via WorkflowImplementationFactory, not interceptors
      }
      .build()

    // Create WorkerFactory with KotlinPlugin added
    val factoryOptionsWithPlugin = WorkerFactoryOptions.newBuilder(factoryOptions)
      .addPlugin(KotlinPlugin())
      .build()

    workerFactory = WorkerFactory.newInstance(client.workflowClient, factoryOptionsWithPlugin)
  }

  /**
   * Creates a new Kotlin worker listening on the specified task queue.
   *
   * @param taskQueue The task queue name
   * @param options DSL builder for WorkerOptions
   * @return A new KWorker instance
   */
  public fun newWorker(taskQueue: String, options: WorkerOptions.Builder.() -> Unit = {}): KWorker {
    val workerOptions = WorkerOptions.newBuilder().apply(options).build()
    return KWorker(workerFactory.newWorker(taskQueue, workerOptions))
  }

  /**
   * Starts all workers created by this factory.
   *
   * This method blocks until the worker is fully started.
   */
  public fun start() {
    workerFactory.start()
  }

  /**
   * Initiates an orderly shutdown.
   *
   * Workers will stop accepting new tasks but will finish processing
   * any tasks that have already started.
   */
  public fun shutdown() {
    workerFactory.shutdown()
  }

  /**
   * Initiates an immediate shutdown.
   *
   * Workers will attempt to stop all processing immediately.
   */
  public fun shutdownNow() {
    workerFactory.shutdownNow()
  }

  /**
   * Waits for all workers to terminate.
   *
   * @param timeout Maximum time to wait for termination
   */
  public suspend fun awaitTermination(timeout: Duration) {
    withContext(Dispatchers.IO) {
      workerFactory.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Checks if all workers have terminated.
   *
   * @return true if all workers have terminated
   */
  public fun isTerminated(): Boolean {
    return workerFactory.isTerminated
  }

  /**
   * Checks if shutdown has been initiated.
   *
   * @return true if shutdown has been initiated
   */
  public fun isShutdown(): Boolean {
    return workerFactory.isShutdown
  }

  /**
   * Checks if all workers are started.
   *
   * @return true if all workers are started
   */
  public fun isStarted(): Boolean {
    return workerFactory.isStarted
  }
}

/**
 * Extension function to register workflow implementation types using KClass.
 *
 * Example:
 * ```kotlin
 * worker.registerWorkflowImplementationTypes(
 *     GreetingWorkflowImpl::class,
 *     OrderWorkflowImpl::class
 * )
 * ```
 */
public fun Worker.registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>) {
  registerWorkflowImplementationTypes(*workflowClasses.map { it.java }.toTypedArray())
}

/**
 * Extension function to register workflow implementation types with options using KClass.
 *
 * Example:
 * ```kotlin
 * worker.registerWorkflowImplementationTypes(
 *     options,
 *     GreetingWorkflowImpl::class,
 *     OrderWorkflowImpl::class
 * )
 * ```
 */
public fun Worker.registerWorkflowImplementationTypes(
  options: io.temporal.worker.WorkflowImplementationOptions,
  vararg workflowClasses: KClass<*>
) {
  registerWorkflowImplementationTypes(options, *workflowClasses.map { it.java }.toTypedArray())
}
