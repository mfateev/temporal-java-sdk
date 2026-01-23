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

import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KOptionsConverters
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

// TODO: Switch from Dispatchers.IO + blocking Java SDK calls to fully async implementation
//  using gRPC async client. This will eliminate thread pool overhead and provide true
//  non-blocking suspension.

/**
 * Kotlin worker factory that automatically enables coroutine support.
 *
 * Wraps [WorkerFactory] with [KotlinPlugin] pre-configured, providing
 * seamless support for Kotlin coroutine-based workflows.
 *
 * Example:
 * ```kotlin
 * val service = WorkflowServiceStubs.newLocalServiceStubs()
 * val client = KClient(service) { setNamespace("default") }
 *
 * // KWorkerFactory automatically enables Kotlin coroutine support
 * val factory = KWorkerFactory(client) {
 *     workerInterceptors = listOf(LoggingInterceptor())
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
 * @param client The KClient to use for workflow interactions
 * @param options DSL builder for KWorkerFactoryOptionsBuilder
 */
@OptIn(InternalTemporalApi::class)
public class KWorkerFactory(
  client: KClient,
  options: KWorkerFactoryOptionsBuilder.() -> Unit = {}
) {

  /**
   * The underlying WorkerFactory for advanced use cases.
   */
  public val workerFactory: WorkerFactory

  /**
   * The registered Kotlin worker interceptors.
   */
  internal val workerInterceptors: List<KWorkerInterceptor>

  init {
    val kOptions = KWorkerFactoryOptionsBuilder().apply(options).build()
    workerInterceptors = kOptions.workerInterceptors

    val factoryOptions = KOptionsConverters.toJava(kOptions)

    // Create WorkerFactory with KotlinPlugin added (including interceptors)
    val kotlinPlugin = KotlinPlugin.create(
      KotlinPluginOptions(
        workerInterceptors = workerInterceptors
      )
    )
    val factoryOptionsWithPlugin = WorkerFactoryOptions.newBuilder(factoryOptions)
      .addPlugin(kotlinPlugin)
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
    return KWorker(workerFactory.newWorker(taskQueue, workerOptions), workerInterceptors)
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
