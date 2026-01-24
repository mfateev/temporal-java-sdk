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

@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin

import io.temporal.client.WorkflowClient
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.worker.KWorker
import io.temporal.kotlin.worker.KWorkerFactory
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.workflow.Promise
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred

/**
 * Java SDK interoperability utilities.
 *
 * This object provides access to underlying Java SDK types from Kotlin SDK wrappers.
 * Use these utilities when you need to:
 * - Access Java SDK features not yet exposed in the Kotlin SDK
 * - Integrate with existing Java SDK code
 * - Use advanced Java SDK APIs directly
 *
 * Note: Using these APIs ties your code to Java SDK types and may require updates
 * when the Kotlin SDK evolves. Prefer using the Kotlin SDK APIs when possible.
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.JavaInterop
 *
 * // Access the underlying WorkflowClient
 * val workflowClient: WorkflowClient = JavaInterop.workflowClient(kClient)
 *
 * // Access the underlying WorkerFactory
 * val workerFactory: WorkerFactory = JavaInterop.workerFactory(kWorkerFactory)
 *
 * // Access the underlying Worker
 * val worker: Worker = JavaInterop.worker(kWorker)
 *
 * // Convert a Promise to a Deferred (inside workflows)
 * val deferred: Deferred<String> = JavaInterop.toDeferred(promise)
 * ```
 */
public object JavaInterop {

  /**
   * Returns the underlying [WorkflowClient] from a [KClient].
   *
   * Use this when you need to access Java SDK WorkflowClient features directly.
   *
   * @param client The Kotlin SDK client
   * @return The underlying Java SDK WorkflowClient
   */
  @JvmStatic
  public fun workflowClient(client: KClient): WorkflowClient = client.workflowClient

  /**
   * Returns the underlying [WorkerFactory] from a [KWorkerFactory].
   *
   * Use this when you need to access Java SDK WorkerFactory features directly.
   *
   * @param factory The Kotlin SDK worker factory
   * @return The underlying Java SDK WorkerFactory
   */
  @JvmStatic
  public fun workerFactory(factory: KWorkerFactory): WorkerFactory = factory.workerFactory

  /**
   * Returns the underlying [Worker] from a [KWorker].
   *
   * Use this when you need to access Java SDK Worker features directly.
   *
   * @param worker The Kotlin SDK worker
   * @return The underlying Java SDK Worker
   */
  @JvmStatic
  public fun worker(worker: KWorker): Worker = worker.worker

  /**
   * Converts a Temporal [Promise] to a Kotlin [Deferred].
   *
   * This is useful inside workflows when you need to work with Promise results
   * using Kotlin coroutine APIs.
   *
   * Note: This should only be used inside workflow code where Promises are available.
   *
   * @param promise The Temporal Promise to convert
   * @return A Kotlin Deferred that completes when the Promise completes
   */
  @JvmStatic
  public fun <R> toDeferred(promise: Promise<R>): Deferred<R> {
    val deferred = CompletableDeferred<R>()
    promise.handle { result, exception ->
      if (exception != null) {
        deferred.completeExceptionally(exception)
      } else {
        deferred.complete(result)
      }
      null
    }
    return deferred
  }

  /**
   * Awaits a Temporal [Promise] using Kotlin coroutine suspension.
   *
   * This is useful inside workflows when you need to await Promise results
   * using Kotlin suspend semantics.
   *
   * Note: This should only be used inside workflow code where Promises are available.
   *
   * @param promise The Temporal Promise to await
   * @return The result of the Promise
   */
  @JvmStatic
  public suspend fun <R> await(promise: Promise<R>): R = toDeferred(promise).await()
}

/**
 * Extension property to access the underlying [WorkflowClient] from a [KClient].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorkflowClient
 *
 * val workflowClient: WorkflowClient = kClient.javaWorkflowClient
 * ```
 */
public val KClient.javaWorkflowClient: WorkflowClient
  get() = workflowClient

/**
 * Extension property to access the underlying [WorkerFactory] from a [KWorkerFactory].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorkerFactory
 *
 * val workerFactory: WorkerFactory = kWorkerFactory.javaWorkerFactory
 * ```
 */
public val KWorkerFactory.javaWorkerFactory: WorkerFactory
  get() = workerFactory

/**
 * Extension property to access the underlying [Worker] from a [KWorker].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorker
 *
 * val worker: Worker = kWorker.javaWorker
 * ```
 */
public val KWorker.javaWorker: Worker
  get() = worker

/**
 * Extension function to convert a Temporal [Promise] to a Kotlin [Deferred].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.toKotlinDeferred
 *
 * val deferred: Deferred<String> = promise.toKotlinDeferred()
 * ```
 */
public fun <R> Promise<R>.toKotlinDeferred(): Deferred<R> = JavaInterop.toDeferred(this)

/**
 * Extension function to await a Temporal [Promise] using Kotlin coroutine suspension.
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.awaitKotlin
 *
 * val result: String = promise.awaitKotlin()
 * ```
 */
public suspend fun <R> Promise<R>.awaitKotlin(): R = JavaInterop.await(this)
