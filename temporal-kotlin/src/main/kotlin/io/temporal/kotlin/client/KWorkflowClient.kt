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

package io.temporal.kotlin.client

import io.temporal.client.UpdateOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowUpdateStage
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.workflow.UpdateMethod
import io.temporal.workflow.WorkflowMethod
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3
import kotlin.reflect.KFunction4
import kotlin.reflect.KFunction5
import kotlin.reflect.KFunction6
import kotlin.reflect.KFunction7
import kotlin.reflect.KSuspendFunction1
import kotlin.reflect.KSuspendFunction2
import kotlin.reflect.KSuspendFunction3
import kotlin.reflect.KSuspendFunction4
import kotlin.reflect.KSuspendFunction5
import kotlin.reflect.KSuspendFunction6
import kotlin.reflect.KSuspendFunction7
import kotlin.reflect.jvm.javaMethod

// TODO: Switch from Dispatchers.IO + blocking Java SDK calls to fully async implementation
//  using gRPC async client. This will eliminate thread pool overhead and provide true
//  non-blocking suspension.

/**
 * Kotlin workflow client providing suspend functions and type-safe workflow APIs.
 *
 * This class wraps a [WorkflowClient] and provides Kotlin-idiomatic APIs including
 * suspend functions and type-safe method references.
 *
 * Example:
 * ```kotlin
 * val service = WorkflowServiceStubs.newLocalServiceStubs()
 * val client = KWorkflowClient(service) {
 *     setNamespace("default")
 * }
 *
 * // Execute workflow and wait for result
 * val result = client.executeWorkflow(
 *     GreetingWorkflow::getGreeting,
 *     KWorkflowOptions(
 *         workflowId = "greeting-123",
 *         taskQueue = "greeting-queue"
 *     ),
 *     "Temporal"
 * )
 * ```
 *
 * @param workflowClient The underlying WorkflowClient to wrap
 */
public class KWorkflowClient(
  public val workflowClient: WorkflowClient
) {

  public companion object {
    /**
     * Create a KWorkflowClient connected to the specified service.
     *
     * Example:
     * ```kotlin
     * val client = KWorkflowClient(service) {
     *     setNamespace("my-namespace")
     * }
     * ```
     *
     * @param service The WorkflowServiceStubs to connect to
     * @param options DSL builder for WorkflowClientOptions
     * @return A new KWorkflowClient instance
     */
    @JvmStatic
    public operator fun invoke(
      service: WorkflowServiceStubs,
      options: WorkflowClientOptions.Builder.() -> Unit = {}
    ): KWorkflowClient {
      val client = WorkflowClient.newInstance(
        service,
        WorkflowClientOptions.newBuilder().apply(options).build()
      )
      return KWorkflowClient(client)
    }
  }

  // ========== Start Workflow (0-6 args) ==========

  /**
   * Start a workflow with no arguments and return a handle for interaction.
   * Does not wait for the workflow to complete.
   */
  public suspend fun <T, R> startWorkflow(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions())
  }

  /**
   * Start a workflow with one argument and return a handle.
   */
  public suspend fun <T, A1, R> startWorkflow(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1)
  }

  /**
   * Start a workflow with two arguments and return a handle.
   */
  public suspend fun <T, A1, A2, R> startWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2)
  }

  /**
   * Start a workflow with three arguments and return a handle.
   */
  public suspend fun <T, A1, A2, A3, R> startWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3)
  }

  /**
   * Start a workflow with four arguments and return a handle.
   */
  public suspend fun <T, A1, A2, A3, A4, R> startWorkflow(
    workflow: KFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4)
  }

  /**
   * Start a workflow with five arguments and return a handle.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> startWorkflow(
    workflow: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5)
  }

  /**
   * Start a workflow with six arguments and return a handle.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> startWorkflow(
    workflow: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5, arg6)
  }

  // ========== Start Suspend Workflow (0-6 args) ==========
  // These overloads support suspend workflow methods (suspend fun in interfaces)

  /**
   * Start a suspend workflow with no arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow0")
  public suspend fun <T, R> startWorkflow(
    workflow: KSuspendFunction1<T, R>,
    options: KWorkflowOptions
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions())
  }

  /**
   * Start a suspend workflow with one argument and return a handle.
   */
  @JvmName("startSuspendWorkflow1")
  public suspend fun <T, A1, R> startWorkflow(
    workflow: KSuspendFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1)
  }

  /**
   * Start a suspend workflow with two arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow2")
  public suspend fun <T, A1, A2, R> startWorkflow(
    workflow: KSuspendFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2)
  }

  /**
   * Start a suspend workflow with three arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow3")
  public suspend fun <T, A1, A2, A3, R> startWorkflow(
    workflow: KSuspendFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3)
  }

  /**
   * Start a suspend workflow with four arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow4")
  public suspend fun <T, A1, A2, A3, A4, R> startWorkflow(
    workflow: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4)
  }

  /**
   * Start a suspend workflow with five arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow5")
  public suspend fun <T, A1, A2, A3, A4, A5, R> startWorkflow(
    workflow: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5)
  }

  /**
   * Start a suspend workflow with six arguments and return a handle.
   */
  @JvmName("startSuspendWorkflow6")
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> startWorkflow(
    workflow: KSuspendFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return startWorkflowInternal(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5, arg6)
  }

  // ========== Execute Workflow (0-6 args) ==========

  /**
   * Start a workflow with no arguments and wait for its result.
   * Suspends until the workflow completes.
   */
  public suspend fun <T, R> executeWorkflow(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions
  ): R {
    val handle = startWorkflow(workflow, options)
    return handle.result()
  }

  /**
   * Start a workflow with one argument and wait for its result.
   */
  public suspend fun <T, A1, R> executeWorkflow(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): R {
    val handle = startWorkflow(workflow, options, arg1)
    return handle.result()
  }

  /**
   * Start a workflow with two arguments and wait for its result.
   */
  public suspend fun <T, A1, A2, R> executeWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): R {
    val handle = startWorkflow(workflow, options, arg1, arg2)
    return handle.result()
  }

  /**
   * Start a workflow with three arguments and wait for its result.
   */
  public suspend fun <T, A1, A2, A3, R> executeWorkflow(
    workflow: KFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): R {
    val handle = startWorkflow(workflow, options, arg1, arg2, arg3)
    return handle.result()
  }

  /**
   * Start a workflow with four arguments and wait for its result.
   */
  public suspend fun <T, A1, A2, A3, A4, R> executeWorkflow(
    workflow: KFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): R {
    val handle = startWorkflow(workflow, options, arg1, arg2, arg3, arg4)
    return handle.result()
  }

  /**
   * Start a workflow with five arguments and wait for its result.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeWorkflow(
    workflow: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): R {
    val handle = startWorkflow(workflow, options, arg1, arg2, arg3, arg4, arg5)
    return handle.result()
  }

  /**
   * Start a workflow with six arguments and wait for its result.
   */
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeWorkflow(
    workflow: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): R {
    val handle = startWorkflow(workflow, options, arg1, arg2, arg3, arg4, arg5, arg6)
    return handle.result()
  }

  // ========== Suspend Workflow Method Overloads ==========
  // These overloads support workflow methods defined as suspend functions

  /**
   * Start a suspend workflow with no arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow0")
  public suspend fun <T, R> executeWorkflow(
    workflow: KSuspendFunction1<T, R>,
    options: KWorkflowOptions
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions())
    return handle.result()
  }

  /**
   * Start a suspend workflow with one argument and wait for its result.
   */
  @JvmName("executeSuspendWorkflow1")
  public suspend fun <T, A1, R> executeWorkflow(
    workflow: KSuspendFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1)
    return handle.result()
  }

  /**
   * Start a suspend workflow with two arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow2")
  public suspend fun <T, A1, A2, R> executeWorkflow(
    workflow: KSuspendFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2)
    return handle.result()
  }

  /**
   * Start a suspend workflow with three arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow3")
  public suspend fun <T, A1, A2, A3, R> executeWorkflow(
    workflow: KSuspendFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3)
    return handle.result()
  }

  /**
   * Start a suspend workflow with four arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow4")
  public suspend fun <T, A1, A2, A3, A4, R> executeWorkflow(
    workflow: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4)
    return handle.result()
  }

  /**
   * Start a suspend workflow with five arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow5")
  public suspend fun <T, A1, A2, A3, A4, A5, R> executeWorkflow(
    workflow: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5)
    return handle.result()
  }

  /**
   * Start a suspend workflow with six arguments and wait for its result.
   */
  @JvmName("executeSuspendWorkflow6")
  public suspend fun <T, A1, A2, A3, A4, A5, A6, R> executeWorkflow(
    workflow: KSuspendFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): R {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)

    @Suppress("UNCHECKED_CAST")
    val handle = startWorkflowInternal<T, R>(workflowType, resultClass as Class<R>, options.toJavaOptions(), arg1, arg2, arg3, arg4, arg5, arg6)
    return handle.result()
  }

  // ========== Get Workflow Handle ==========

  /**
   * Get a typed handle for an existing workflow by ID.
   * Use this to signal, query, or get results from a workflow started elsewhere.
   */
  public inline fun <reified T> getWorkflowHandle(workflowId: String): KWorkflowHandle<T> {
    return getWorkflowHandle(workflowId, T::class.java)
  }

  /**
   * Get a typed handle for an existing workflow by ID and run ID.
   */
  public inline fun <reified T> getWorkflowHandle(workflowId: String, runId: String): KWorkflowHandle<T> {
    return getWorkflowHandle(workflowId, runId, T::class.java)
  }

  /**
   * Get a typed handle for an existing workflow by ID.
   */
  public fun <T> getWorkflowHandle(workflowId: String, workflowClass: Class<T>): KWorkflowHandle<T> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId)
    return KWorkflowHandle(stub, workflowClass)
  }

  /**
   * Get a typed handle for an existing workflow by ID and run ID.
   */
  public fun <T> getWorkflowHandle(workflowId: String, runId: String, workflowClass: Class<T>): KWorkflowHandle<T> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId, java.util.Optional.of(runId), java.util.Optional.empty())
    return KWorkflowHandle(stub, workflowClass)
  }

  /**
   * Get an untyped handle for an existing workflow by ID.
   * Use when you don't know the workflow type at compile time.
   */
  public fun getUntypedWorkflowHandle(workflowId: String): WorkflowHandle {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId)
    return WorkflowHandle(stub)
  }

  /**
   * Get an untyped handle for an existing workflow by ID and run ID.
   */
  public fun getUntypedWorkflowHandle(workflowId: String, runId: String): WorkflowHandle {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId, java.util.Optional.of(runId), java.util.Optional.empty())
    return WorkflowHandle(stub)
  }

  // ========== Signal With Start ==========

  /**
   * Atomically start a workflow and send a signal.
   * If the workflow already exists, only the signal is sent.
   */
  public suspend fun <T, A1, R, SA1> signalWithStart(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    workflowArg: A1,
    signal: KFunction2<T, SA1, *>,
    signalArg: SA1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    val signalName = extractSignalName(signal)

    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options.toJavaOptions())
    val signalArgs: Array<Any?> = arrayOf(signalArg)
    val workflowArgs: Array<Any?> = arrayOf(workflowArg)
    val execution = stub.signalWithStart(signalName, signalArgs, workflowArgs)

    @Suppress("UNCHECKED_CAST")
    return KTypedWorkflowHandle(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
      workflowClass as Class<T>,
      resultClass as Class<R>
    )
  }

  /**
   * Atomically start a workflow (no args) and send a signal.
   */
  public suspend fun <T, R, SA1> signalWithStart(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions,
    signal: KFunction2<T, SA1, *>,
    signalArg: SA1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    val signalName = extractSignalName(signal)

    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options.toJavaOptions())
    val signalArgs: Array<Any?> = arrayOf(signalArg)
    val execution = stub.signalWithStart(signalName, signalArgs, emptyArray())

    @Suppress("UNCHECKED_CAST")
    return KTypedWorkflowHandle(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
      workflowClass as Class<T>,
      resultClass as Class<R>
    )
  }

  // ========== With Start Workflow Operation (0-6 args) ==========

  /**
   * Create a workflow start operation for use with update-with-start.
   * Captures the workflow method, arguments, and options for atomic execution.
   *
   * @param workflow the workflow method reference (no arguments)
   * @param options workflow options including workflowIdConflictPolicy
   * @return a start operation that can be used with [startUpdateWithStart] or [executeUpdateWithStart]
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, R> newWithStartWorkflowOperation(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      emptyArray()
    )
  }

  /**
   * Create a workflow start operation with one argument.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, R> newWithStartWorkflowOperation(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1)
    )
  }

  /**
   * Create a workflow start operation with two arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, R> newWithStartWorkflowOperation(
    workflow: KFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2)
    )
  }

  /**
   * Create a workflow start operation with three arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, R> newWithStartWorkflowOperation(
    workflow: KFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3)
    )
  }

  /**
   * Create a workflow start operation with four arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, R> newWithStartWorkflowOperation(
    workflow: KFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4)
    )
  }

  /**
   * Create a workflow start operation with five arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, A5, R> newWithStartWorkflowOperation(
    workflow: KFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4, arg5)
    )
  }

  /**
   * Create a workflow start operation with six arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, A5, A6, R> newWithStartWorkflowOperation(
    workflow: KFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4, arg5, arg6)
    )
  }

  // ========== With Start Workflow Operation - Suspend Variants (0-6 args) ==========

  /**
   * Create a workflow start operation for a suspend workflow method.
   */
  @JvmName("withStartSuspendWorkflowOperation0")
  @OptIn(InternalTemporalApi::class)
  public fun <T, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction1<T, R>,
    options: KWorkflowOptions
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      emptyArray()
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with one argument.
   */
  @JvmName("withStartSuspendWorkflowOperation1")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1)
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with two arguments.
   */
  @JvmName("withStartSuspendWorkflowOperation2")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2)
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with three arguments.
   */
  @JvmName("withStartSuspendWorkflowOperation3")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction4<T, A1, A2, A3, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3)
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with four arguments.
   */
  @JvmName("withStartSuspendWorkflowOperation4")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction5<T, A1, A2, A3, A4, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4)
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with five arguments.
   */
  @JvmName("withStartSuspendWorkflowOperation5")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, A5, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction6<T, A1, A2, A3, A4, A5, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4, arg5)
    )
  }

  /**
   * Create a workflow start operation for a suspend workflow method with six arguments.
   */
  @JvmName("withStartSuspendWorkflowOperation6")
  @OptIn(InternalTemporalApi::class)
  public fun <T, A1, A2, A3, A4, A5, A6, R> newWithStartWorkflowOperation(
    workflow: KSuspendFunction7<T, A1, A2, A3, A4, A5, A6, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2,
    arg3: A3,
    arg4: A4,
    arg5: A5,
    arg6: A6
  ): KWithStartWorkflowOperation<T, R> {
    val (workflowType, workflowClass, resultClass) = extractFullWorkflowMetadata(workflow)
    @Suppress("UNCHECKED_CAST")
    return KWithStartWorkflowOperation(
      workflowType,
      workflowClass as Class<T>,
      resultClass as Class<R>,
      options.toJavaOptions(),
      arrayOf<Any?>(arg1, arg2, arg3, arg4, arg5, arg6)
    )
  }

  // ========== Start Update With Start (0-6 update args) ==========

  /**
   * Atomically start a workflow and send an update, returning immediately after
   * the update reaches the specified wait stage.
   *
   * If the workflow is not running, starts it and sends the update.
   * Behavior for existing workflows depends on [KWorkflowOptions.workflowIdConflictPolicy]:
   * - USE_EXISTING: sends update to existing workflow
   * - FAIL: throws WorkflowExecutionAlreadyStarted
   *
   * @param update Update method reference (must be a suspend function)
   * @param options Options containing the start operation and wait stage
   * @return Handle to track the update result
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UR> startUpdateWithStart(
    update: KSuspendFunction1<T, UR>,
    options: KUpdateWithStartOptions<T, R, UR>
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      emptyArray()
    )
  }

  /**
   * Start update with start - one update argument.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UR> startUpdateWithStart(
    update: KSuspendFunction2<T, UA1, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1)
    )
  }

  /**
   * Start update with start - two update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UR> startUpdateWithStart(
    update: KSuspendFunction3<T, UA1, UA2, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2)
    )
  }

  /**
   * Start update with start - three update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UR> startUpdateWithStart(
    update: KSuspendFunction4<T, UA1, UA2, UA3, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3)
    )
  }

  /**
   * Start update with start - four update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UR> startUpdateWithStart(
    update: KSuspendFunction5<T, UA1, UA2, UA3, UA4, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4)
    )
  }

  /**
   * Start update with start - five update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UA5, UR> startUpdateWithStart(
    update: KSuspendFunction6<T, UA1, UA2, UA3, UA4, UA5, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4,
    updateArg5: UA5
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4, updateArg5)
    )
  }

  /**
   * Start update with start - six update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UA5, UA6, UR> startUpdateWithStart(
    update: KSuspendFunction7<T, UA1, UA2, UA3, UA4, UA5, UA6, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4,
    updateArg5: UA5,
    updateArg6: UA6
  ): KUpdateHandle<UR> {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return startUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4, updateArg5, updateArg6)
    )
  }

  // ========== Execute Update With Start (0-6 update args) ==========

  /**
   * Atomically start a workflow and execute an update, waiting for completion.
   * Convenience method equivalent to startUpdateWithStart with waitForStage=COMPLETED.
   *
   * @param update Update method reference (must be a suspend function)
   * @param options Options containing the start operation
   * @return The update result
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UR> executeUpdateWithStart(
    update: KSuspendFunction1<T, UR>,
    options: KUpdateWithStartOptions<T, R, UR>
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      emptyArray()
    )
  }

  /**
   * Execute update with start - one update argument.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UR> executeUpdateWithStart(
    update: KSuspendFunction2<T, UA1, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1)
    )
  }

  /**
   * Execute update with start - two update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UR> executeUpdateWithStart(
    update: KSuspendFunction3<T, UA1, UA2, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2)
    )
  }

  /**
   * Execute update with start - three update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UR> executeUpdateWithStart(
    update: KSuspendFunction4<T, UA1, UA2, UA3, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3)
    )
  }

  /**
   * Execute update with start - four update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UR> executeUpdateWithStart(
    update: KSuspendFunction5<T, UA1, UA2, UA3, UA4, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4)
    )
  }

  /**
   * Execute update with start - five update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UA5, UR> executeUpdateWithStart(
    update: KSuspendFunction6<T, UA1, UA2, UA3, UA4, UA5, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4,
    updateArg5: UA5
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4, updateArg5)
    )
  }

  /**
   * Execute update with start - six update arguments.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, UA1, UA2, UA3, UA4, UA5, UA6, UR> executeUpdateWithStart(
    update: KSuspendFunction7<T, UA1, UA2, UA3, UA4, UA5, UA6, UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArg1: UA1,
    updateArg2: UA2,
    updateArg3: UA3,
    updateArg4: UA4,
    updateArg5: UA5,
    updateArg6: UA6
  ): UR {
    val (updateName, updateResultClass) = extractUpdateMetadata(update)
    @Suppress("UNCHECKED_CAST")
    return executeUpdateWithStartInternal(
      updateName,
      updateResultClass as Class<UR>,
      options,
      arrayOf<Any?>(updateArg1, updateArg2, updateArg3, updateArg4, updateArg5, updateArg6)
    )
  }

  // ========== Internal Helpers ==========

  @Suppress("UNCHECKED_CAST")
  private fun <T, R> startWorkflowInternal(
    workflowType: String,
    resultClass: Class<R>,
    options: WorkflowOptions,
    vararg args: Any?
  ): KTypedWorkflowHandle<T, R> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options)
    val execution = stub.start(*args)
    // We don't have the workflow interface class here, so we use Any::class.java
    // This is safe because the handle methods that need the interface class
    // are defined on the typed handle which already has the correct type parameter
    @Suppress("UNCHECKED_CAST")
    return KTypedWorkflowHandle(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
      Any::class.java as Class<T>,
      resultClass
    )
  }

  private fun extractWorkflowMetadata(workflow: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = workflow.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve workflow method reference")

    // Get workflow type from annotation or method name
    val workflowMethod = javaMethod.getAnnotation(WorkflowMethod::class.java)
    val workflowType = if (workflowMethod != null && workflowMethod.name.isNotEmpty()) {
      workflowMethod.name
    } else {
      // Default to interface name
      javaMethod.declaringClass.simpleName
    }

    val resultClass = javaMethod.returnType
    return Pair(workflowType, resultClass)
  }

  private fun extractSignalName(signal: KFunction<*>): String {
    val javaMethod = signal.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve signal method reference")

    val signalMethod = javaMethod.getAnnotation(io.temporal.workflow.SignalMethod::class.java)
    return if (signalMethod != null && signalMethod.name.isNotEmpty()) {
      signalMethod.name
    } else {
      javaMethod.name
    }
  }

  /**
   * Extracts workflow type, declaring class, and result class from a workflow method reference.
   */
  private fun extractFullWorkflowMetadata(workflow: KFunction<*>): Triple<String, Class<*>, Class<*>> {
    val javaMethod = workflow.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve workflow method reference")

    // Get workflow type from annotation or method name
    val workflowMethod = javaMethod.getAnnotation(WorkflowMethod::class.java)
    val workflowType = if (workflowMethod != null && workflowMethod.name.isNotEmpty()) {
      workflowMethod.name
    } else {
      // Default to interface name
      javaMethod.declaringClass.simpleName
    }

    val workflowClass = javaMethod.declaringClass
    val resultClass = javaMethod.returnType
    return Triple(workflowType, workflowClass, resultClass)
  }

  /**
   * Extracts update name and result class from an update method reference.
   */
  private fun extractUpdateMetadata(update: KFunction<*>): Pair<String, Class<*>> {
    val javaMethod = update.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve update method reference")

    val updateMethod = javaMethod.getAnnotation(UpdateMethod::class.java)
    val updateName = if (updateMethod != null && updateMethod.name.isNotEmpty()) {
      updateMethod.name
    } else {
      javaMethod.name
    }

    val resultClass = javaMethod.returnType
    return Pair(updateName, resultClass)
  }

  /**
   * Internal implementation for startUpdateWithStart.
   */
  @OptIn(InternalTemporalApi::class)
  private suspend fun <T, R, UR> startUpdateWithStartInternal(
    updateName: String,
    updateResultClass: Class<UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArgs: Array<out Any?>
  ): KUpdateHandle<UR> {
    val startOp = options.startWorkflowOperation

    if (!startOp.markInvoked()) {
      throw IllegalStateException("WithStartWorkflowOperation was already executed")
    }

    require(options.waitForStage != WorkflowUpdateStage.ADMITTED) {
      "waitForStage cannot be ADMITTED"
    }

    return withContext(Dispatchers.IO) {
      val stub = startOp.createStub(workflowClient)

      val updateOptions = UpdateOptions.newBuilder(updateResultClass)
        .setUpdateName(updateName)
        .setWaitForStage(options.waitForStage)
        .apply { options.updateId?.let { setUpdateId(it) } }
        .build()

      val handle = stub.startUpdateWithStart(updateOptions, updateArgs, startOp.args)

      KUpdateHandle(handle)
    }
  }

  /**
   * Internal implementation for executeUpdateWithStart.
   */
  @OptIn(InternalTemporalApi::class)
  private suspend fun <T, R, UR> executeUpdateWithStartInternal(
    updateName: String,
    updateResultClass: Class<UR>,
    options: KUpdateWithStartOptions<T, R, UR>,
    updateArgs: Array<out Any?>
  ): UR {
    val startOp = options.startWorkflowOperation

    if (!startOp.markInvoked()) {
      throw IllegalStateException("WithStartWorkflowOperation was already executed")
    }

    return withContext(Dispatchers.IO) {
      val stub = startOp.createStub(workflowClient)

      val updateOptions = UpdateOptions.newBuilder(updateResultClass)
        .setUpdateName(updateName)
        .setWaitForStage(WorkflowUpdateStage.COMPLETED)
        .apply { options.updateId?.let { setUpdateId(it) } }
        .build()

      stub.executeUpdateWithStart(updateOptions, updateArgs, startOp.args)
    }
  }
}
