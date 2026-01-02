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

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KTypedWorkflowHandleImpl
import io.temporal.kotlin.internal.KWorkflowHandleImpl
import io.temporal.kotlin.internal.WorkflowHandleImpl
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.workflow.WorkflowMethod
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

/**
 * Kotlin workflow client providing suspend functions and type-safe workflow APIs.
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
 * @param service The WorkflowServiceStubs to connect to
 * @param options DSL builder for WorkflowClientOptions
 */
public class KWorkflowClient(
  service: WorkflowServiceStubs,
  options: WorkflowClientOptions.Builder.() -> Unit = {}
) {

  /**
   * The underlying WorkflowClient for advanced use cases.
   */
  public val workflowClient: WorkflowClient = WorkflowClient.newInstance(
    service,
    WorkflowClientOptions.newBuilder().apply(options).build()
  )

  /**
   * Creates a KWorkflowClient wrapping an existing WorkflowClient.
   */
  public constructor(client: WorkflowClient) : this(client.workflowServiceStubs) {
    // Note: This creates a new WorkflowClient with default options
    // For exact wrapping, use the workflowClient property directly
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
  @OptIn(InternalTemporalApi::class)
  public fun <T> getWorkflowHandle(workflowId: String, workflowClass: Class<T>): KWorkflowHandle<T> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId)
    return KWorkflowHandleImpl(stub, workflowClass)
  }

  /**
   * Get a typed handle for an existing workflow by ID and run ID.
   */
  @OptIn(InternalTemporalApi::class)
  public fun <T> getWorkflowHandle(workflowId: String, runId: String, workflowClass: Class<T>): KWorkflowHandle<T> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId, java.util.Optional.of(runId), java.util.Optional.empty())
    return KWorkflowHandleImpl(stub, workflowClass)
  }

  /**
   * Get an untyped handle for an existing workflow by ID.
   * Use when you don't know the workflow type at compile time.
   */
  @OptIn(InternalTemporalApi::class)
  public fun getUntypedWorkflowHandle(workflowId: String): WorkflowHandle {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId)
    return WorkflowHandleImpl(stub)
  }

  /**
   * Get an untyped handle for an existing workflow by ID and run ID.
   */
  @OptIn(InternalTemporalApi::class)
  public fun getUntypedWorkflowHandle(workflowId: String, runId: String): WorkflowHandle {
    val stub = workflowClient.newUntypedWorkflowStub(workflowId, java.util.Optional.of(runId), java.util.Optional.empty())
    return WorkflowHandleImpl(stub)
  }

  // ========== Signal With Start ==========

  /**
   * Atomically start a workflow and send a signal.
   * If the workflow already exists, only the signal is sent.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, A1, R, SA1> signalWithStart(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    workflowArg: A1,
    signal: KFunction2<T, SA1, *>,
    signalArg: SA1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val signalName = extractSignalName(signal)

    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options.toJavaOptions())
    val signalArgs: Array<Any?> = arrayOf(signalArg)
    val workflowArgs: Array<Any?> = arrayOf(workflowArg)
    val execution = stub.signalWithStart(signalName, signalArgs, workflowArgs)

    @Suppress("UNCHECKED_CAST")
    return KTypedWorkflowHandleImpl(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
      resultClass as Class<R>
    )
  }

  /**
   * Atomically start a workflow (no args) and send a signal.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun <T, R, SA1> signalWithStart(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions,
    signal: KFunction2<T, SA1, *>,
    signalArg: SA1
  ): KTypedWorkflowHandle<T, R> {
    val (workflowType, resultClass) = extractWorkflowMetadata(workflow)
    val signalName = extractSignalName(signal)

    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options.toJavaOptions())
    val signalArgs: Array<Any?> = arrayOf(signalArg)
    val execution = stub.signalWithStart(signalName, signalArgs, emptyArray())

    @Suppress("UNCHECKED_CAST")
    return KTypedWorkflowHandleImpl(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
      resultClass as Class<R>
    )
  }

  // ========== Internal Helpers ==========

  @OptIn(InternalTemporalApi::class)
  @Suppress("UNCHECKED_CAST")
  private fun <T, R> startWorkflowInternal(
    workflowType: String,
    resultClass: Class<R>,
    options: WorkflowOptions,
    vararg args: Any?
  ): KTypedWorkflowHandle<T, R> {
    val stub = workflowClient.newUntypedWorkflowStub(workflowType, options)
    val execution = stub.start(*args)
    return KTypedWorkflowHandleImpl<T, R>(
      workflowClient.newUntypedWorkflowStub(execution, java.util.Optional.empty()),
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
}
