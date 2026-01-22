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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin.internal

import io.temporal.api.common.v1.Header
import io.temporal.api.common.v1.Payloads
import io.temporal.api.history.v1.HistoryEvent
import io.temporal.api.query.v1.WorkflowQuery
import io.temporal.common.converter.DataConverter
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.internal.replay.WorkflowContext
import io.temporal.internal.statemachines.UpdateProtocolCallback
import io.temporal.kotlin.common.KEncodedValues
import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalInput
import io.temporal.kotlin.interceptor.KUpdateInput
import io.temporal.kotlin.interceptor.KUpdateOutput
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInput
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowOutput
import io.temporal.kotlin.internal.interceptor.InterceptorChain
import io.temporal.kotlin.internal.interceptor.RootWorkflowInboundCallsInterceptor
import io.temporal.kotlin.internal.interceptor.RootWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.internal.interceptor.WorkflowExecutor
import io.temporal.kotlin.workflow.KDynamicWorkflow
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance

/**
 * Implementation of [ReplayWorkflow] for dynamic Kotlin workflows using coroutines.
 *
 * This class manages the execution of a [KDynamicWorkflow] implementation,
 * integrating with Temporal's replay mechanism through a custom coroutine
 * dispatcher that ensures deterministic execution.
 *
 * Dynamic workflows receive all arguments as [KEncodedValues] and can handle
 * any workflow type at runtime. This class provides the same coroutine support
 * as [KotlinReplayWorkflow] but for dynamic workflow implementations.
 */
@InternalTemporalApi
internal class KotlinDynamicReplayWorkflow(
  private val dynamicWorkflowClass: KClass<out KDynamicWorkflow>,
  private val dataConverter: DataConverter,
  private val deadlockDetectionTimeoutMs: Long,
  private val workerInterceptors: List<KWorkerInterceptor> = emptyList()
) : ReplayWorkflow {

  private var workflowContext: KotlinWorkflowContext? = null
  private var replayContext: ReplayWorkflowContext? = null
  private var dispatcher: KotlinCoroutineDispatcher? = null
  private var coroutineScope: CoroutineScope? = null
  private var workflowJob: Job? = null
  private var inboundInterceptor: KWorkflowInboundCallsInterceptor? = null

  private val workflowInstance = AtomicReference<KDynamicWorkflow?>(null)
  private val workflowOutput = AtomicReference<Optional<Payloads>>(Optional.empty())
  private val workflowException = AtomicReference<Throwable?>(null)
  private val workflowCompleted = AtomicBoolean(false)
  private val cancelled = AtomicBoolean(false)

  // Store the input payloads for execution
  private var inputPayloads: Optional<Payloads> = Optional.empty()

  override fun start(event: HistoryEvent, context: ReplayWorkflowContext) {
    this.replayContext = context
    this.workflowContext = KotlinWorkflowContext(context, dataConverter)
    this.dispatcher = KotlinCoroutineDispatcher(workflowContext!!)
    // Set dispatcher reference for explicit dispatch operations
    this.workflowContext!!.dispatcher = this.dispatcher

    // Create coroutine scope with our deterministic dispatcher
    val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
      workflowException.set(throwable)
      workflowCompleted.set(true)
    }

    // WorkflowContextElement ensures the workflow context ThreadLocal is properly
    // set when coroutines run, including nested async blocks
    val contextElement = WorkflowContextElement(workflowContext!!)

    this.coroutineScope = CoroutineScope(
      dispatcher!! + SupervisorJob() + exceptionHandler + contextElement
    )

    // Set coroutine scope reference for async operations
    this.workflowContext!!.coroutineScope = this.coroutineScope

    // Create workflow instance
    val instance = dynamicWorkflowClass.createInstance()
    workflowInstance.set(instance)

    // Build interceptor chain
    val executor = DynamicWorkflowExecutor()
    val rootInterceptor = RootWorkflowInboundCallsInterceptor(executor)
    inboundInterceptor = InterceptorChain.buildWorkflowInboundChain(workerInterceptors, rootInterceptor)

    // Extract input from the start event
    val startedAttributes = event.workflowExecutionStartedEventAttributes
    inputPayloads = if (startedAttributes.hasInput()) {
      Optional.of(startedAttributes.input)
    } else {
      Optional.empty()
    }

    // Extract header from the start event
    val startHeader = startedAttributes.header
    val headerMap = io.temporal.common.interceptors.Header(startHeader.fieldsMap)

    // For dynamic workflows, we pass the raw payloads as the argument
    // The execute method receives KEncodedValues which wraps these payloads
    val workflowInput = KWorkflowInput(
      header = headerMap,
      arguments = emptyArray() // Arguments handled via KEncodedValues
    )

    // Launch the workflow coroutine
    workflowJob = coroutineScope!!.launch {
      try {
        // Execute through the interceptor chain
        val interceptor = inboundInterceptor!!

        // Initialize the interceptor chain with the real outbound interceptor
        val rootOutbound = RootWorkflowOutboundCallsInterceptor(workflowContext!!, dataConverter)
        interceptor.init(rootOutbound)

        // Execute the workflow through interceptors
        val output = interceptor.execute(workflowInput)

        // Serialize the result
        val resultPayloads = if (output.result != null && output.result != Unit) {
          Optional.of(dataConverter.toPayloads(output.result).orElse(Payloads.getDefaultInstance()))
        } else {
          Optional.empty()
        }
        workflowOutput.set(resultPayloads)
        workflowCompleted.set(true)
      } catch (e: CancellationException) {
        // Workflow was cancelled
        workflowException.set(e)
        workflowCompleted.set(true)
      } catch (e: Throwable) {
        workflowException.set(e)
        workflowCompleted.set(true)
      }
    }
  }

  override fun handleSignal(
    signalName: String,
    input: Optional<Payloads>,
    eventId: Long,
    header: Header
  ) {
    val ctx = workflowContext ?: return
    val interceptor = inboundInterceptor ?: return

    // Create encoded values for dynamic handlers
    val encodedValues = ctx.createEncodedValues(input)

    // Create signal input for interceptor
    val headerMap = io.temporal.common.interceptors.Header(header.fieldsMap)
    val signalInput = KSignalInput(
      signalName = signalName,
      arguments = emptyArray(),
      encodedValues = encodedValues,
      eventId = eventId,
      header = headerMap
    )

    // Execute through interceptor chain
    // Signals execute immediately (before main workflow) to prevent data loss on closure.
    // For dynamic handlers, signals are buffered until a handler is registered.
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        ctx.runningSignalHandlers.incrementAndGet()
        try {
          interceptor.handleSignal(signalInput)
        } catch (e: Throwable) {
          workflowContext?.failWorkflowTask(e)
        } finally {
          ctx.runningSignalHandlers.decrementAndGet()
        }
      }
    }
  }

  override fun handleUpdate(
    updateName: String,
    updateId: String,
    input: Optional<Payloads>,
    eventId: Long,
    header: Header,
    callbacks: UpdateProtocolCallback
  ) {
    val ctx = workflowContext
    if (ctx == null) {
      callbacks.reject(createFailure("Workflow context not initialized"))
      return
    }

    val interceptor = inboundInterceptor
    if (interceptor == null) {
      callbacks.reject(createFailure("Inbound interceptor not initialized"))
      return
    }

    // Create encoded values for dynamic handlers
    val encodedValues = ctx.createEncodedValues(input)

    // Create update input for interceptor
    val headerMap = io.temporal.common.interceptors.Header(header.fieldsMap)
    val updateInput = KUpdateInput(
      updateName = updateName,
      arguments = emptyArray(),
      encodedValues = encodedValues,
      header = headerMap
    )

    // Execute validation and update handler in the workflow context
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        // Run validation first (must happen before accept)
        try {
          interceptor.validateUpdate(updateInput)
        } catch (e: Throwable) {
          val msg = e.message ?: "[${e.javaClass.name}] Update validation failed"
          callbacks.reject(createFailure(msg, e))
          return@launch
        }

        // Accept the update - validation passed
        callbacks.accept()

        ctx.runningUpdateHandlers.incrementAndGet()
        ctx.currentUpdateInfo.set(KUpdateInfo(updateName, updateId))
        try {
          // Execute through interceptor chain
          val output = interceptor.executeUpdate(updateInput)

          // Complete with result
          val resultPayloads = if (output.result != null && output.result != Unit) {
            dataConverter.toPayloads(output.result)
          } else {
            Optional.empty()
          }
          callbacks.complete(resultPayloads, null)
        } catch (e: Throwable) {
          callbacks.complete(Optional.empty(), createFailure(e.message ?: "Update failed", e))
        } finally {
          ctx.currentUpdateInfo.set(null)
          ctx.runningUpdateHandlers.decrementAndGet()
        }
      }
    }
  }

  private fun createFailure(message: String, cause: Throwable? = null): io.temporal.api.failure.v1.Failure {
    val builder = io.temporal.api.failure.v1.Failure.newBuilder()
      .setMessage(message)
    if (cause != null) {
      builder.setStackTrace(cause.stackTraceToString())
    }
    return builder.build()
  }

  override fun eventLoop(): Boolean {
    if (workflowCompleted.get()) {
      return true
    }

    val disp = dispatcher ?: return true

    try {
      disp.runUntilAllBlocked(deadlockDetectionTimeoutMs)
    } catch (e: WorkflowDeadlockException) {
      workflowContext?.failWorkflowTask(e)
      return false
    }

    return workflowCompleted.get()
  }

  override fun getOutput(): Optional<Payloads> {
    val exception = workflowException.get()
    if (exception != null) {
      throw exception
    }
    return workflowOutput.get()
  }

  override fun cancel(reason: String?) {
    if (cancelled.compareAndSet(false, true)) {
      coroutineScope?.cancel(CancellationException(reason ?: "Workflow cancelled"))
    }
  }

  override fun close() {
    dispatcher?.close()
    coroutineScope?.cancel()
  }

  override fun query(query: WorkflowQuery): Optional<Payloads> {
    val queryName = query.queryType
    val interceptor = inboundInterceptor
      ?: throw IllegalStateException("Inbound interceptor not initialized")

    val input = if (query.hasQueryArgs()) {
      Optional.of(query.queryArgs)
    } else {
      Optional.empty()
    }

    // Create encoded values for dynamic handlers
    val ctx = workflowContext
      ?: throw IllegalStateException("Workflow context not initialized")
    val encodedValues = ctx.createEncodedValues(input)

    // Create query input for interceptor
    val headerMap = io.temporal.common.interceptors.Header.empty()
    val queryInput = KQueryInput(
      queryName = queryName,
      arguments = emptyArray(),
      encodedValues = encodedValues,
      header = headerMap
    )

    // Execute through interceptor chain
    val output = interceptor.handleQuery(queryInput)

    // Serialize the result
    return if (output.result != null && output.result != Unit) {
      Optional.of(dataConverter.toPayloads(output.result).orElse(Payloads.getDefaultInstance()))
    } else {
      Optional.empty()
    }
  }

  override fun getWorkflowContext(): WorkflowContext {
    return object : WorkflowContext {
      override fun getReplayContext(): ReplayWorkflowContext = replayContext!!

      override fun mapWorkflowExceptionToFailure(exception: Throwable): io.temporal.api.failure.v1.Failure {
        return createFailure(exception.message ?: "Workflow exception", exception)
      }

      override fun getWorkflowImplementationOptions(): io.temporal.worker.WorkflowImplementationOptions {
        return io.temporal.worker.WorkflowImplementationOptions.getDefaultInstance()
      }

      override fun <R : Any?> getLastCompletionResult(resultClass: Class<R>?, resultType: java.lang.reflect.Type?): R? {
        return null
      }

      override fun getContextPropagators(): List<io.temporal.common.context.ContextPropagator> {
        return emptyList()
      }

      override fun getPropagatedContexts(): Map<String, Any> {
        return emptyMap()
      }

      override fun getRunningSignalHandlers(): Map<Long, io.temporal.internal.sync.SignalHandlerInfo> {
        return emptyMap()
      }

      override fun getRunningUpdateHandlers(): Map<String, io.temporal.internal.sync.UpdateHandlerInfo> {
        return emptyMap()
      }

      override fun getVersioningBehavior(): io.temporal.common.VersioningBehavior {
        return io.temporal.common.VersioningBehavior.UNSPECIFIED
      }
    }
  }

  /**
   * Inner class that implements WorkflowExecutor for dynamic workflow execution.
   */
  private inner class DynamicWorkflowExecutor : WorkflowExecutor {
    private var outboundInterceptor: KWorkflowOutboundCallsInterceptor? = null

    override fun setOutboundInterceptor(outboundCalls: KWorkflowOutboundCallsInterceptor) {
      this.outboundInterceptor = outboundCalls
      workflowContext?.outboundInterceptor = outboundCalls
    }

    override suspend fun executeWorkflow(input: KWorkflowInput): KWorkflowOutput {
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")

      // Create KEncodedValues from the input payloads
      val kEncodedValues = workflowContext!!.createEncodedValues(inputPayloads)

      // Call the suspend execute method
      val result = instance.execute(kEncodedValues)

      return KWorkflowOutput(result = result)
    }

    override suspend fun handleSignal(input: KSignalInput) {
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // Check for dynamically registered signal handler
      val dynamicHandler = ctx.signalHandlers[input.signalName]
      if (dynamicHandler != null) {
        val encodedValues = input.encodedValues
        dynamicHandler(encodedValues)
        return
      }

      // Check for catch-all dynamic signal handler
      val catchAllHandler = ctx.dynamicSignalHandler
      if (catchAllHandler != null) {
        val encodedValues = input.encodedValues
        catchAllHandler(input.signalName, encodedValues)
        return
      }

      // No handler registered yet - buffer the signal for later replay
      // when a dynamic handler is registered
      ctx.bufferedSignals.add(Pair(input.signalName, input.encodedValues))
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // Check for dynamically registered query handler
      val dynamicHandler = ctx.queryHandlers[input.queryName]
      if (dynamicHandler != null) {
        val encodedValues = input.encodedValues
        val result = dynamicHandler(encodedValues)
        return KQueryOutput(result = result)
      }

      // Check for catch-all dynamic query handler
      val catchAllHandler = ctx.dynamicQueryHandler
      if (catchAllHandler != null) {
        val encodedValues = input.encodedValues
        val result = catchAllHandler(input.queryName, encodedValues)
        return KQueryOutput(result = result)
      }

      throw IllegalArgumentException("Unknown query: ${input.queryName}")
    }

    override fun validateUpdate(input: KUpdateInput) {
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // Check for dynamically registered validator (by update name)
      val dynamicValidator = ctx.updateValidators[input.updateName]
      if (dynamicValidator != null) {
        val encodedValues = input.encodedValues
        dynamicValidator(encodedValues)
        return
      }

      // Check for catch-all dynamic validator
      val catchAllValidator = ctx.dynamicUpdateValidator
      if (catchAllValidator != null) {
        val encodedValues = input.encodedValues
        catchAllValidator(input.updateName, encodedValues)
        return
      }

      // No validator = validation passes
    }

    override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // Check for dynamically registered update handler
      val dynamicHandler = ctx.updateHandlers[input.updateName]
      if (dynamicHandler != null) {
        val encodedValues = input.encodedValues
        val result = dynamicHandler(encodedValues)
        return KUpdateOutput(result = result)
      }

      // Check for catch-all dynamic update handler
      val catchAllHandler = ctx.dynamicUpdateHandler
      if (catchAllHandler != null) {
        val encodedValues = input.encodedValues
        val result = catchAllHandler(input.updateName, encodedValues)
        return KUpdateOutput(result = result)
      }

      throw IllegalArgumentException("Unknown update: ${input.updateName}")
    }
  }
}
