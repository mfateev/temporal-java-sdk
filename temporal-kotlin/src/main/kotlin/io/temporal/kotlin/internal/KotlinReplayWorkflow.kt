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
import io.temporal.kotlin.interceptor.KActivityInvocationInput
import io.temporal.kotlin.interceptor.KCancelWorkflowInput
import io.temporal.kotlin.interceptor.KChildWorkflowInvocationInput
import io.temporal.kotlin.interceptor.KContinueAsNewInput
import io.temporal.kotlin.interceptor.KLocalActivityInvocationInput
import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalExternalInput
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
import io.temporal.kotlin.internal.interceptor.WorkflowExecutor
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
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend

/**
 * Implementation of [ReplayWorkflow] using Kotlin coroutines.
 *
 * This class manages the execution of a Kotlin suspend-based workflow,
 * integrating with Temporal's replay mechanism through a custom coroutine
 * dispatcher that ensures deterministic execution.
 */
@InternalTemporalApi
internal class KotlinReplayWorkflow(
  private val workflowDefinition: KotlinWorkflowDefinition,
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

  private val workflowInstance = AtomicReference<Any?>(null)
  private val workflowOutput = AtomicReference<Optional<Payloads>>(Optional.empty())
  private val workflowException = AtomicReference<Throwable?>(null)
  private val workflowCompleted = AtomicBoolean(false)
  private val cancelled = AtomicBoolean(false)

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

    this.coroutineScope = CoroutineScope(
      dispatcher!! + SupervisorJob() + exceptionHandler
    )

    // Set coroutine scope reference for async operations
    this.workflowContext!!.coroutineScope = this.coroutineScope

    // Create workflow instance
    val instance = workflowDefinition.createInstance()
    workflowInstance.set(instance)

    // Build interceptor chain
    val executor = DefaultWorkflowExecutor()
    val rootInterceptor = RootWorkflowInboundCallsInterceptor(executor)
    inboundInterceptor = InterceptorChain.buildWorkflowInboundChain(workerInterceptors, rootInterceptor)

    // Extract input from the start event
    val startedAttributes = event.workflowExecutionStartedEventAttributes
    val input = if (startedAttributes.hasInput()) {
      Optional.of(startedAttributes.input)
    } else {
      Optional.empty()
    }

    // Extract header from the start event
    val startHeader = startedAttributes.header
    val headerMap = io.temporal.common.interceptors.Header(startHeader.fieldsMap)

    // Convert input payloads to arguments array
    val inputArgs = if (input.isPresent) {
      val payloads = input.get()
      val method = workflowDefinition.workflowMethod
      val parameters = method.parameters
      if (parameters.size > 1) {
        val paramTypes = parameters.drop(1).map { param ->
          val classifier = param.type.classifier
          when (classifier) {
            is KClass<*> -> classifier.java
            is Class<*> -> classifier
            else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
          }
        }
        deserializeArguments(payloads, paramTypes)
      } else {
        emptyArray()
      }
    } else {
      emptyArray()
    }

    // Create the workflow input for interceptors
    val workflowInput = KWorkflowInput(
      header = headerMap,
      arguments = inputArgs
    )

    // Launch the workflow coroutine
    workflowJob = coroutineScope!!.launch {
      try {
        // Execute through the interceptor chain
        val interceptor = inboundInterceptor!!

        // Initialize the interceptor chain (outbound interceptor can be added later)
        interceptor.init(NoOpWorkflowOutboundCallsInterceptor())

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

  private suspend fun executeWorkflowMethod(
    instance: Any,
    input: Optional<Payloads>
  ): Optional<Payloads> {
    val method = workflowDefinition.workflowMethod
    val parameters = method.parameters

    // Deserialize input arguments
    val args = if (input.isPresent && parameters.size > 1) {
      // First parameter is 'this' for instance methods
      // Convert KClass to Java Class for deserialization
      val paramTypes = parameters.drop(1).map { param ->
        val classifier = param.type.classifier
        when (classifier) {
          is KClass<*> -> classifier.java
          is Class<*> -> classifier
          else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
        }
      }
      deserializeArguments(input.get(), paramTypes)
    } else {
      emptyArray()
    }

    // Call the workflow method
    val result = if (workflowDefinition.isSuspendFunction) {
      method.callSuspend(instance, *args)
    } else {
      method.call(instance, *args)
    }

    // Serialize the result
    return if (result != null && result != Unit) {
      Optional.of(dataConverter.toPayloads(result).orElse(Payloads.getDefaultInstance()))
    } else {
      Optional.empty()
    }
  }

  private fun deserializeArguments(payloads: Payloads, types: List<Class<*>>): Array<Any?> {
    return types.mapIndexed { index, type ->
      if (index < payloads.payloadsCount) {
        dataConverter.fromPayload(payloads.getPayloads(index), type, type)
      } else {
        null
      }
    }.toTypedArray()
  }

  override fun handleSignal(
    signalName: String,
    input: Optional<Payloads>,
    eventId: Long,
    header: Header
  ) {
    val instance = workflowInstance.get() ?: return
    val ctx = workflowContext ?: return

    // First check for annotation-based signal handler
    val signalMethod = workflowDefinition.signalMethods[signalName]
    if (signalMethod != null) {
      // Execute annotation-based signal handler
      dispatcher?.executeImmediately {
        coroutineScope?.launch {
          ctx.runningSignalHandlers.incrementAndGet()
          try {
            val parameters = signalMethod.parameters
            val args = if (input.isPresent && parameters.size > 1) {
              val paramTypes = parameters.drop(1).map { param ->
                val classifier = param.type.classifier
                when (classifier) {
                  is KClass<*> -> classifier.java
                  is Class<*> -> classifier
                  else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
                }
              }
              deserializeArguments(input.get(), paramTypes)
            } else {
              emptyArray()
            }

            if (signalMethod.isSuspend) {
              signalMethod.callSuspend(instance, *args)
            } else {
              signalMethod.call(instance, *args)
            }
          } catch (e: Throwable) {
            workflowContext?.failWorkflowTask(e)
          } finally {
            ctx.runningSignalHandlers.decrementAndGet()
          }
        }
      }
      return
    }

    // Check for dynamically registered signal handler
    val dynamicHandler = ctx.signalHandlers[signalName]
    if (dynamicHandler != null) {
      dispatcher?.executeImmediately {
        coroutineScope?.launch {
          ctx.runningSignalHandlers.incrementAndGet()
          try {
            val encodedValues = ctx.createEncodedValues(input)
            dynamicHandler(encodedValues)
          } catch (e: Throwable) {
            workflowContext?.failWorkflowTask(e)
          } finally {
            ctx.runningSignalHandlers.decrementAndGet()
          }
        }
      }
      return
    }

    // Check for catch-all dynamic signal handler
    val catchAllHandler = ctx.dynamicSignalHandler
    if (catchAllHandler != null) {
      dispatcher?.executeImmediately {
        coroutineScope?.launch {
          ctx.runningSignalHandlers.incrementAndGet()
          try {
            val encodedValues = ctx.createEncodedValues(input)
            catchAllHandler(signalName, encodedValues)
          } catch (e: Throwable) {
            workflowContext?.failWorkflowTask(e)
          } finally {
            ctx.runningSignalHandlers.decrementAndGet()
          }
        }
      }
      return
    }

    // Unknown signal with no handler - ignore (could log warning)
  }

  override fun handleUpdate(
    updateName: String,
    updateId: String,
    input: Optional<Payloads>,
    eventId: Long,
    header: Header,
    callbacks: UpdateProtocolCallback
  ) {
    val instance = workflowInstance.get()
    if (instance == null) {
      callbacks.reject(createFailure("Workflow instance not initialized"))
      return
    }

    val ctx = workflowContext
    if (ctx == null) {
      callbacks.reject(createFailure("Workflow context not initialized"))
      return
    }

    // First check for annotation-based update handler
    val updateMethod = workflowDefinition.updateMethods[updateName]
    if (updateMethod != null) {
      handleAnnotationBasedUpdate(updateName, updateId, input, callbacks, instance, ctx, updateMethod)
      return
    }

    // Check for dynamically registered named update handler
    val namedHandler = ctx.updateHandlers[updateName]
    if (namedHandler != null) {
      handleDynamicNamedUpdate(updateName, updateId, input, callbacks, ctx, namedHandler)
      return
    }

    // Check for catch-all dynamic update handler
    val dynamicHandler = ctx.dynamicUpdateHandler
    if (dynamicHandler != null) {
      handleDynamicFallbackUpdate(updateName, updateId, input, callbacks, ctx, dynamicHandler)
      return
    }

    // No handler found
    callbacks.reject(createFailure("Unknown update: $updateName"))
  }

  private fun handleAnnotationBasedUpdate(
    updateName: String,
    updateId: String,
    input: Optional<Payloads>,
    callbacks: UpdateProtocolCallback,
    instance: Any,
    ctx: KotlinWorkflowContext,
    updateMethod: KFunction<*>
  ) {
    // Deserialize arguments synchronously (needed for validation)
    val parameters = updateMethod.parameters
    val args = if (input.isPresent && parameters.size > 1) {
      val paramTypes = parameters.drop(1).map { param ->
        val classifier = param.type.classifier
        when (classifier) {
          is KClass<*> -> classifier.java
          is Class<*> -> classifier
          else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
        }
      }
      deserializeArguments(input.get(), paramTypes)
    } else {
      emptyArray()
    }

    // Execute update handler in the workflow context
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        ctx.runningUpdateHandlers.incrementAndGet()
        ctx.currentUpdateInfo.set(KUpdateInfo(updateName, updateId))
        try {
          // Accept the update - must happen before handler runs
          callbacks.accept()

          // Execute the update
          val result = if (updateMethod.isSuspend) {
            updateMethod.callSuspend(instance, *args)
          } else {
            updateMethod.call(instance, *args)
          }

          // Complete with result
          val resultPayloads = if (result != null && result != Unit) {
            dataConverter.toPayloads(result)
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

  private fun handleDynamicNamedUpdate(
    updateName: String,
    updateId: String,
    input: Optional<Payloads>,
    callbacks: UpdateProtocolCallback,
    ctx: KotlinWorkflowContext,
    handler: UpdateHandler
  ) {
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        ctx.runningUpdateHandlers.incrementAndGet()
        ctx.currentUpdateInfo.set(KUpdateInfo(updateName, updateId))
        try {
          // Run validator if registered
          val validator = ctx.updateValidators[updateName]
          if (validator != null) {
            val encodedValues = ctx.createEncodedValues(input)
            validator(encodedValues)
          }

          // Accept the update - must happen before handler runs
          callbacks.accept()

          // Execute the handler
          val encodedValues = ctx.createEncodedValues(input)
          val result = handler(encodedValues)

          // Complete with result
          val resultPayloads = if (result != null && result != Unit) {
            dataConverter.toPayloads(result)
          } else {
            Optional.empty()
          }
          callbacks.complete(resultPayloads, null)
        } catch (e: Throwable) {
          // If validation failed, reject; otherwise complete with failure
          if (ctx.updateValidators[updateName] != null) {
            callbacks.reject(createFailure(e.message ?: "Update validation failed", e))
          } else {
            callbacks.complete(Optional.empty(), createFailure(e.message ?: "Update failed", e))
          }
        } finally {
          ctx.currentUpdateInfo.set(null)
          ctx.runningUpdateHandlers.decrementAndGet()
        }
      }
    }
  }

  private fun handleDynamicFallbackUpdate(
    updateName: String,
    updateId: String,
    input: Optional<Payloads>,
    callbacks: UpdateProtocolCallback,
    ctx: KotlinWorkflowContext,
    handler: DynamicUpdateHandler
  ) {
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        ctx.runningUpdateHandlers.incrementAndGet()
        ctx.currentUpdateInfo.set(KUpdateInfo(updateName, updateId))
        try {
          // Run dynamic validator if registered
          val validator = ctx.dynamicUpdateValidator
          if (validator != null) {
            val encodedValues = ctx.createEncodedValues(input)
            validator(updateName, encodedValues)
          }

          // Accept the update - must happen before handler runs
          callbacks.accept()

          // Execute the handler
          val encodedValues = ctx.createEncodedValues(input)
          val result = handler(updateName, encodedValues)

          // Complete with result
          val resultPayloads = if (result != null && result != Unit) {
            dataConverter.toPayloads(result)
          } else {
            Optional.empty()
          }
          callbacks.complete(resultPayloads, null)
        } catch (e: Throwable) {
          // If validation failed, reject; otherwise complete with failure
          if (ctx.dynamicUpdateValidator != null) {
            callbacks.reject(createFailure(e.message ?: "Update validation failed", e))
          } else {
            callbacks.complete(Optional.empty(), createFailure(e.message ?: "Update failed", e))
          }
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

    // Only return true when the workflow has actually completed.
    // When suspended waiting for activities/timers, we return false
    // to indicate more work is expected after external events.
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
    val ctx = workflowContext
      ?: throw IllegalStateException("Workflow context not initialized")

    val input = if (query.hasQueryArgs()) {
      Optional.of(query.queryArgs)
    } else {
      Optional.empty()
    }

    // First check for annotation-based query handler
    val queryMethod = workflowDefinition.queryMethods[queryName]
    if (queryMethod != null) {
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")

      val parameters = queryMethod.parameters
      val args = if (input.isPresent && parameters.size > 1) {
        deserializeArguments(input.get(), parameters.drop(1).map { it.type.classifier as Class<*> })
      } else {
        emptyArray()
      }

      // Query methods should not be suspend functions
      val result = queryMethod.call(instance, *args)

      return if (result != null && result != Unit) {
        Optional.of(dataConverter.toPayloads(result).orElse(Payloads.getDefaultInstance()))
      } else {
        Optional.empty()
      }
    }

    // Check for dynamically registered query handler
    val dynamicHandler = ctx.queryHandlers[queryName]
    if (dynamicHandler != null) {
      val encodedValues = ctx.createEncodedValues(input)
      val result = dynamicHandler(encodedValues)

      return if (result != null && result != Unit) {
        Optional.of(dataConverter.toPayloads(result).orElse(Payloads.getDefaultInstance()))
      } else {
        Optional.empty()
      }
    }

    // Check for catch-all dynamic query handler
    val catchAllHandler = ctx.dynamicQueryHandler
    if (catchAllHandler != null) {
      val encodedValues = ctx.createEncodedValues(input)
      val result = catchAllHandler(queryName, encodedValues)

      return if (result != null && result != Unit) {
        Optional.of(dataConverter.toPayloads(result).orElse(Payloads.getDefaultInstance()))
      } else {
        Optional.empty()
      }
    }

    throw IllegalArgumentException("Unknown query: $queryName")
  }

  override fun getWorkflowContext(): WorkflowContext {
    // Return a minimal WorkflowContext implementation
    // This is needed for the ReplayWorkflow interface
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
   * Inner class that implements WorkflowExecutor to provide actual workflow execution.
   * This is used as the root of the interceptor chain.
   */
  private inner class DefaultWorkflowExecutor : WorkflowExecutor {
    private var outboundInterceptor: KWorkflowOutboundCallsInterceptor? = null

    override fun setOutboundInterceptor(outboundCalls: KWorkflowOutboundCallsInterceptor) {
      this.outboundInterceptor = outboundCalls
    }

    override suspend fun executeWorkflow(input: KWorkflowInput): KWorkflowOutput {
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")

      val payloadsInput = if (input.arguments.isNotEmpty()) {
        Optional.of(
          Payloads.newBuilder()
            .addAllPayloads(input.arguments.map { dataConverter.toPayloads(it).orElse(Payloads.getDefaultInstance()).getPayloads(0) })
            .build()
        )
      } else {
        Optional.empty()
      }

      val result = executeWorkflowMethod(instance, payloadsInput)
      return KWorkflowOutput(
        result = if (result.isPresent) {
          dataConverter.fromPayloads(0, Optional.of(result.get()), Any::class.java, Any::class.java)
        } else {
          null
        }
      )
    }

    override suspend fun handleSignal(input: KSignalInput) {
      // TODO: Implement signal handling through interceptor
      // For now, signals are handled directly in handleSignal override
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      // TODO: Implement query handling through interceptor
      // For now, queries are handled directly in query override
      throw UnsupportedOperationException("Query handling through interceptor not yet implemented")
    }

    override fun validateUpdate(input: KUpdateInput) {
      // TODO: Implement update validation through interceptor
    }

    override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
      // TODO: Implement update execution through interceptor
      throw UnsupportedOperationException("Update handling through interceptor not yet implemented")
    }
  }
}

/**
 * No-op implementation of KWorkflowOutboundCallsInterceptor for initialization.
 * TODO: Implement proper outbound interceptor chain when outbound operations are supported.
 */
private class NoOpWorkflowOutboundCallsInterceptor : KWorkflowOutboundCallsInterceptor {
  override suspend fun <R> executeActivity(input: KActivityInvocationInput<R>): R {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun <R> executeLocalActivity(input: KLocalActivityInvocationInput<R>): R {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun <T, R> startChildWorkflow(
    input: KChildWorkflowInvocationInput<R>
  ): io.temporal.kotlin.workflow.KChildWorkflowHandle<T, R> {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun delay(duration: kotlin.time.Duration) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun awaitCondition(
    timeout: kotlin.time.Duration,
    reason: String,
    condition: () -> Boolean
  ): Boolean {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun awaitCondition(reason: String, condition: () -> Boolean) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun <R> mutableSideEffect(
    id: String,
    resultClass: Class<R>,
    updated: (R?, R?) -> Boolean,
    func: () -> R
  ): R {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun continueAsNew(input: KContinueAsNewInput): Nothing {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun signalExternalWorkflow(input: KSignalExternalInput) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override suspend fun cancelWorkflow(input: KCancelWorkflowInput) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun upsertTypedSearchAttributes(vararg updates: io.temporal.common.SearchAttributeUpdate<*>) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun upsertMemo(memo: Map<String, Any>) {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun newRandom(): java.util.Random {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun randomUUID(): java.util.UUID {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }

  override fun currentTimeMillis(): Long {
    throw UnsupportedOperationException("Outbound interceptor not yet wired")
  }
}
