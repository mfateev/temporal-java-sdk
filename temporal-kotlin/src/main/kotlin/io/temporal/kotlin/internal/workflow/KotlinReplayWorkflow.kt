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

package io.temporal.kotlin.internal.workflow

import io.temporal.api.common.v1.Header
import io.temporal.api.common.v1.Payloads
import io.temporal.api.history.v1.HistoryEvent
import io.temporal.api.query.v1.WorkflowQuery
import io.temporal.common.converter.DataConverter
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.internal.replay.WorkflowContext
import io.temporal.internal.statemachines.UpdateProtocolCallback
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
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.interceptor.InterceptorChain
import io.temporal.kotlin.internal.interceptor.RootWorkflowInboundCallsInterceptor
import io.temporal.kotlin.internal.interceptor.RootWorkflowOutboundCallsInterceptor
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
import kotlin.reflect.full.callSuspend
import kotlin.reflect.full.declaredFunctions

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

    // WorkflowContextElement ensures the workflow context ThreadLocal is properly
    // set when coroutines run, including nested async blocks
    val contextElement = WorkflowContextElement(workflowContext!!)

    this.coroutineScope = CoroutineScope(
      dispatcher!! + SupervisorJob() + exceptionHandler + contextElement
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
    val ctx = workflowContext ?: return
    val interceptor = inboundInterceptor ?: return

    // Deserialize signal arguments
    val args = deserializeSignalArgs(signalName, input)

    // Create encoded values for dynamic handlers
    val encodedValues = ctx.createEncodedValues(input)

    // Create signal input for interceptor
    val headerMap = io.temporal.common.interceptors.Header(header.fieldsMap)
    val signalInput = KSignalInput(
      signalName = signalName,
      arguments = args,
      encodedValues = encodedValues,
      eventId = eventId,
      header = headerMap
    )

    // Execute through interceptor chain
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

  private fun deserializeSignalArgs(signalName: String, input: Optional<Payloads>): Array<Any?> {
    if (!input.isPresent) return emptyArray()

    // Check for annotation-based signal handler to get parameter types
    val signalMethod = workflowDefinition.signalMethods[signalName]
    if (signalMethod != null) {
      val parameters = signalMethod.parameters
      if (parameters.size > 1) {
        val paramTypes = parameters.drop(1).map { param ->
          val classifier = param.type.classifier
          when (classifier) {
            is KClass<*> -> classifier.java
            is Class<*> -> classifier
            else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
          }
        }
        return deserializeArguments(input.get(), paramTypes)
      }
    }

    // For dynamic handlers, we can't deserialize without knowing the types
    // Return empty and let the handler use encoded values
    return emptyArray()
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

    // Deserialize arguments
    val args = deserializeUpdateArgs(updateName, input)

    // Create encoded values for dynamic handlers
    val encodedValues = ctx.createEncodedValues(input)

    // Create update input for interceptor
    val headerMap = io.temporal.common.interceptors.Header(header.fieldsMap)
    val updateInput = KUpdateInput(
      updateName = updateName,
      arguments = args,
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
          // Include exception type in message for debugging
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

  private fun deserializeUpdateArgs(updateName: String, input: Optional<Payloads>): Array<Any?> {
    if (!input.isPresent) return emptyArray()

    // Check for annotation-based update handler to get parameter types
    val updateMethod = workflowDefinition.updateMethods[updateName]
    if (updateMethod != null) {
      val parameters = updateMethod.parameters
      if (parameters.size > 1) {
        val paramTypes = parameters.drop(1).map { param ->
          val classifier = param.type.classifier
          when (classifier) {
            is KClass<*> -> classifier.java
            is Class<*> -> classifier
            else -> throw IllegalArgumentException("Unsupported parameter type: $classifier")
          }
        }
        return deserializeArguments(input.get(), paramTypes)
      }
    }

    // For dynamic handlers, return empty (they use encoded values)
    return emptyArray()
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
    val interceptor = inboundInterceptor
      ?: throw IllegalStateException("Inbound interceptor not initialized")

    val input = if (query.hasQueryArgs()) {
      Optional.of(query.queryArgs)
    } else {
      Optional.empty()
    }

    // Deserialize query arguments
    val args = deserializeQueryArgs(queryName, input)

    // Create encoded values for dynamic handlers
    val ctx = workflowContext
      ?: throw IllegalStateException("Workflow context not initialized")
    val encodedValues = ctx.createEncodedValues(input)

    // Create query input for interceptor
    val headerMap = io.temporal.common.interceptors.Header.empty()
    val queryInput = KQueryInput(
      queryName = queryName,
      arguments = args,
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

  private fun deserializeQueryArgs(queryName: String, input: Optional<Payloads>): Array<Any?> {
    if (!input.isPresent) return emptyArray()

    // Check for annotation-based query handler to get parameter types
    val queryMethod = workflowDefinition.queryMethods[queryName]
    if (queryMethod != null) {
      val parameters = queryMethod.parameters
      if (parameters.size > 1) {
        return deserializeArguments(input.get(), parameters.drop(1).map { it.type.classifier as Class<*> })
      }
    }

    // For dynamic handlers, return empty (they use encoded values)
    return emptyArray()
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
      // Also set on context so KWorkflow static methods can route through the interceptor chain
      workflowContext?.outboundInterceptor = outboundCalls
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
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // First check for annotation-based signal handler
      val signalMethod = workflowDefinition.signalMethods[input.signalName]
      if (signalMethod != null) {
        if (signalMethod.isSuspend) {
          signalMethod.callSuspend(instance, *input.arguments)
        } else {
          signalMethod.call(instance, *input.arguments)
        }
        return
      }

      // Check for dynamically registered signal handler
      val dynamicHandler = ctx.signalHandlers[input.signalName]
      if (dynamicHandler != null) {
        // For dynamic handlers, create encoded values from the arguments
        // Note: arguments may be empty if types couldn't be determined at deserialization
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

      // Unknown signal with no handler - ignore
    }

    override fun handleQuery(input: KQueryInput): KQueryOutput {
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // First check for annotation-based query handler
      val queryMethod = workflowDefinition.queryMethods[input.queryName]
      if (queryMethod != null) {
        // Query methods should not be suspend functions
        val result = queryMethod.call(instance, *input.arguments)
        return KQueryOutput(result = result)
      }

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
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // First check for annotation-based validator method
      val interfaceValidatorMethod = workflowDefinition.updateValidatorMethods[input.updateName]
      if (interfaceValidatorMethod != null) {
        // Get the actual implementation method (interface method is abstract)
        val implValidatorMethod = workflowDefinition.workflowImplementationClass.declaredFunctions
          .find { it.name == interfaceValidatorMethod.name }
          ?: throw IllegalStateException(
            "Validator method ${interfaceValidatorMethod.name} not found in implementation class"
          )
        // Validators must NOT be suspend functions
        try {
          implValidatorMethod.call(instance, *input.arguments)
        } catch (e: java.lang.reflect.InvocationTargetException) {
          // Unwrap the exception thrown by the validator
          throw e.targetException ?: e
        }
        return
      }

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
      val instance = workflowInstance.get()
        ?: throw IllegalStateException("Workflow instance not initialized")
      val ctx = workflowContext
        ?: throw IllegalStateException("Workflow context not initialized")

      // First check for annotation-based update handler
      val updateMethod = workflowDefinition.updateMethods[input.updateName]
      if (updateMethod != null) {
        val result = if (updateMethod.isSuspend) {
          updateMethod.callSuspend(instance, *input.arguments)
        } else {
          updateMethod.call(instance, *input.arguments)
        }
        return KUpdateOutput(result = result)
      }

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
