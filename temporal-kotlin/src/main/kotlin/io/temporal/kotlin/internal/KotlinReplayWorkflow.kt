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
  private val deadlockDetectionTimeoutMs: Long
) : ReplayWorkflow {

  private var workflowContext: KotlinWorkflowContext? = null
  private var replayContext: ReplayWorkflowContext? = null
  private var dispatcher: KotlinCoroutineDispatcher? = null
  private var coroutineScope: CoroutineScope? = null
  private var workflowJob: Job? = null

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

    // Extract input from the start event
    val startedAttributes = event.workflowExecutionStartedEventAttributes
    val input = if (startedAttributes.hasInput()) {
      Optional.of(startedAttributes.input)
    } else {
      Optional.empty()
    }

    // Launch the workflow coroutine
    workflowJob = coroutineScope!!.launch {
      try {
        val result = executeWorkflowMethod(instance, input)
        workflowOutput.set(result)
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
          try {
            val encodedValues = ctx.createEncodedValues(input)
            dynamicHandler(encodedValues)
          } catch (e: Throwable) {
            workflowContext?.failWorkflowTask(e)
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
          try {
            val encodedValues = ctx.createEncodedValues(input)
            catchAllHandler(signalName, encodedValues)
          } catch (e: Throwable) {
            workflowContext?.failWorkflowTask(e)
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
    val updateMethod = workflowDefinition.updateMethods[updateName]
    if (updateMethod == null) {
      callbacks.reject(createFailure("Unknown update: $updateName"))
      return
    }

    val instance = workflowInstance.get()
    if (instance == null) {
      callbacks.reject(createFailure("Workflow instance not initialized"))
      return
    }

    // Execute update handler in the workflow context
    dispatcher?.executeImmediately {
      coroutineScope?.launch {
        try {
          val parameters = updateMethod.parameters
          val args = if (input.isPresent && parameters.size > 1) {
            deserializeArguments(input.get(), parameters.drop(1).map { it.type.classifier as Class<*> })
          } else {
            emptyArray()
          }

          // Accept the update (validation passed)
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
}
