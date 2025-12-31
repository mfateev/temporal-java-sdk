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

import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes
import io.temporal.api.common.v1.Payloads
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.workflow.KChildWorkflowHandle
import io.temporal.workflow.SignalMethod
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.Optional
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.jvm.javaMethod

/**
 * Internal implementation of [KChildWorkflowHandle].
 */
@InternalTemporalApi
internal class KChildWorkflowHandleImpl<T, R>(
  override val workflowId: String,
  override val firstExecutionRunId: String,
  private val resultClass: Class<R>,
  private val context: KotlinWorkflowContext,
  private val dataConverter: DataConverter,
  private val resultProvider: suspend () -> Optional<Payloads>
) : KChildWorkflowHandle<T, R> {

  override suspend fun result(): R {
    val payloads = resultProvider()
    return deserializeResult(payloads)
  }

  override suspend fun signal(signal: KFunction1<T, Unit>) {
    val signalName = extractSignalName(signal)
    signal(signalName)
  }

  override suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A) {
    val signalName = extractSignalName(signal)
    signal(signalName, arg)
  }

  override suspend fun signal(signalName: String, vararg args: Any?) {
    val input = if (args.isEmpty()) {
      Optional.empty()
    } else {
      dataConverter.toPayloads(*args)
    }

    val execution = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .setRunId(firstExecutionRunId)
      .build()

    val attributes = SignalExternalWorkflowExecutionCommandAttributes.newBuilder()
      .setSignalName(signalName)
      .setExecution(execution)
    input.ifPresent { attributes.setInput(it) }

    suspendCancellableCoroutine<Unit> { cont ->
      context.replayContext.signalExternalWorkflowExecution(
        attributes
      ) { _, failure ->
        if (failure != null) {
          cont.resumeWithException(RuntimeException(failure.message))
        } else {
          cont.resume(Unit)
        }
      }
    }
  }

  override suspend fun cancel() {
    val execution = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .setRunId(firstExecutionRunId)
      .build()

    suspendCancellableCoroutine<Unit> { cont ->
      context.replayContext.requestCancelExternalWorkflowExecution(
        execution,
        null // reason
      ) { _, exception ->
        if (exception != null) {
          cont.resumeWithException(exception)
        } else {
          cont.resume(Unit)
        }
      }
    }
  }

  private fun extractSignalName(signal: KFunction<*>): String {
    val javaMethod = signal.javaMethod
      ?: throw IllegalArgumentException("Cannot resolve signal method reference")

    val signalMethod = javaMethod.getAnnotation(SignalMethod::class.java)
    return if (signalMethod != null && signalMethod.name.isNotEmpty()) {
      signalMethod.name
    } else {
      javaMethod.name
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun deserializeResult(payloads: Optional<Payloads>): R {
    return if (payloads.isPresent && resultClass != Unit::class.java && resultClass != Void.TYPE) {
      dataConverter.fromPayload(payloads.get().getPayloads(0), resultClass, resultClass) as R
    } else {
      null as R
    }
  }
}
