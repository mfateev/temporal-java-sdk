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

package io.temporal.kotlin.workflow

import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes
import io.temporal.api.common.v1.Payloads
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.common.KArgs2
import io.temporal.kotlin.common.KArgs3
import io.temporal.kotlin.common.KArgs4
import io.temporal.kotlin.common.KArgs5
import io.temporal.kotlin.common.KArgs6
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.workflow.SignalMethod
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.Optional
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3
import kotlin.reflect.KFunction4
import kotlin.reflect.KFunction5
import kotlin.reflect.KFunction6
import kotlin.reflect.KFunction7
import kotlin.reflect.jvm.javaMethod

/**
 * Handle to a child workflow execution for sending signals and awaiting results.
 *
 * Use [KWorkflow.startChildWorkflow] to obtain a handle, or [KWorkflow.getChildWorkflowHandle]
 * to get a handle to an existing child workflow.
 *
 * Example:
 * ```kotlin
 * val handle = KWorkflow.startChildWorkflow(
 *   ChildWorkflow::processOrder,
 *   ChildWorkflowOptions { workflowId = "child-123" },
 *   order
 * )
 *
 * // Send a signal to the child
 * handle.signal(ChildWorkflow::updatePriority, Priority.HIGH)
 *
 * // Wait for the result
 * val result = handle.result()
 * ```
 *
 * @param T the child workflow interface type
 * @param R the result type of the child workflow
 */
public class KChildWorkflowHandle<T, R> @InternalTemporalApi internal constructor(
  /**
   * The workflow ID of the child workflow.
   */
  public val workflowId: String,

  /**
   * The run ID of the first execution of this child workflow.
   * This remains constant across continue-as-new.
   */
  public val firstExecutionRunId: String,

  @PublishedApi internal val resultClass: Class<R>,
  @PublishedApi internal val context: KotlinWorkflowContext,
  @PublishedApi internal val dataConverter: DataConverter,
  @PublishedApi internal val resultProvider: suspend () -> Optional<Payloads>
) {

  /**
   * Waits for the child workflow to complete and returns the result.
   *
   * @return the result of the child workflow
   * @throws ChildWorkflowException if the child workflow fails
   */
  public suspend fun result(): R {
    val payloads = resultProvider()
    return deserializeResult(payloads)
  }

  /**
   * Sends a signal to the child workflow using a method reference.
   *
   * Example:
   * ```kotlin
   * handle.signal(ChildWorkflow::updateStatus)
   * ```
   *
   * @param signal the signal method reference
   */
  public suspend fun signal(signal: KFunction1<T, Unit>) {
    val signalName = extractSignalName(signal)
    signal(signalName)
  }

  /**
   * Sends a signal with one argument to the child workflow.
   *
   * @param signal the signal method reference
   * @param arg the signal argument
   */
  public suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A) {
    val signalName = extractSignalName(signal)
    signal(signalName, arg)
  }

  /**
   * Sends a signal with two arguments to the child workflow.
   */
  public suspend fun <A1, A2> signal(signal: KFunction3<T, A1, A2, Unit>, args: KArgs2<A1, A2>) {
    val signalName = extractSignalName(signal)
    signal(signalName, args.a1, args.a2)
  }

  /**
   * Sends a signal with three arguments to the child workflow.
   */
  public suspend fun <A1, A2, A3> signal(
    signal: KFunction4<T, A1, A2, A3, Unit>,
    args: KArgs3<A1, A2, A3>
  ) {
    val signalName = extractSignalName(signal)
    signal(signalName, args.a1, args.a2, args.a3)
  }

  /**
   * Sends a signal with four arguments to the child workflow.
   */
  public suspend fun <A1, A2, A3, A4> signal(
    signal: KFunction5<T, A1, A2, A3, A4, Unit>,
    args: KArgs4<A1, A2, A3, A4>
  ) {
    val signalName = extractSignalName(signal)
    signal(signalName, args.a1, args.a2, args.a3, args.a4)
  }

  /**
   * Sends a signal with five arguments to the child workflow.
   */
  public suspend fun <A1, A2, A3, A4, A5> signal(
    signal: KFunction6<T, A1, A2, A3, A4, A5, Unit>,
    args: KArgs5<A1, A2, A3, A4, A5>
  ) {
    val signalName = extractSignalName(signal)
    signal(signalName, args.a1, args.a2, args.a3, args.a4, args.a5)
  }

  /**
   * Sends a signal with six arguments to the child workflow.
   */
  public suspend fun <A1, A2, A3, A4, A5, A6> signal(
    signal: KFunction7<T, A1, A2, A3, A4, A5, A6, Unit>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>
  ) {
    val signalName = extractSignalName(signal)
    signal(signalName, args.a1, args.a2, args.a3, args.a4, args.a5, args.a6)
  }

  /**
   * Sends a signal by name to the child workflow.
   *
   * @param signalName the name of the signal
   * @param args the signal arguments
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun signal(signalName: String, vararg args: Any?) {
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

  /**
   * Requests cancellation of the child workflow.
   *
   * This is a request; the child workflow may choose to ignore it
   * or perform cleanup before terminating.
   */
  @OptIn(InternalTemporalApi::class)
  public suspend fun cancel() {
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
