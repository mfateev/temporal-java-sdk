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

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.kotlin.common.KArgs2
import io.temporal.kotlin.common.KArgs3
import io.temporal.kotlin.common.KArgs4
import io.temporal.kotlin.common.KArgs5
import io.temporal.kotlin.common.KArgs6
import io.temporal.workflow.ExternalWorkflowStub
import io.temporal.workflow.SignalMethod
import kotlin.reflect.KFunction
import kotlin.reflect.KFunction1
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3
import kotlin.reflect.KFunction4
import kotlin.reflect.KFunction5
import kotlin.reflect.KFunction6
import kotlin.reflect.KFunction7
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.jvm.javaMethod

/**
 * Handle for interacting with an external workflow (workflow in a different execution).
 *
 * External workflows can be signaled and cancelled, but their results cannot be awaited
 * from within a workflow (use the client API for that).
 *
 * Obtain a handle via [KWorkflow.getExternalWorkflowHandle].
 *
 * Example:
 * ```kotlin
 * // Get typed handle for external workflow
 * val handle = KWorkflow.getExternalWorkflowHandle<OrderWorkflow>("order-123")
 *
 * // Signal using method reference (type-safe)
 * handle.signal(OrderWorkflow::updatePriority, Priority.HIGH)
 *
 * // Cancel the external workflow
 * handle.cancel()
 * ```
 *
 * @param T The external workflow interface type (for type-safe signals)
 */
public class KExternalWorkflowHandle<T : Any> internal constructor(
  private val stub: ExternalWorkflowStub,
  private val workflowInterface: Class<T>
) {

  /**
   * The workflow ID of the external workflow.
   */
  public val workflowId: String
    get() = stub.execution.workflowId

  /**
   * The run ID of the external workflow, or null if not specified.
   */
  public val runId: String?
    get() = stub.execution.runId.takeIf { it.isNotEmpty() }

  /**
   * The workflow execution details.
   */
  public val execution: WorkflowExecution
    get() = stub.execution

  // ==================== Signal Methods (0-6 args) ====================

  /**
   * Sends a signal to the external workflow using a method reference.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   */
  public fun signal(method: KFunction1<T, *>) {
    val signalName = extractSignalName(method)
    stub.signal(signalName)
  }

  /**
   * Sends a signal to the external workflow with 1 argument.
   */
  public fun <A1> signal(method: KFunction2<T, A1, *>, arg1: A1) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, arg1)
  }

  /**
   * Sends a signal to the external workflow with 2 arguments.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   * @param args the arguments wrapped using [io.temporal.kotlin.common.kargs]
   */
  public fun <A1, A2> signal(method: KFunction3<T, A1, A2, *>, args: KArgs2<A1, A2>) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, args.a1, args.a2)
  }

  /**
   * Sends a signal to the external workflow with 3 arguments.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   * @param args the arguments wrapped using [io.temporal.kotlin.common.kargs]
   */
  public fun <A1, A2, A3> signal(method: KFunction4<T, A1, A2, A3, *>, args: KArgs3<A1, A2, A3>) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, args.a1, args.a2, args.a3)
  }

  /**
   * Sends a signal to the external workflow with 4 arguments.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   * @param args the arguments wrapped using [io.temporal.kotlin.common.kargs]
   */
  public fun <A1, A2, A3, A4> signal(
    method: KFunction5<T, A1, A2, A3, A4, *>,
    args: KArgs4<A1, A2, A3, A4>
  ) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, args.a1, args.a2, args.a3, args.a4)
  }

  /**
   * Sends a signal to the external workflow with 5 arguments.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   * @param args the arguments wrapped using [io.temporal.kotlin.common.kargs]
   */
  public fun <A1, A2, A3, A4, A5> signal(
    method: KFunction6<T, A1, A2, A3, A4, A5, *>,
    args: KArgs5<A1, A2, A3, A4, A5>
  ) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, args.a1, args.a2, args.a3, args.a4, args.a5)
  }

  /**
   * Sends a signal to the external workflow with 6 arguments.
   *
   * @param method the signal method reference (must be annotated with @SignalMethod)
   * @param args the arguments wrapped using [io.temporal.kotlin.common.kargs]
   */
  public fun <A1, A2, A3, A4, A5, A6> signal(
    method: KFunction7<T, A1, A2, A3, A4, A5, A6, *>,
    args: KArgs6<A1, A2, A3, A4, A5, A6>
  ) {
    val signalName = extractSignalName(method)
    stub.signal(signalName, args.a1, args.a2, args.a3, args.a4, args.a5, args.a6)
  }

  // ==================== Cancel ====================

  /**
   * Requests cancellation of the external workflow.
   */
  public fun cancel() {
    stub.cancel()
  }

  /**
   * Requests cancellation of the external workflow with a reason.
   *
   * @param reason optional reason for cancellation
   */
  public fun cancel(reason: String?) {
    stub.cancel(reason)
  }

  // ==================== Helper Methods ====================

  private fun extractSignalName(method: KFunction<*>): String {
    // Check for @SignalMethod annotation
    val annotation = method.findAnnotation<SignalMethod>()
    if (annotation != null && annotation.name.isNotEmpty()) {
      return annotation.name
    }

    // Check on Java method
    val javaMethod = method.javaMethod
    if (javaMethod != null) {
      val javaAnnotation = javaMethod.getAnnotation(SignalMethod::class.java)
      if (javaAnnotation != null && javaAnnotation.name.isNotEmpty()) {
        return javaAnnotation.name
      }
    }

    // Default to method name
    return method.name
  }
}

/**
 * Untyped handle for interacting with an external workflow.
 *
 * Use this when the workflow type is not known at compile time.
 *
 * Obtain a handle via [KWorkflow.getExternalWorkflowHandle] (untyped overload).
 *
 * Example:
 * ```kotlin
 * // Get untyped handle
 * val handle = KWorkflow.getExternalWorkflowHandle("order-123")
 *
 * // Signal by name
 * handle.signal("updatePriority", Priority.HIGH)
 *
 * // Cancel
 * handle.cancel()
 * ```
 */
public class KUntypedExternalWorkflowHandle internal constructor(
  private val stub: ExternalWorkflowStub
) {

  /**
   * The workflow ID of the external workflow.
   */
  public val workflowId: String
    get() = stub.execution.workflowId

  /**
   * The run ID of the external workflow, or null if not specified.
   */
  public val runId: String?
    get() = stub.execution.runId.takeIf { it.isNotEmpty() }

  /**
   * The workflow execution details.
   */
  public val execution: WorkflowExecution
    get() = stub.execution

  /**
   * Sends a signal to the external workflow.
   *
   * @param signalName the name of the signal
   * @param args arguments to pass to the signal handler
   */
  public fun signal(signalName: String, vararg args: Any?) {
    stub.signal(signalName, *args)
  }

  /**
   * Requests cancellation of the external workflow.
   */
  public fun cancel() {
    stub.cancel()
  }

  /**
   * Requests cancellation of the external workflow with a reason.
   *
   * @param reason optional reason for cancellation
   */
  public fun cancel(reason: String?) {
    stub.cancel(reason)
  }
}
