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

package io.temporal.kotlin.interceptor

import io.temporal.common.interceptors.Header
import io.temporal.kotlin.common.KEncodedValues

/**
 * Input for workflow execution.
 *
 * @property header the workflow header containing metadata
 * @property arguments the arguments passed to the workflow method
 */
public data class KWorkflowInput(
  val header: Header,
  val arguments: Array<Any?>
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KWorkflowInput
    if (header != other.header) return false
    if (!arguments.contentEquals(other.arguments)) return false
    return true
  }

  override fun hashCode(): Int {
    var result = header.hashCode()
    result = 31 * result + arguments.contentHashCode()
    return result
  }
}

/**
 * Output from workflow execution.
 *
 * @property result the result of the workflow execution
 */
public data class KWorkflowOutput(val result: Any?)

/**
 * Input for signal handling.
 *
 * @property signalName the name of the signal
 * @property arguments the arguments passed with the signal (for annotation-based handlers)
 * @property encodedValues the raw encoded values for dynamic handlers to decode
 * @property eventId the event ID of the signal in the workflow history
 * @property header the signal header containing metadata
 */
public data class KSignalInput(
  val signalName: String,
  val arguments: Array<Any?>,
  val encodedValues: KEncodedValues,
  val eventId: Long,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KSignalInput
    if (signalName != other.signalName) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (eventId != other.eventId) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = signalName.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + eventId.hashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Input for query handling.
 *
 * @property queryName the name of the query
 * @property arguments the arguments passed with the query (for annotation-based handlers)
 * @property encodedValues the raw encoded values for dynamic handlers to decode
 * @property header the query header containing metadata
 */
public data class KQueryInput(
  val queryName: String,
  val arguments: Array<Any?>,
  val encodedValues: KEncodedValues,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KQueryInput
    if (queryName != other.queryName) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = queryName.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Output from query handling.
 *
 * @property result the result of the query
 */
public data class KQueryOutput(val result: Any?)

/**
 * Input for update handling.
 *
 * @property updateName the name of the update
 * @property arguments the arguments passed with the update (for annotation-based handlers)
 * @property encodedValues the raw encoded values for dynamic handlers to decode
 * @property header the update header containing metadata
 */
public data class KUpdateInput(
  val updateName: String,
  val arguments: Array<Any?>,
  val encodedValues: KEncodedValues,
  val header: Header
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KUpdateInput
    if (updateName != other.updateName) return false
    if (!arguments.contentEquals(other.arguments)) return false
    if (header != other.header) return false
    return true
  }

  override fun hashCode(): Int {
    var result = updateName.hashCode()
    result = 31 * result + arguments.contentHashCode()
    result = 31 * result + header.hashCode()
    return result
  }
}

/**
 * Output from update handling.
 *
 * @property result the result of the update
 */
public data class KUpdateOutput(val result: Any?)

/**
 * Intercepts inbound calls to workflow execution (workflow method, signals, queries, updates).
 *
 * An instance should be created in [KWorkerInterceptor.interceptWorkflow].
 *
 * The calls to this interceptor are executed under workflow context, all the rules and
 * restrictions on the workflow code apply.
 *
 * Prefer extending [KWorkflowInboundCallsInterceptorBase] and overriding only the methods
 * you need instead of implementing this interface directly.
 *
 * The implementation must forward all the calls to the next interceptor.
 *
 * Example:
 * ```kotlin
 * class LoggingWorkflowInterceptor(
 *     next: KWorkflowInboundCallsInterceptor
 * ) : KWorkflowInboundCallsInterceptorBase(next) {
 *
 *     override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
 *         val log = KWorkflow.logger()
 *         log.info("Workflow started")
 *         return try {
 *             val result = next.execute(input)
 *             log.info("Workflow completed")
 *             result
 *         } catch (e: Exception) {
 *             log.error("Workflow failed", e)
 *             throw e
 *         }
 *     }
 * }
 * ```
 *
 * @see KWorkerInterceptor.interceptWorkflow
 * @see KWorkflowInboundCallsInterceptorBase
 */
public interface KWorkflowInboundCallsInterceptor {
  /**
   * Called when the workflow is instantiated. Use this to wrap the outbound interceptor.
   *
   * The instance should be passed into `next.init(newWorkflowOutboundCallsInterceptor)`.
   *
   * @param outboundCalls an existing interceptor instance to be proxied by the interceptor
   *     created inside this method
   */
  public suspend fun init(outboundCalls: KWorkflowOutboundCallsInterceptor)

  /**
   * Called when the workflow main method is invoked.
   *
   * @param input the workflow input containing header and arguments
   * @return the workflow output containing the result
   */
  public suspend fun execute(input: KWorkflowInput): KWorkflowOutput

  /**
   * Called when a signal is delivered to the workflow.
   *
   * @param input the signal input containing name, arguments, event ID, and header
   */
  public suspend fun handleSignal(input: KSignalInput)

  /**
   * Called when a query is made to the workflow.
   *
   * Note: Queries must be synchronous and cannot modify workflow state.
   *
   * @param input the query input containing name, arguments, and header
   * @return the query output containing the result
   */
  public fun handleQuery(input: KQueryInput): KQueryOutput

  /**
   * Called to validate an update before execution.
   *
   * Throw an exception to reject the update.
   *
   * @param input the update input containing name, arguments, and header
   */
  public fun validateUpdate(input: KUpdateInput)

  /**
   * Called to execute an update after validation passes.
   *
   * @param input the update input containing name, arguments, and header
   * @return the update output containing the result
   */
  public suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput
}

/**
 * Base implementation that forwards all calls to the next interceptor.
 *
 * Extend this class and override only the methods you need.
 *
 * @param next the next interceptor in the chain
 */
public open class KWorkflowInboundCallsInterceptorBase(
  protected val next: KWorkflowInboundCallsInterceptor
) : KWorkflowInboundCallsInterceptor {

  override suspend fun init(outboundCalls: KWorkflowOutboundCallsInterceptor) {
    next.init(outboundCalls)
  }

  override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
    return next.execute(input)
  }

  override suspend fun handleSignal(input: KSignalInput) {
    next.handleSignal(input)
  }

  override fun handleQuery(input: KQueryInput): KQueryOutput {
    return next.handleQuery(input)
  }

  override fun validateUpdate(input: KUpdateInput) {
    next.validateUpdate(input)
  }

  override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
    return next.executeUpdate(input)
  }
}
