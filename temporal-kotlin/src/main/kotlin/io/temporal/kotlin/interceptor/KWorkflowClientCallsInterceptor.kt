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

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.update.v1.WaitPolicy
import io.temporal.client.WorkflowExecutionCount
import io.temporal.client.WorkflowExecutionDescription
import io.temporal.client.WorkflowExecutionMetadata
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowUpdateHandle
import io.temporal.common.interceptors.Header
import kotlinx.coroutines.flow.Flow
import java.lang.reflect.Type
import java.util.Optional
import java.util.concurrent.TimeUnit

/**
 * Intercepts calls to the WorkflowClient related to the lifecycle of a Workflow.
 *
 * This is a suspend function interface to support non-blocking client operations.
 *
 * Prefer extending [KWorkflowClientCallsInterceptorBase] and overriding only the methods
 * you need instead of implementing this interface directly.
 *
 * The implementation must forward all the calls to the next interceptor.
 *
 * Example:
 * ```kotlin
 * class LoggingCallsInterceptor(
 *     next: KWorkflowClientCallsInterceptor
 * ) : KWorkflowClientCallsInterceptorBase(next) {
 *
 *     override suspend fun start(input: StartInput): StartOutput {
 *         println("Starting workflow: ${input.workflowType}")
 *         return next.start(input)
 *     }
 *
 *     override suspend fun signal(input: SignalInput): SignalOutput {
 *         println("Signaling workflow: ${input.signalName}")
 *         return next.signal(input)
 *     }
 * }
 * ```
 *
 * @see KWorkflowClientInterceptor.workflowClientCallsInterceptor
 * @see KWorkflowClientCallsInterceptorBase
 */
public interface KWorkflowClientCallsInterceptor {

  /**
   * Intercepts workflow start.
   *
   * If you implement this method, [signalWithStart] and [updateWithStart] most likely
   * need to be implemented too.
   *
   * @see signalWithStart
   * @see updateWithStart
   */
  public suspend fun start(input: StartInput): StartOutput

  /**
   * Intercepts workflow signal.
   *
   * If you implement this method, [signalWithStart] most likely needs to be implemented too.
   *
   * @see signalWithStart
   */
  public suspend fun signal(input: SignalInput): SignalOutput

  /**
   * Intercepts signal-with-start operation.
   */
  public suspend fun signalWithStart(input: SignalWithStartInput): SignalWithStartOutput

  /**
   * Intercepts update-with-start operation.
   */
  public suspend fun <R> updateWithStart(input: UpdateWithStartInput<R>): UpdateWithStartOutput<R>

  /**
   * Intercepts getting workflow result.
   *
   * If you implement this method, [getResultAsync] most likely needs to be implemented too.
   *
   * @see getResultAsync
   */
  public suspend fun <R> getResult(input: GetResultInput<R>): GetResultOutput<R>

  /**
   * Intercepts getting workflow result asynchronously.
   *
   * If you implement this method, [getResult] most likely needs to be implemented too.
   *
   * @see getResult
   */
  public suspend fun <R> getResultAsync(input: GetResultInput<R>): GetResultAsyncOutput<R>

  /**
   * Intercepts workflow query.
   */
  public suspend fun <R> query(input: QueryInput<R>): QueryOutput<R>

  /**
   * Intercepts starting a workflow update.
   */
  public suspend fun <R> startUpdate(input: StartUpdateInput<R>): WorkflowUpdateHandle<R>

  /**
   * Intercepts polling for workflow update result.
   */
  public suspend fun <R> pollWorkflowUpdate(input: PollWorkflowUpdateInput<R>): PollWorkflowUpdateOutput<R>

  /**
   * Intercepts workflow cancellation.
   */
  public suspend fun cancel(input: CancelInput): CancelOutput

  /**
   * Intercepts workflow termination.
   */
  public suspend fun terminate(input: TerminateInput): TerminateOutput

  /**
   * Intercepts workflow describe.
   */
  public suspend fun describe(input: DescribeInput): DescribeOutput

  /**
   * Intercepts listing workflow executions.
   */
  public suspend fun listWorkflowExecutions(input: ListWorkflowExecutionsInput): ListWorkflowExecutionsOutput

  /**
   * Intercepts counting workflow executions.
   */
  public suspend fun countWorkflows(input: CountWorkflowsInput): CountWorkflowOutput

  // ========== Input/Output Types ==========

  /**
   * Input for workflow start operation.
   *
   * @property workflowId ID of the workflow to be started
   * @property workflowType workflow type name
   * @property header internal Temporal header for context propagation
   * @property arguments input arguments for the workflow
   * @property options workflow options
   */
  public data class StartInput(
    val workflowId: String,
    val workflowType: String,
    val header: Header,
    val arguments: Array<Any?>,
    val options: WorkflowOptions
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as StartInput
      if (workflowId != other.workflowId) return false
      if (workflowType != other.workflowType) return false
      if (header != other.header) return false
      if (!arguments.contentEquals(other.arguments)) return false
      if (options != other.options) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowId.hashCode()
      result = 31 * result + workflowType.hashCode()
      result = 31 * result + header.hashCode()
      result = 31 * result + arguments.contentHashCode()
      result = 31 * result + options.hashCode()
      return result
    }
  }

  /**
   * Output from workflow start operation.
   *
   * @property workflowExecution the started workflow execution
   */
  public data class StartOutput(
    val workflowExecution: WorkflowExecution
  )

  /**
   * Input for workflow signal operation.
   *
   * @property workflowExecution the target workflow execution
   * @property signalName the signal name
   * @property header internal Temporal header for context propagation
   * @property arguments the signal arguments
   */
  public data class SignalInput(
    val workflowExecution: WorkflowExecution,
    val signalName: String,
    val header: Header,
    val arguments: Array<Any?>
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as SignalInput
      if (workflowExecution != other.workflowExecution) return false
      if (signalName != other.signalName) return false
      if (header != other.header) return false
      if (!arguments.contentEquals(other.arguments)) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowExecution.hashCode()
      result = 31 * result + signalName.hashCode()
      result = 31 * result + header.hashCode()
      result = 31 * result + arguments.contentHashCode()
      return result
    }
  }

  /**
   * Output from workflow signal operation.
   */
  public class SignalOutput

  /**
   * Input for signal-with-start operation.
   *
   * @property workflowStartInput the workflow start input
   * @property signalName the signal name
   * @property signalArguments the signal arguments
   */
  public data class SignalWithStartInput(
    val workflowStartInput: StartInput,
    val signalName: String,
    val signalArguments: Array<Any?>
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as SignalWithStartInput
      if (workflowStartInput != other.workflowStartInput) return false
      if (signalName != other.signalName) return false
      if (!signalArguments.contentEquals(other.signalArguments)) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowStartInput.hashCode()
      result = 31 * result + signalName.hashCode()
      result = 31 * result + signalArguments.contentHashCode()
      return result
    }
  }

  /**
   * Output from signal-with-start operation.
   *
   * @property workflowStartOutput the workflow start output
   */
  public data class SignalWithStartOutput(
    val workflowStartOutput: StartOutput
  )

  /**
   * Input for update-with-start operation.
   *
   * @property workflowStartInput the workflow start input
   * @property startUpdateInput the update start input
   */
  public data class UpdateWithStartInput<R>(
    val workflowStartInput: StartInput,
    val startUpdateInput: StartUpdateInput<R>
  )

  /**
   * Output from update-with-start operation.
   *
   * @property workflowStartOutput the workflow start output
   * @property updateHandle the update handle
   */
  public data class UpdateWithStartOutput<R>(
    val workflowStartOutput: StartOutput,
    val updateHandle: WorkflowUpdateHandle<R>
  )

  /**
   * Input for getting workflow result.
   *
   * @property workflowExecution the workflow execution
   * @property workflowType optional workflow type
   * @property timeout timeout value
   * @property timeoutUnit timeout unit
   * @property resultClass the result class
   * @property resultType the result type (for generics)
   */
  public data class GetResultInput<R>(
    val workflowExecution: WorkflowExecution,
    val workflowType: Optional<String>,
    val timeout: Long,
    val timeoutUnit: TimeUnit,
    val resultClass: Class<R>,
    val resultType: Type
  )

  /**
   * Output from getting workflow result.
   *
   * @property result the workflow result
   */
  public data class GetResultOutput<R>(
    val result: R
  )

  /**
   * Output from getting workflow result asynchronously.
   *
   * @property resultFlow a Flow that emits the result when available
   */
  public data class GetResultAsyncOutput<R>(
    val resultFlow: Flow<R>
  )

  /**
   * Input for workflow query.
   *
   * @property workflowExecution the workflow execution
   * @property queryType the query type name
   * @property header internal Temporal header for context propagation
   * @property arguments the query arguments
   * @property resultClass the result class
   * @property resultType the result type (for generics)
   */
  public data class QueryInput<R>(
    val workflowExecution: WorkflowExecution,
    val queryType: String,
    val header: Header,
    val arguments: Array<Any?>,
    val resultClass: Class<R>,
    val resultType: Type
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as QueryInput<*>
      if (workflowExecution != other.workflowExecution) return false
      if (queryType != other.queryType) return false
      if (header != other.header) return false
      if (!arguments.contentEquals(other.arguments)) return false
      if (resultClass != other.resultClass) return false
      if (resultType != other.resultType) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowExecution.hashCode()
      result = 31 * result + queryType.hashCode()
      result = 31 * result + header.hashCode()
      result = 31 * result + arguments.contentHashCode()
      result = 31 * result + resultClass.hashCode()
      result = 31 * result + resultType.hashCode()
      return result
    }
  }

  /**
   * Output from workflow query.
   *
   * @property queryRejectedStatus null if query is not rejected, otherwise the rejection status
   * @property result the query result
   */
  public data class QueryOutput<R>(
    val queryRejectedStatus: WorkflowExecutionStatus?,
    val result: R
  ) {
    /**
     * Whether the query was rejected.
     */
    val isQueryRejected: Boolean
      get() = queryRejectedStatus != null
  }

  /**
   * Input for starting a workflow update.
   *
   * @property workflowExecution the workflow execution
   * @property workflowType optional workflow type
   * @property updateName the update name
   * @property header internal Temporal header for context propagation
   * @property updateId the update ID
   * @property arguments the update arguments
   * @property resultClass the result class
   * @property resultType the result type (for generics)
   * @property firstExecutionRunId the first execution run ID
   * @property waitPolicy the wait policy
   */
  public data class StartUpdateInput<R>(
    val workflowExecution: WorkflowExecution,
    val workflowType: Optional<String>,
    val updateName: String,
    val header: Header,
    val updateId: String,
    val arguments: Array<Any?>,
    val resultClass: Class<R>,
    val resultType: Type,
    val firstExecutionRunId: String,
    val waitPolicy: WaitPolicy
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as StartUpdateInput<*>
      if (workflowExecution != other.workflowExecution) return false
      if (workflowType != other.workflowType) return false
      if (updateName != other.updateName) return false
      if (header != other.header) return false
      if (updateId != other.updateId) return false
      if (!arguments.contentEquals(other.arguments)) return false
      if (resultClass != other.resultClass) return false
      if (resultType != other.resultType) return false
      if (firstExecutionRunId != other.firstExecutionRunId) return false
      if (waitPolicy != other.waitPolicy) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowExecution.hashCode()
      result = 31 * result + workflowType.hashCode()
      result = 31 * result + updateName.hashCode()
      result = 31 * result + header.hashCode()
      result = 31 * result + updateId.hashCode()
      result = 31 * result + arguments.contentHashCode()
      result = 31 * result + resultClass.hashCode()
      result = 31 * result + resultType.hashCode()
      result = 31 * result + firstExecutionRunId.hashCode()
      result = 31 * result + waitPolicy.hashCode()
      return result
    }
  }

  /**
   * Input for polling workflow update.
   *
   * @property workflowExecution the workflow execution
   * @property updateName the update name
   * @property updateId the update ID
   * @property resultClass the result class
   * @property resultType the result type (for generics)
   * @property timeout timeout value
   * @property timeoutUnit timeout unit
   */
  public data class PollWorkflowUpdateInput<R>(
    val workflowExecution: WorkflowExecution,
    val updateName: String,
    val updateId: String,
    val resultClass: Class<R>,
    val resultType: Type,
    val timeout: Long,
    val timeoutUnit: TimeUnit
  )

  /**
   * Output from polling workflow update.
   *
   * @property resultFlow a Flow that emits the result when available
   */
  public data class PollWorkflowUpdateOutput<R>(
    val resultFlow: Flow<R>
  )

  /**
   * Input for workflow cancellation.
   *
   * @property workflowExecution the workflow execution
   * @property firstExecutionRunId optional first execution run ID
   * @property reason optional cancellation reason
   */
  public data class CancelInput(
    val workflowExecution: WorkflowExecution,
    val firstExecutionRunId: String?,
    val reason: String?
  )

  /**
   * Output from workflow cancellation.
   */
  public class CancelOutput

  /**
   * Input for workflow termination.
   *
   * @property workflowExecution the workflow execution
   * @property firstExecutionRunId optional first execution run ID
   * @property reason optional termination reason
   * @property details termination details
   */
  public data class TerminateInput(
    val workflowExecution: WorkflowExecution,
    val firstExecutionRunId: String?,
    val reason: String?,
    val details: Array<Any?>
  ) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false
      other as TerminateInput
      if (workflowExecution != other.workflowExecution) return false
      if (firstExecutionRunId != other.firstExecutionRunId) return false
      if (reason != other.reason) return false
      if (!details.contentEquals(other.details)) return false
      return true
    }

    override fun hashCode(): Int {
      var result = workflowExecution.hashCode()
      result = 31 * result + (firstExecutionRunId?.hashCode() ?: 0)
      result = 31 * result + (reason?.hashCode() ?: 0)
      result = 31 * result + details.contentHashCode()
      return result
    }
  }

  /**
   * Output from workflow termination.
   */
  public class TerminateOutput

  /**
   * Input for workflow describe.
   *
   * @property workflowExecution the workflow execution
   */
  public data class DescribeInput(
    val workflowExecution: WorkflowExecution
  )

  /**
   * Output from workflow describe.
   *
   * @property description the workflow execution description
   */
  public data class DescribeOutput(
    val description: WorkflowExecutionDescription
  )

  /**
   * Input for listing workflow executions.
   *
   * @property query optional visibility query
   * @property pageSize optional page size
   */
  public data class ListWorkflowExecutionsInput(
    val query: String?,
    val pageSize: Int?
  )

  /**
   * Output from listing workflow executions.
   *
   * @property executionsFlow a Flow of workflow execution metadata
   */
  public data class ListWorkflowExecutionsOutput(
    val executionsFlow: Flow<WorkflowExecutionMetadata>
  )

  /**
   * Input for counting workflow executions.
   *
   * @property query optional visibility query
   */
  public data class CountWorkflowsInput(
    val query: String?
  )

  /**
   * Output from counting workflow executions.
   *
   * @property count the workflow execution count
   */
  public data class CountWorkflowOutput(
    val count: WorkflowExecutionCount
  )
}

/**
 * Base implementation that forwards all calls to the next interceptor.
 *
 * Extend this class and override only the methods you need.
 *
 * @param next the next interceptor in the chain
 */
public open class KWorkflowClientCallsInterceptorBase(
  protected val next: KWorkflowClientCallsInterceptor
) : KWorkflowClientCallsInterceptor {

  override suspend fun start(input: KWorkflowClientCallsInterceptor.StartInput): KWorkflowClientCallsInterceptor.StartOutput {
    return next.start(input)
  }

  override suspend fun signal(input: KWorkflowClientCallsInterceptor.SignalInput): KWorkflowClientCallsInterceptor.SignalOutput {
    return next.signal(input)
  }

  override suspend fun signalWithStart(input: KWorkflowClientCallsInterceptor.SignalWithStartInput): KWorkflowClientCallsInterceptor.SignalWithStartOutput {
    return next.signalWithStart(input)
  }

  override suspend fun <R> updateWithStart(input: KWorkflowClientCallsInterceptor.UpdateWithStartInput<R>): KWorkflowClientCallsInterceptor.UpdateWithStartOutput<R> {
    return next.updateWithStart(input)
  }

  override suspend fun <R> getResult(input: KWorkflowClientCallsInterceptor.GetResultInput<R>): KWorkflowClientCallsInterceptor.GetResultOutput<R> {
    return next.getResult(input)
  }

  override suspend fun <R> getResultAsync(input: KWorkflowClientCallsInterceptor.GetResultInput<R>): KWorkflowClientCallsInterceptor.GetResultAsyncOutput<R> {
    return next.getResultAsync(input)
  }

  override suspend fun <R> query(input: KWorkflowClientCallsInterceptor.QueryInput<R>): KWorkflowClientCallsInterceptor.QueryOutput<R> {
    return next.query(input)
  }

  override suspend fun <R> startUpdate(input: KWorkflowClientCallsInterceptor.StartUpdateInput<R>): WorkflowUpdateHandle<R> {
    return next.startUpdate(input)
  }

  override suspend fun <R> pollWorkflowUpdate(input: KWorkflowClientCallsInterceptor.PollWorkflowUpdateInput<R>): KWorkflowClientCallsInterceptor.PollWorkflowUpdateOutput<R> {
    return next.pollWorkflowUpdate(input)
  }

  override suspend fun cancel(input: KWorkflowClientCallsInterceptor.CancelInput): KWorkflowClientCallsInterceptor.CancelOutput {
    return next.cancel(input)
  }

  override suspend fun terminate(input: KWorkflowClientCallsInterceptor.TerminateInput): KWorkflowClientCallsInterceptor.TerminateOutput {
    return next.terminate(input)
  }

  override suspend fun describe(input: KWorkflowClientCallsInterceptor.DescribeInput): KWorkflowClientCallsInterceptor.DescribeOutput {
    return next.describe(input)
  }

  override suspend fun listWorkflowExecutions(input: KWorkflowClientCallsInterceptor.ListWorkflowExecutionsInput): KWorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput {
    return next.listWorkflowExecutions(input)
  }

  override suspend fun countWorkflows(input: KWorkflowClientCallsInterceptor.CountWorkflowsInput): KWorkflowClientCallsInterceptor.CountWorkflowOutput {
    return next.countWorkflows(input)
  }
}
