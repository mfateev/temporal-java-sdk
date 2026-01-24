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

package io.temporal.kotlin.internal.converters

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.client.ActivityCompletionClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateHandle
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor
import io.temporal.common.interceptors.WorkflowClientCallsInterceptorBase
import io.temporal.common.interceptors.WorkflowClientInterceptor
import io.temporal.kotlin.interceptor.KWorkflowClientCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowClientInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.Optional
import java.util.concurrent.TimeoutException

/**
 * Wraps a Kotlin [KWorkflowClientInterceptor] to be used with the Java SDK.
 *
 * This handles the conversion from suspend functions to blocking calls using runBlocking.
 */
@InternalTemporalApi
public class KWorkflowClientInterceptorJavaWrapper(
  private val kotlinInterceptor: KWorkflowClientInterceptor
) : WorkflowClientInterceptor {

  @Deprecated("Use workflowClientCallsInterceptor instead")
  override fun newUntypedWorkflowStub(
    workflowType: String,
    options: WorkflowOptions,
    next: WorkflowStub
  ): WorkflowStub = next

  @Deprecated("Use workflowClientCallsInterceptor instead")
  override fun newUntypedWorkflowStub(
    execution: WorkflowExecution,
    workflowType: Optional<String>,
    next: WorkflowStub
  ): WorkflowStub = next

  override fun newActivityCompletionClient(next: ActivityCompletionClient): ActivityCompletionClient = next

  override fun workflowClientCallsInterceptor(
    next: WorkflowClientCallsInterceptor
  ): WorkflowClientCallsInterceptor {
    // Wrap the Java next interceptor as Kotlin, then pass to Kotlin interceptor
    val kotlinNext = JavaCallsInterceptorToKotlin(next)
    val kotlinIntercepted = kotlinInterceptor.workflowClientCallsInterceptor(kotlinNext)
    // Wrap the Kotlin result back as Java
    return KotlinCallsInterceptorToJava(kotlinIntercepted)
  }
}

/**
 * Wraps a Java [WorkflowClientInterceptor] to be used with the Kotlin SDK.
 *
 * This handles the conversion from blocking calls to suspend functions.
 */
@InternalTemporalApi
public class WorkflowClientInterceptorKotlinWrapper(
  private val javaInterceptor: WorkflowClientInterceptor
) : KWorkflowClientInterceptor {

  override fun workflowClientCallsInterceptor(
    next: KWorkflowClientCallsInterceptor
  ): KWorkflowClientCallsInterceptor {
    // Wrap the Kotlin next interceptor as Java, then pass to Java interceptor
    val javaNext = KotlinCallsInterceptorToJava(next)
    val javaIntercepted = javaInterceptor.workflowClientCallsInterceptor(javaNext)
    // Wrap the Java result back as Kotlin
    return JavaCallsInterceptorToKotlin(javaIntercepted)
  }
}

/**
 * Wraps a Java [WorkflowClientCallsInterceptor] to be used as a Kotlin [KWorkflowClientCallsInterceptor].
 *
 * Blocking Java calls are wrapped with withContext(Dispatchers.IO).
 */
@InternalTemporalApi
internal class JavaCallsInterceptorToKotlin(
  private val javaInterceptor: WorkflowClientCallsInterceptor
) : KWorkflowClientCallsInterceptor {

  override suspend fun start(input: KWorkflowClientCallsInterceptor.StartInput): KWorkflowClientCallsInterceptor.StartOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.WorkflowStartInput(
        input.workflowId,
        input.workflowType,
        input.header,
        input.arguments,
        input.options
      )
      val javaOutput = javaInterceptor.start(javaInput)
      KWorkflowClientCallsInterceptor.StartOutput(javaOutput.workflowExecution)
    }
  }

  override suspend fun signal(input: KWorkflowClientCallsInterceptor.SignalInput): KWorkflowClientCallsInterceptor.SignalOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.WorkflowSignalInput(
        input.workflowExecution,
        input.signalName,
        input.header,
        input.arguments
      )
      javaInterceptor.signal(javaInput)
      KWorkflowClientCallsInterceptor.SignalOutput()
    }
  }

  override suspend fun signalWithStart(input: KWorkflowClientCallsInterceptor.SignalWithStartInput): KWorkflowClientCallsInterceptor.SignalWithStartOutput {
    return withContext(Dispatchers.IO) {
      val javaStartInput = WorkflowClientCallsInterceptor.WorkflowStartInput(
        input.workflowStartInput.workflowId,
        input.workflowStartInput.workflowType,
        input.workflowStartInput.header,
        input.workflowStartInput.arguments,
        input.workflowStartInput.options
      )
      val javaInput = WorkflowClientCallsInterceptor.WorkflowSignalWithStartInput(
        javaStartInput,
        input.signalName,
        input.signalArguments
      )
      val javaOutput = javaInterceptor.signalWithStart(javaInput)
      KWorkflowClientCallsInterceptor.SignalWithStartOutput(
        KWorkflowClientCallsInterceptor.StartOutput(javaOutput.workflowStartOutput.workflowExecution)
      )
    }
  }

  override suspend fun <R> updateWithStart(input: KWorkflowClientCallsInterceptor.UpdateWithStartInput<R>): KWorkflowClientCallsInterceptor.UpdateWithStartOutput<R> {
    return withContext(Dispatchers.IO) {
      val javaStartInput = WorkflowClientCallsInterceptor.WorkflowStartInput(
        input.workflowStartInput.workflowId,
        input.workflowStartInput.workflowType,
        input.workflowStartInput.header,
        input.workflowStartInput.arguments,
        input.workflowStartInput.options
      )
      val javaUpdateInput = WorkflowClientCallsInterceptor.StartUpdateInput(
        input.startUpdateInput.workflowExecution,
        input.startUpdateInput.workflowType,
        input.startUpdateInput.updateName,
        input.startUpdateInput.header,
        input.startUpdateInput.updateId,
        input.startUpdateInput.arguments,
        input.startUpdateInput.resultClass,
        input.startUpdateInput.resultType,
        input.startUpdateInput.firstExecutionRunId,
        input.startUpdateInput.waitPolicy
      )
      val javaInput = WorkflowClientCallsInterceptor.WorkflowUpdateWithStartInput(
        javaStartInput,
        javaUpdateInput
      )
      val javaOutput = javaInterceptor.updateWithStart(javaInput)
      KWorkflowClientCallsInterceptor.UpdateWithStartOutput(
        KWorkflowClientCallsInterceptor.StartOutput(javaOutput.workflowStartOutput.workflowExecution),
        javaOutput.updateHandle
      )
    }
  }

  override suspend fun <R> getResult(input: KWorkflowClientCallsInterceptor.GetResultInput<R>): KWorkflowClientCallsInterceptor.GetResultOutput<R> {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.GetResultInput(
        input.workflowExecution,
        input.workflowType,
        input.timeout,
        input.timeoutUnit,
        input.resultClass,
        input.resultType
      )
      try {
        val javaOutput = javaInterceptor.getResult(javaInput)
        KWorkflowClientCallsInterceptor.GetResultOutput(javaOutput.result)
      } catch (e: TimeoutException) {
        throw e
      }
    }
  }

  override suspend fun <R> getResultAsync(input: KWorkflowClientCallsInterceptor.GetResultInput<R>): KWorkflowClientCallsInterceptor.GetResultAsyncOutput<R> {
    val javaInput = WorkflowClientCallsInterceptor.GetResultInput(
      input.workflowExecution,
      input.workflowType,
      input.timeout,
      input.timeoutUnit,
      input.resultClass,
      input.resultType
    )
    val javaOutput = javaInterceptor.getResultAsync(javaInput)
    val resultFlow: Flow<R> = flow {
      emit(javaOutput.result.await())
    }.flowOn(Dispatchers.IO)
    return KWorkflowClientCallsInterceptor.GetResultAsyncOutput(resultFlow)
  }

  override suspend fun <R> query(input: KWorkflowClientCallsInterceptor.QueryInput<R>): KWorkflowClientCallsInterceptor.QueryOutput<R> {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.QueryInput(
        input.workflowExecution,
        input.queryType,
        input.header,
        input.arguments,
        input.resultClass,
        input.resultType
      )
      val javaOutput = javaInterceptor.query(javaInput)
      KWorkflowClientCallsInterceptor.QueryOutput(
        javaOutput.queryRejectedStatus,
        javaOutput.result
      )
    }
  }

  override suspend fun <R> startUpdate(input: KWorkflowClientCallsInterceptor.StartUpdateInput<R>): WorkflowUpdateHandle<R> {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.StartUpdateInput(
        input.workflowExecution,
        input.workflowType,
        input.updateName,
        input.header,
        input.updateId,
        input.arguments,
        input.resultClass,
        input.resultType,
        input.firstExecutionRunId,
        input.waitPolicy
      )
      javaInterceptor.startUpdate(javaInput)
    }
  }

  override suspend fun <R> pollWorkflowUpdate(input: KWorkflowClientCallsInterceptor.PollWorkflowUpdateInput<R>): KWorkflowClientCallsInterceptor.PollWorkflowUpdateOutput<R> {
    val javaInput = WorkflowClientCallsInterceptor.PollWorkflowUpdateInput(
      input.workflowExecution,
      input.updateName,
      input.updateId,
      input.resultClass,
      input.resultType,
      input.timeout,
      input.timeoutUnit
    )
    val javaOutput = javaInterceptor.pollWorkflowUpdate(javaInput)
    val resultFlow: Flow<R> = flow {
      emit(javaOutput.result.await())
    }.flowOn(Dispatchers.IO)
    return KWorkflowClientCallsInterceptor.PollWorkflowUpdateOutput(resultFlow)
  }

  override suspend fun cancel(input: KWorkflowClientCallsInterceptor.CancelInput): KWorkflowClientCallsInterceptor.CancelOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.CancelInput(
        input.workflowExecution,
        input.firstExecutionRunId,
        input.reason
      )
      javaInterceptor.cancel(javaInput)
      KWorkflowClientCallsInterceptor.CancelOutput()
    }
  }

  override suspend fun terminate(input: KWorkflowClientCallsInterceptor.TerminateInput): KWorkflowClientCallsInterceptor.TerminateOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.TerminateInput(
        input.workflowExecution,
        input.firstExecutionRunId,
        input.reason,
        input.details
      )
      javaInterceptor.terminate(javaInput)
      KWorkflowClientCallsInterceptor.TerminateOutput()
    }
  }

  override suspend fun describe(input: KWorkflowClientCallsInterceptor.DescribeInput): KWorkflowClientCallsInterceptor.DescribeOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.DescribeWorkflowInput(input.workflowExecution)
      val javaOutput = javaInterceptor.describe(javaInput)
      KWorkflowClientCallsInterceptor.DescribeOutput(javaOutput.description)
    }
  }

  override suspend fun listWorkflowExecutions(input: KWorkflowClientCallsInterceptor.ListWorkflowExecutionsInput): KWorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput {
    val javaInput = WorkflowClientCallsInterceptor.ListWorkflowExecutionsInput(input.query, input.pageSize)
    val javaOutput = javaInterceptor.listWorkflowExecutions(javaInput)
    val executionsFlow = flow {
      val iterator = javaOutput.stream.iterator()
      while (iterator.hasNext()) {
        emit(iterator.next())
      }
    }.flowOn(Dispatchers.IO)
    return KWorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput(executionsFlow)
  }

  override suspend fun countWorkflows(input: KWorkflowClientCallsInterceptor.CountWorkflowsInput): KWorkflowClientCallsInterceptor.CountWorkflowOutput {
    return withContext(Dispatchers.IO) {
      val javaInput = WorkflowClientCallsInterceptor.CountWorkflowsInput(input.query)
      val javaOutput = javaInterceptor.countWorkflows(javaInput)
      KWorkflowClientCallsInterceptor.CountWorkflowOutput(javaOutput.count)
    }
  }
}

/**
 * Wraps a Kotlin [KWorkflowClientCallsInterceptor] to be used as a Java [WorkflowClientCallsInterceptor].
 *
 * Suspend Kotlin calls are wrapped with runBlocking.
 */
@InternalTemporalApi
internal class KotlinCallsInterceptorToJava(
  private val kotlinInterceptor: KWorkflowClientCallsInterceptor
) : WorkflowClientCallsInterceptorBase(NoOpJavaInterceptor) {

  override fun start(input: WorkflowClientCallsInterceptor.WorkflowStartInput): WorkflowClientCallsInterceptor.WorkflowStartOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.StartInput(
        input.workflowId,
        input.workflowType,
        input.header,
        input.arguments,
        input.options
      )
      val kotlinOutput = kotlinInterceptor.start(kotlinInput)
      WorkflowClientCallsInterceptor.WorkflowStartOutput(kotlinOutput.workflowExecution)
    }
  }

  override fun signal(input: WorkflowClientCallsInterceptor.WorkflowSignalInput): WorkflowClientCallsInterceptor.WorkflowSignalOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.SignalInput(
        input.workflowExecution,
        input.signalName,
        input.header,
        input.arguments
      )
      kotlinInterceptor.signal(kotlinInput)
      WorkflowClientCallsInterceptor.WorkflowSignalOutput()
    }
  }

  override fun signalWithStart(input: WorkflowClientCallsInterceptor.WorkflowSignalWithStartInput): WorkflowClientCallsInterceptor.WorkflowSignalWithStartOutput {
    return runBlocking {
      val kotlinStartInput = KWorkflowClientCallsInterceptor.StartInput(
        input.workflowStartInput.workflowId,
        input.workflowStartInput.workflowType,
        input.workflowStartInput.header,
        input.workflowStartInput.arguments,
        input.workflowStartInput.options
      )
      val kotlinInput = KWorkflowClientCallsInterceptor.SignalWithStartInput(
        kotlinStartInput,
        input.signalName,
        input.signalArguments
      )
      val kotlinOutput = kotlinInterceptor.signalWithStart(kotlinInput)
      WorkflowClientCallsInterceptor.WorkflowSignalWithStartOutput(
        WorkflowClientCallsInterceptor.WorkflowStartOutput(kotlinOutput.workflowStartOutput.workflowExecution)
      )
    }
  }

  override fun <R> updateWithStart(input: WorkflowClientCallsInterceptor.WorkflowUpdateWithStartInput<R>): WorkflowClientCallsInterceptor.WorkflowUpdateWithStartOutput<R> {
    return runBlocking {
      val kotlinStartInput = KWorkflowClientCallsInterceptor.StartInput(
        input.workflowStartInput.workflowId,
        input.workflowStartInput.workflowType,
        input.workflowStartInput.header,
        input.workflowStartInput.arguments,
        input.workflowStartInput.options
      )
      val kotlinUpdateInput = KWorkflowClientCallsInterceptor.StartUpdateInput(
        input.startUpdateInput.workflowExecution,
        input.startUpdateInput.workflowType,
        input.startUpdateInput.updateName,
        input.startUpdateInput.header,
        input.startUpdateInput.updateId,
        input.startUpdateInput.arguments,
        input.startUpdateInput.resultClass,
        input.startUpdateInput.resultType,
        input.startUpdateInput.firstExecutionRunId,
        input.startUpdateInput.waitPolicy
      )
      val kotlinInput = KWorkflowClientCallsInterceptor.UpdateWithStartInput(
        kotlinStartInput,
        kotlinUpdateInput
      )
      val kotlinOutput = kotlinInterceptor.updateWithStart(kotlinInput)
      WorkflowClientCallsInterceptor.WorkflowUpdateWithStartOutput(
        WorkflowClientCallsInterceptor.WorkflowStartOutput(kotlinOutput.workflowStartOutput.workflowExecution),
        kotlinOutput.updateHandle
      )
    }
  }

  override fun <R> getResult(input: WorkflowClientCallsInterceptor.GetResultInput<R>): WorkflowClientCallsInterceptor.GetResultOutput<R> {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.GetResultInput(
        input.workflowExecution,
        input.workflowType,
        input.timeout,
        input.timeoutUnit,
        input.resultClass,
        input.resultType
      )
      val kotlinOutput = kotlinInterceptor.getResult(kotlinInput)
      WorkflowClientCallsInterceptor.GetResultOutput(kotlinOutput.result)
    }
  }

  override fun <R> getResultAsync(input: WorkflowClientCallsInterceptor.GetResultInput<R>): WorkflowClientCallsInterceptor.GetResultAsyncOutput<R> {
    val kotlinInput = KWorkflowClientCallsInterceptor.GetResultInput(
      input.workflowExecution,
      input.workflowType,
      input.timeout,
      input.timeoutUnit,
      input.resultClass,
      input.resultType
    )
    val future = java.util.concurrent.CompletableFuture<R>()
    // Start a coroutine to get the result and complete the future
    kotlinx.coroutines.GlobalScope.launch(Dispatchers.IO) {
      try {
        val kotlinOutput = kotlinInterceptor.getResultAsync(kotlinInput)
        kotlinOutput.resultFlow.collect { result ->
          future.complete(result)
        }
      } catch (e: Exception) {
        future.completeExceptionally(e)
      }
    }
    return WorkflowClientCallsInterceptor.GetResultAsyncOutput(future)
  }

  override fun <R> query(input: WorkflowClientCallsInterceptor.QueryInput<R>): WorkflowClientCallsInterceptor.QueryOutput<R> {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.QueryInput(
        input.workflowExecution,
        input.queryType,
        input.header,
        input.arguments,
        input.resultClass,
        input.resultType
      )
      val kotlinOutput = kotlinInterceptor.query(kotlinInput)
      WorkflowClientCallsInterceptor.QueryOutput(
        kotlinOutput.queryRejectedStatus,
        kotlinOutput.result
      )
    }
  }

  override fun <R> startUpdate(input: WorkflowClientCallsInterceptor.StartUpdateInput<R>): WorkflowUpdateHandle<R> {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.StartUpdateInput(
        input.workflowExecution,
        input.workflowType,
        input.updateName,
        input.header,
        input.updateId,
        input.arguments,
        input.resultClass,
        input.resultType,
        input.firstExecutionRunId,
        input.waitPolicy
      )
      kotlinInterceptor.startUpdate(kotlinInput)
    }
  }

  override fun <R> pollWorkflowUpdate(input: WorkflowClientCallsInterceptor.PollWorkflowUpdateInput<R>): WorkflowClientCallsInterceptor.PollWorkflowUpdateOutput<R> {
    val kotlinInput = KWorkflowClientCallsInterceptor.PollWorkflowUpdateInput(
      input.workflowExecution,
      input.updateName,
      input.updateId,
      input.resultClass,
      input.resultType,
      input.timeout,
      input.timeoutUnit
    )
    val future = java.util.concurrent.CompletableFuture<R>()
    // Start a coroutine to poll and complete the future
    kotlinx.coroutines.GlobalScope.launch(Dispatchers.IO) {
      try {
        val kotlinOutput = kotlinInterceptor.pollWorkflowUpdate(kotlinInput)
        kotlinOutput.resultFlow.collect { result ->
          future.complete(result)
        }
      } catch (e: Exception) {
        future.completeExceptionally(e)
      }
    }
    return WorkflowClientCallsInterceptor.PollWorkflowUpdateOutput(future)
  }

  override fun cancel(input: WorkflowClientCallsInterceptor.CancelInput): WorkflowClientCallsInterceptor.CancelOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.CancelInput(
        input.workflowExecution,
        input.firstExecutionRunId,
        input.reason
      )
      kotlinInterceptor.cancel(kotlinInput)
      WorkflowClientCallsInterceptor.CancelOutput()
    }
  }

  override fun terminate(input: WorkflowClientCallsInterceptor.TerminateInput): WorkflowClientCallsInterceptor.TerminateOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.TerminateInput(
        input.workflowExecution,
        input.firstExecutionRunId,
        input.reason,
        input.details
      )
      kotlinInterceptor.terminate(kotlinInput)
      WorkflowClientCallsInterceptor.TerminateOutput()
    }
  }

  override fun describe(input: WorkflowClientCallsInterceptor.DescribeWorkflowInput): WorkflowClientCallsInterceptor.DescribeWorkflowOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.DescribeInput(input.workflowExecution)
      val kotlinOutput = kotlinInterceptor.describe(kotlinInput)
      WorkflowClientCallsInterceptor.DescribeWorkflowOutput(kotlinOutput.description)
    }
  }

  override fun listWorkflowExecutions(input: WorkflowClientCallsInterceptor.ListWorkflowExecutionsInput): WorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.ListWorkflowExecutionsInput(input.query, input.pageSize)
      val kotlinOutput = kotlinInterceptor.listWorkflowExecutions(kotlinInput)
      // Convert Flow to Stream
      val list = mutableListOf<io.temporal.client.WorkflowExecutionMetadata>()
      kotlinOutput.executionsFlow.collect { list.add(it) }
      WorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput(list.stream())
    }
  }

  override fun countWorkflows(input: WorkflowClientCallsInterceptor.CountWorkflowsInput): WorkflowClientCallsInterceptor.CountWorkflowOutput {
    return runBlocking {
      val kotlinInput = KWorkflowClientCallsInterceptor.CountWorkflowsInput(input.query)
      val kotlinOutput = kotlinInterceptor.countWorkflows(kotlinInput)
      WorkflowClientCallsInterceptor.CountWorkflowOutput(kotlinOutput.count)
    }
  }

  companion object {
    /**
     * A no-op interceptor used as a placeholder for the base class.
     * The actual interception is done by delegating to the Kotlin interceptor.
     */
    private val NoOpJavaInterceptor = object : WorkflowClientCallsInterceptor {
      override fun start(input: WorkflowClientCallsInterceptor.WorkflowStartInput): WorkflowClientCallsInterceptor.WorkflowStartOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun signal(input: WorkflowClientCallsInterceptor.WorkflowSignalInput): WorkflowClientCallsInterceptor.WorkflowSignalOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun signalWithStart(input: WorkflowClientCallsInterceptor.WorkflowSignalWithStartInput): WorkflowClientCallsInterceptor.WorkflowSignalWithStartOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> updateWithStart(input: WorkflowClientCallsInterceptor.WorkflowUpdateWithStartInput<R>): WorkflowClientCallsInterceptor.WorkflowUpdateWithStartOutput<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> getResult(input: WorkflowClientCallsInterceptor.GetResultInput<R>): WorkflowClientCallsInterceptor.GetResultOutput<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> getResultAsync(input: WorkflowClientCallsInterceptor.GetResultInput<R>): WorkflowClientCallsInterceptor.GetResultAsyncOutput<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> query(input: WorkflowClientCallsInterceptor.QueryInput<R>): WorkflowClientCallsInterceptor.QueryOutput<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> startUpdate(input: WorkflowClientCallsInterceptor.StartUpdateInput<R>): WorkflowUpdateHandle<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun <R> pollWorkflowUpdate(input: WorkflowClientCallsInterceptor.PollWorkflowUpdateInput<R>): WorkflowClientCallsInterceptor.PollWorkflowUpdateOutput<R> {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun cancel(input: WorkflowClientCallsInterceptor.CancelInput): WorkflowClientCallsInterceptor.CancelOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun terminate(input: WorkflowClientCallsInterceptor.TerminateInput): WorkflowClientCallsInterceptor.TerminateOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun describe(input: WorkflowClientCallsInterceptor.DescribeWorkflowInput): WorkflowClientCallsInterceptor.DescribeWorkflowOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun listWorkflowExecutions(input: WorkflowClientCallsInterceptor.ListWorkflowExecutionsInput): WorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }

      override fun countWorkflows(input: WorkflowClientCallsInterceptor.CountWorkflowsInput): WorkflowClientCallsInterceptor.CountWorkflowOutput {
        throw UnsupportedOperationException("Should not be called directly")
      }
    }
  }
}
