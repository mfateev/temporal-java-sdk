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

package io.temporal.kotlin.internal.interceptor

import io.temporal.kotlin.interceptor.KQueryInput
import io.temporal.kotlin.interceptor.KQueryOutput
import io.temporal.kotlin.interceptor.KSignalInput
import io.temporal.kotlin.interceptor.KUpdateInput
import io.temporal.kotlin.interceptor.KUpdateOutput
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInput
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkflowOutput
import io.temporal.kotlin.internal.InternalTemporalApi

/**
 * Root workflow inbound calls interceptor that performs the actual workflow execution.
 *
 * This is the final interceptor in the chain - it doesn't delegate to a next interceptor
 * but instead executes the actual workflow logic through the provided executor.
 */
@InternalTemporalApi
internal class RootWorkflowInboundCallsInterceptor(
  private val executor: WorkflowExecutor
) : KWorkflowInboundCallsInterceptor {

  private var outboundInterceptor: KWorkflowOutboundCallsInterceptor? = null

  override suspend fun init(outboundCalls: KWorkflowOutboundCallsInterceptor) {
    this.outboundInterceptor = outboundCalls
    // Initialize the executor with the outbound interceptor
    executor.setOutboundInterceptor(outboundCalls)
  }

  override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
    return executor.executeWorkflow(input)
  }

  override suspend fun handleSignal(input: KSignalInput) {
    executor.handleSignal(input)
  }

  override fun handleQuery(input: KQueryInput): KQueryOutput {
    return executor.handleQuery(input)
  }

  override fun validateUpdate(input: KUpdateInput) {
    executor.validateUpdate(input)
  }

  override suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput {
    return executor.executeUpdate(input)
  }
}

/**
 * Interface for the actual workflow execution.
 * This is implemented by the workflow runtime to execute the workflow logic.
 */
@InternalTemporalApi
internal interface WorkflowExecutor {
  fun setOutboundInterceptor(outboundCalls: KWorkflowOutboundCallsInterceptor)
  suspend fun executeWorkflow(input: KWorkflowInput): KWorkflowOutput
  suspend fun handleSignal(input: KSignalInput)
  fun handleQuery(input: KQueryInput): KQueryOutput
  fun validateUpdate(input: KUpdateInput)
  suspend fun executeUpdate(input: KUpdateInput): KUpdateOutput
}
