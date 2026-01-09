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

import io.temporal.activity.ActivityExecutionContext
import io.temporal.kotlin.interceptor.KActivityExecutionInput
import io.temporal.kotlin.interceptor.KActivityExecutionOutput
import io.temporal.kotlin.interceptor.KActivityInboundCallsInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi

/**
 * Root activity inbound calls interceptor that performs the actual activity execution.
 *
 * This is the final interceptor in the chain - it doesn't delegate to a next interceptor
 * but instead executes the actual activity logic through the provided executor.
 */
@InternalTemporalApi
internal class RootActivityInboundCallsInterceptor(
  private val executor: ActivityExecutor
) : KActivityInboundCallsInterceptor {

  private var context: ActivityExecutionContext? = null

  override fun init(context: ActivityExecutionContext) {
    this.context = context
    executor.setContext(context)
  }

  override suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput {
    return executor.executeActivity(input)
  }
}

/**
 * Interface for the actual activity execution.
 * This is implemented by the activity runtime to execute the activity logic.
 */
@InternalTemporalApi
internal interface ActivityExecutor {
  fun setContext(context: ActivityExecutionContext)
  suspend fun executeActivity(input: KActivityExecutionInput): KActivityExecutionOutput
}
