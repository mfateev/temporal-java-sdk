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

/**
 * Intercepts workflow and activity executions.
 *
 * Prefer extending [KWorkerInterceptorBase] and overriding only the methods you need
 * instead of implementing this interface directly. [KWorkerInterceptorBase] provides
 * correct default implementations to all the methods of this interface.
 *
 * Example:
 * ```kotlin
 * class CustomWorkerInterceptor : KWorkerInterceptorBase() {
 *     override fun interceptWorkflow(
 *         next: KWorkflowInboundCallsInterceptor
 *     ): KWorkflowInboundCallsInterceptor {
 *         return object : KWorkflowInboundCallsInterceptorBase(next) {
 *             override suspend fun execute(input: KWorkflowInput): KWorkflowOutput {
 *                 println("Workflow starting")
 *                 return next.execute(input)
 *             }
 *         }
 *     }
 * }
 * ```
 *
 * @see KWorkerInterceptorBase
 */
public interface KWorkerInterceptor {
  /**
   * Called when a workflow is instantiated. May create a [KWorkflowInboundCallsInterceptor].
   * The returned interceptor must forward all calls to [next].
   *
   * @param next an existing interceptor instance to be proxied by the interceptor created
   *     inside this method
   * @return an interceptor that passes all the calls to [next]
   */
  public fun interceptWorkflow(next: KWorkflowInboundCallsInterceptor): KWorkflowInboundCallsInterceptor

  /**
   * Called when an activity task is received. May create a [KActivityInboundCallsInterceptor].
   * The returned interceptor must forward all calls to [next].
   *
   * @param next an existing interceptor instance to be proxied by the interceptor created
   *     inside this method
   * @return an interceptor that passes all the calls to [next]
   */
  public fun interceptActivity(next: KActivityInboundCallsInterceptor): KActivityInboundCallsInterceptor
}

/**
 * Base implementation that passes through all calls.
 *
 * Extend this class and override only the methods you need.
 *
 * Example:
 * ```kotlin
 * class LoggingInterceptor : KWorkerInterceptorBase() {
 *     override fun interceptWorkflow(
 *         next: KWorkflowInboundCallsInterceptor
 *     ): KWorkflowInboundCallsInterceptor {
 *         return LoggingWorkflowInterceptor(next)
 *     }
 * }
 * ```
 */
public open class KWorkerInterceptorBase : KWorkerInterceptor {
  override fun interceptWorkflow(next: KWorkflowInboundCallsInterceptor): KWorkflowInboundCallsInterceptor = next
  override fun interceptActivity(next: KActivityInboundCallsInterceptor): KActivityInboundCallsInterceptor = next
}
