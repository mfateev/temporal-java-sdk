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
 * Intercepts calls to the WorkflowClient related to the lifecycle of a Workflow.
 *
 * Prefer extending [KWorkflowClientInterceptorBase] and overriding only the methods you need
 * instead of implementing this interface directly. [KWorkflowClientInterceptorBase] provides
 * correct default implementations to all the methods of this interface.
 *
 * Example:
 * ```kotlin
 * class LoggingClientInterceptor : KWorkflowClientInterceptorBase() {
 *     override fun workflowClientCallsInterceptor(
 *         next: KWorkflowClientCallsInterceptor
 *     ): KWorkflowClientCallsInterceptor {
 *         return object : KWorkflowClientCallsInterceptorBase(next) {
 *             override suspend fun start(input: KWorkflowClientCallsInterceptor.StartInput): KWorkflowClientCallsInterceptor.StartOutput {
 *                 println("Starting workflow: ${input.workflowType}")
 *                 return next.start(input)
 *             }
 *         }
 *     }
 * }
 * ```
 *
 * @see KWorkflowClientInterceptorBase
 * @see KWorkflowClientCallsInterceptor
 */
public interface KWorkflowClientInterceptor {
  /**
   * Called once during creation of WorkflowClient to create a chain of Client Workflow Interceptors.
   *
   * @param next next workflow client interceptor in the chain of interceptors
   * @return new interceptor that should decorate calls to [next]
   */
  public fun workflowClientCallsInterceptor(
    next: KWorkflowClientCallsInterceptor
  ): KWorkflowClientCallsInterceptor
}

/**
 * Base implementation that passes through all calls.
 *
 * Extend this class and override only the methods you need.
 *
 * Example:
 * ```kotlin
 * class MetricsClientInterceptor : KWorkflowClientInterceptorBase() {
 *     override fun workflowClientCallsInterceptor(
 *         next: KWorkflowClientCallsInterceptor
 *     ): KWorkflowClientCallsInterceptor {
 *         return MetricsCallsInterceptor(next)
 *     }
 * }
 * ```
 */
public open class KWorkflowClientInterceptorBase : KWorkflowClientInterceptor {
  override fun workflowClientCallsInterceptor(
    next: KWorkflowClientCallsInterceptor
  ): KWorkflowClientCallsInterceptor = next
}
