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

import io.temporal.kotlin.interceptor.KActivityInboundCallsInterceptor
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.kotlin.interceptor.KWorkflowInboundCallsInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi

/**
 * Utility class for building interceptor chains.
 */
@InternalTemporalApi
internal object InterceptorChain {

  /**
   * Builds a workflow inbound calls interceptor chain.
   *
   * The interceptors are chained in order, with each interceptor wrapping the next.
   * The root interceptor (which performs actual execution) is at the end of the chain.
   *
   * @param interceptors the list of worker interceptors
   * @param rootInterceptor the root interceptor that performs actual execution
   * @return the head of the interceptor chain
   */
  fun buildWorkflowInboundChain(
    interceptors: List<KWorkerInterceptor>,
    rootInterceptor: KWorkflowInboundCallsInterceptor
  ): KWorkflowInboundCallsInterceptor {
    var current = rootInterceptor
    // Build chain in reverse order so first interceptor is called first
    for (interceptor in interceptors.reversed()) {
      current = interceptor.interceptWorkflow(current)
    }
    return current
  }

  /**
   * Builds an activity inbound calls interceptor chain.
   *
   * The interceptors are chained in order, with each interceptor wrapping the next.
   * The root interceptor (which performs actual execution) is at the end of the chain.
   *
   * @param interceptors the list of worker interceptors
   * @param rootInterceptor the root interceptor that performs actual execution
   * @return the head of the interceptor chain
   */
  fun buildActivityInboundChain(
    interceptors: List<KWorkerInterceptor>,
    rootInterceptor: KActivityInboundCallsInterceptor
  ): KActivityInboundCallsInterceptor {
    var current = rootInterceptor
    // Build chain in reverse order so first interceptor is called first
    for (interceptor in interceptors.reversed()) {
      current = interceptor.interceptActivity(current)
    }
    return current
  }
}
