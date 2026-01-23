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

import io.temporal.activity.ActivityExecutionContext
import io.temporal.common.interceptors.Header

/**
 * Input for activity execution.
 *
 * @property header the activity header containing metadata
 * @property arguments the arguments passed to the activity method
 */
public data class KActivityExecutionInput(
  val header: Header,
  val arguments: Array<Any?>
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false
    other as KActivityExecutionInput
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
 * Output from activity execution.
 *
 * @property result the result of the activity execution
 */
public data class KActivityExecutionOutput(val result: Any?)

/**
 * Intercepts inbound calls to activity execution.
 *
 * An instance should be created in [KWorkerInterceptor.interceptActivity].
 *
 * This is a suspend function interface to support suspend activities that may perform
 * async operations like I/O.
 *
 * Prefer extending [KActivityInboundCallsInterceptorBase] and overriding only the methods
 * you need instead of implementing this interface directly.
 *
 * The implementation must forward all the calls to the next interceptor.
 *
 * Example:
 * ```kotlin
 * class LoggingActivityInterceptor(
 *     next: KActivityInboundCallsInterceptor
 * ) : KActivityInboundCallsInterceptorBase(next) {
 *     private val log = LoggerFactory.getLogger(LoggingActivityInterceptor::class.java)
 *
 *     override suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput {
 *         val info = KActivityContext.current.info
 *
 *         log.info("Activity ${info.activityType} started")
 *         val startTime = System.currentTimeMillis()
 *
 *         return try {
 *             val result = next.execute(input)
 *             val duration = System.currentTimeMillis() - startTime
 *             log.info("Activity ${info.activityType} completed in ${duration}ms")
 *             result
 *         } catch (e: Exception) {
 *             val duration = System.currentTimeMillis() - startTime
 *             log.error("Activity ${info.activityType} failed after ${duration}ms", e)
 *             throw e
 *         }
 *     }
 * }
 * ```
 *
 * @see KWorkerInterceptor.interceptActivity
 * @see KActivityInboundCallsInterceptorBase
 */
public interface KActivityInboundCallsInterceptor {
  /**
   * Called when activity is initialized. Provides access to the activity execution context.
   *
   * @param context the activity execution context
   */
  public fun init(context: ActivityExecutionContext)

  /**
   * Called when activity method is invoked.
   *
   * This is a suspend function to support suspend activities that may perform
   * async operations like I/O.
   *
   * @param input the activity execution input containing header and arguments
   * @return the activity execution output containing the result
   */
  public suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput
}

/**
 * Base implementation that forwards all calls to the next interceptor.
 *
 * Extend this class and override only the methods you need.
 *
 * @param next the next interceptor in the chain
 */
public open class KActivityInboundCallsInterceptorBase(
  protected val next: KActivityInboundCallsInterceptor
) : KActivityInboundCallsInterceptor {

  override fun init(context: ActivityExecutionContext) {
    next.init(context)
  }

  override suspend fun execute(input: KActivityExecutionInput): KActivityExecutionOutput {
    return next.execute(input)
  }
}
