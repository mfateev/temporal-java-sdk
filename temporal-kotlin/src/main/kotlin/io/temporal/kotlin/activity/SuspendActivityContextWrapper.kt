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

package io.temporal.kotlin.activity

/**
 * Implementation of [KActivityContext] for suspend activities.
 *
 * This wrapper uses the [SuspendActivityExecutionContext] which contains both
 * the Java SDK's [io.temporal.activity.ActivityExecutionContext] and the
 * [io.temporal.activity.ManualActivityCompletionClient] for heartbeating.
 *
 * The key difference from [KActivityContextImpl] is that heartbeat calls go through
 * the completion client rather than the execution context directly, as suspend activities
 * use manual completion mode.
 */
internal class SuspendActivityContextWrapper(
  private val suspendContext: SuspendActivityExecutionContext
) : KActivityContext {

  private val javaContext = suspendContext.executionContext

  override val info: KActivityInfo
    get() = KActivityInfoImpl(javaContext.info)

  override fun heartbeat(details: Any?) {
    // Suspend activities must use the completion client for heartbeating
    // because they operate in manual completion mode
    suspendContext.completionClient.recordHeartbeat(details)
  }

  override fun <T> heartbeatDetails(detailsClass: Class<T>): T? {
    return javaContext.getHeartbeatDetails(detailsClass).orElse(null)
  }

  override val taskToken: ByteArray
    get() = javaContext.taskToken

  override fun doNotCompleteOnReturn() {
    javaContext.doNotCompleteOnReturn()
  }

  override val isDoNotCompleteOnReturn: Boolean
    get() = javaContext.isDoNotCompleteOnReturn
}
