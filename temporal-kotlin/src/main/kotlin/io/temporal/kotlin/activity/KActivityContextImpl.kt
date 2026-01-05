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

import io.temporal.activity.ActivityExecutionContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Implementation of [KActivityContext] that wraps Java SDK's [ActivityExecutionContext].
 *
 * This implementation supports both regular and suspend activities:
 * - For regular activities, uses the Java SDK context directly
 * - For suspend activities, delegates heartbeat to [SuspendActivityContext]
 */
internal class KActivityContextImpl(
  private val javaContext: ActivityExecutionContext
) : KActivityContext {

  override val info: KActivityInfo
    get() = KActivityInfoImpl(javaContext.info)

  override fun heartbeat(details: Any?) {
    javaContext.heartbeat(details)
  }

  override fun <T> getHeartbeatDetails(detailsClass: Class<T>): T? {
    return javaContext.getHeartbeatDetails(detailsClass).orElse(null)
  }

  override val taskToken: ByteArray
    get() = javaContext.taskToken

  override fun doNotCompleteOnReturn() {
    javaContext.doNotCompleteOnReturn()
  }

  override val isDoNotCompleteOnReturn: Boolean
    get() = javaContext.isDoNotCompleteOnReturn

  override fun logger(): Logger {
    return LoggerFactory.getLogger(javaContext.info.activityType)
  }

  override fun logger(name: String): Logger {
    return LoggerFactory.getLogger(name)
  }

  override fun logger(clazz: Class<*>): Logger {
    return LoggerFactory.getLogger(clazz)
  }
}
