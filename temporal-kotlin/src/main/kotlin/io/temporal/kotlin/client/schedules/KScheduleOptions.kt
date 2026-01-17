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

package io.temporal.kotlin.client.schedules

import io.temporal.client.schedules.ScheduleOptions
import io.temporal.common.SearchAttributes

/**
 * Options for creating a schedule.
 *
 * Example:
 * ```kotlin
 * KScheduleOptions(
 *     triggerImmediately = true,
 *     memo = mapOf("key" to "value"),
 *     searchAttributes = SearchAttributes.newBuilder()
 *         .set(SearchAttributeKey.forText("CustomAttribute"), "value")
 *         .build()
 * )
 * ```
 *
 * @property triggerImmediately If true, the schedule will be triggered immediately upon creation.
 * @property backfills Time periods to take actions on as if that time passed right now.
 * @property memo Memo for the schedule. Values cannot be null.
 * @property searchAttributes Search attributes for the schedule.
 */
public data class KScheduleOptions(
  val triggerImmediately: Boolean = false,
  val backfills: List<KScheduleBackfill> = emptyList(),
  val memo: Map<String, Any>? = null,
  val searchAttributes: SearchAttributes? = null
) {
  /**
   * Converts this KScheduleOptions to a Java SDK ScheduleOptions.
   */
  internal fun toJava(): ScheduleOptions {
    val builder = ScheduleOptions.newBuilder()
      .setTriggerImmediately(triggerImmediately)
    if (backfills.isNotEmpty()) {
      builder.setBackfills(backfills.map { it.toJava() })
    }
    memo?.let { builder.setMemo(it) }
    searchAttributes?.let { builder.setTypedSearchAttributes(it) }
    return builder.build()
  }

  public companion object {
    /**
     * Default schedule options.
     */
    @JvmStatic
    public val DEFAULT: KScheduleOptions = KScheduleOptions()
  }
}
