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

import io.temporal.client.schedules.ScheduleListDescription

/**
 * Description of a listed schedule.
 *
 * @property scheduleId The ID of the schedule.
 * @property schedule Schedule details.
 * @property info Information about the schedule.
 * @property searchAttributes Search attributes on the schedule.
 */
public class KScheduleListDescription internal constructor(
  public val scheduleId: String,
  public val schedule: KScheduleListSchedule,
  public val info: KScheduleListInfo,
  public val searchAttributes: Map<String, *>,
  internal val javaDescription: ScheduleListDescription
) {
  /**
   * Get a memo value by key.
   *
   * @param key The memo key.
   * @param valueClass The class of the memo value.
   * @return The memo value, or null if not found.
   */
  public fun <T> getMemo(key: String, valueClass: Class<T>): T? =
    javaDescription.getMemo(key, valueClass) as T?

  /**
   * Get a memo value by key.
   *
   * @param key The memo key.
   * @return The memo value, or null if not found.
   */
  public inline fun <reified T> getMemo(key: String): T? = getMemo(key, T::class.java)
}
