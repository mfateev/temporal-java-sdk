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

import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import java.time.Duration

/**
 * Policies of a schedule.
 *
 * Example:
 * ```kotlin
 * KSchedulePolicy(
 *     overlap = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP,
 *     catchupWindow = Duration.ofMinutes(5),
 *     pauseOnFailure = true
 * )
 * ```
 *
 * @property overlap Policy for what happens when an action is started while another is running.
 *           Default is SKIP.
 * @property catchupWindow Amount of time in the past to execute missed actions after
 *           the Temporal server is unavailable.
 * @property pauseOnFailure Whether to pause the schedule if an action fails or times out.
 */
public data class KSchedulePolicy(
  val overlap: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP,
  val catchupWindow: Duration? = null,
  val pauseOnFailure: Boolean = false
)
