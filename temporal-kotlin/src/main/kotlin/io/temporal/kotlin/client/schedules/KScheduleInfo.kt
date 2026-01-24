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

import java.time.Instant

/**
 * Information about a schedule.
 *
 * @property numActions The number of actions taken by the schedule.
 * @property numActionsMissedCatchupWindow The number of actions skipped due to missing the catchup window.
 * @property numActionsSkippedOverlap The number of actions skipped due to overlap.
 * @property runningActions List of currently running actions.
 * @property recentActions List of the most recent actions, oldest first.
 * @property nextActionTimes List of the next scheduled action times.
 * @property createdAt Time the schedule was created.
 * @property lastUpdatedAt Last time the schedule was updated.
 */
public data class KScheduleInfo(
  val numActions: Long,
  val numActionsMissedCatchupWindow: Long,
  val numActionsSkippedOverlap: Long,
  val runningActions: List<KScheduleActionExecution>,
  val recentActions: List<KScheduleActionResult>,
  val nextActionTimes: List<Instant>,
  val createdAt: Instant,
  val lastUpdatedAt: Instant?
)
