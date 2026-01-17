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

import io.temporal.client.schedules.ScheduleState

/**
 * State of a schedule.
 *
 * Example:
 * ```kotlin
 * // Create a paused schedule
 * KScheduleState(
 *     paused = true,
 *     note = "Paused for maintenance"
 * )
 *
 * // Create a schedule with limited actions
 * KScheduleState(
 *     limitedActions = true,
 *     remainingActions = 5
 * )
 * ```
 *
 * @property note Human-readable message for the schedule.
 * @property paused Whether this schedule is paused.
 * @property limitedActions If true, remaining actions will be decremented for each action taken.
 * @property remainingActions Actions remaining on this schedule. Once this hits 0, no further
 *           actions are scheduled automatically.
 */
public data class KScheduleState(
  val note: String? = null,
  val paused: Boolean = false,
  val limitedActions: Boolean = false,
  val remainingActions: Long = 0
) {
  /**
   * Converts this KScheduleState to a Java SDK ScheduleState.
   */
  internal fun toJava(): ScheduleState {
    val builder = ScheduleState.newBuilder()
      .setPaused(paused)
      .setLimitedAction(limitedActions)
      .setRemainingActions(remainingActions)
    note?.let { builder.setNote(it) }
    return builder.build()
  }

  public companion object {
    /**
     * Create a KScheduleState from a Java SDK ScheduleState.
     */
    @JvmStatic
    public fun fromJava(state: ScheduleState): KScheduleState = KScheduleState(
      note = state.note,
      paused = state.isPaused,
      limitedActions = state.isLimitedAction,
      remainingActions = state.remainingActions
    )
  }
}
