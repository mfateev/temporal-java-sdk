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
import io.temporal.client.schedules.ScheduleHandle
import io.temporal.client.schedules.ScheduleUpdateInput
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Handle for interacting with a schedule.
 *
 * Example:
 * ```kotlin
 * val handle = client.scheduleHandle("my-schedule-id")
 *
 * // Describe the schedule
 * val description = handle.describe()
 * println("Schedule is paused: ${description.schedule.state?.paused}")
 *
 * // Pause the schedule
 * handle.pause("Pausing for maintenance")
 *
 * // Trigger an immediate action
 * handle.trigger()
 *
 * // Unpause the schedule
 * handle.unpause("Maintenance complete")
 *
 * // Update the schedule
 * handle.update { input ->
 *     val currentSchedule = input.description.schedule
 *     KScheduleUpdate(
 *         schedule = currentSchedule.copy(
 *             state = currentSchedule.state?.copy(paused = false)
 *         )
 *     )
 * }
 *
 * // Delete the schedule
 * handle.delete()
 * ```
 */
public class KScheduleHandle internal constructor(
  private val javaHandle: ScheduleHandle
) {
  /**
   * Get this schedule's ID.
   */
  public val id: String
    get() = javaHandle.id

  /**
   * Backfill this schedule by going through the specified time periods as if they passed right now.
   *
   * @param backfills Backfill requests to run.
   */
  public suspend fun backfill(backfills: List<KScheduleBackfill>) {
    withContext(Dispatchers.IO) {
      javaHandle.backfill(backfills.map { it.toJava() })
    }
  }

  /**
   * Delete this schedule.
   */
  public suspend fun delete() {
    withContext(Dispatchers.IO) {
      javaHandle.delete()
    }
  }

  /**
   * Fetch this schedule's description.
   *
   * @return Description of the schedule.
   */
  public suspend fun describe(): KScheduleDescription {
    return withContext(Dispatchers.IO) {
      KScheduleDescription.fromJava(javaHandle.describe())
    }
  }

  /**
   * Pause this schedule.
   *
   * @param note Note to set on the schedule state.
   */
  public suspend fun pause(note: String) {
    withContext(Dispatchers.IO) {
      javaHandle.pause(note)
    }
  }

  /**
   * Pause this schedule.
   */
  public suspend fun pause() {
    withContext(Dispatchers.IO) {
      javaHandle.pause()
    }
  }

  /**
   * Trigger an action on this schedule to happen immediately.
   *
   * @param overlapPolicy Override the schedule overlap policy.
   */
  public suspend fun trigger(overlapPolicy: ScheduleOverlapPolicy) {
    withContext(Dispatchers.IO) {
      javaHandle.trigger(overlapPolicy)
    }
  }

  /**
   * Trigger an action on this schedule to happen immediately.
   */
  public suspend fun trigger() {
    withContext(Dispatchers.IO) {
      javaHandle.trigger()
    }
  }

  /**
   * Unpause this schedule.
   *
   * @param note Note to set on the schedule state.
   */
  public suspend fun unpause(note: String) {
    withContext(Dispatchers.IO) {
      javaHandle.unpause(note)
    }
  }

  /**
   * Unpause this schedule.
   */
  public suspend fun unpause() {
    withContext(Dispatchers.IO) {
      javaHandle.unpause()
    }
  }

  /**
   * Update this schedule.
   *
   * This is done via a callback which can be called multiple times in case of conflict.
   *
   * @param updater Callback to invoke with the current update input. The result can be null to
   *        signify no update to perform, or a schedule update instance with a schedule to perform
   *        an update.
   */
  public suspend fun update(updater: (KScheduleUpdateInput) -> KScheduleUpdate?) {
    withContext(Dispatchers.IO) {
      javaHandle.update { javaInput: ScheduleUpdateInput ->
        val kotlinInput = KScheduleUpdateInput(
          description = KScheduleDescription.fromJava(javaInput.description)
        )
        updater(kotlinInput)?.toJava()
      }
    }
  }
}
