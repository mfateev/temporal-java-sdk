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

import io.temporal.client.schedules.Schedule

/**
 * A schedule for periodically running an action.
 *
 * Example:
 * ```kotlin
 * val schedule = KSchedule(
 *     action = KScheduleActionStartWorkflow(
 *         workflowType = "MyWorkflow",
 *         options = WorkflowOptions.newBuilder()
 *             .setWorkflowId("scheduled-workflow")
 *             .setTaskQueue("my-task-queue")
 *             .build()
 *     ),
 *     spec = KScheduleSpec(
 *         intervals = listOf(KScheduleIntervalSpec(every = Duration.ofHours(1)))
 *     ),
 *     policy = KSchedulePolicy(
 *         overlap = ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP
 *     ),
 *     state = KScheduleState(
 *         paused = false
 *     )
 * )
 * ```
 *
 * @property action The action to take when the schedule fires. Required.
 * @property spec When the schedule should fire. Required.
 * @property policy Policies for the schedule.
 * @property state State of the schedule.
 */
public data class KSchedule(
  val action: KScheduleAction,
  val spec: KScheduleSpec,
  val policy: KSchedulePolicy? = null,
  val state: KScheduleState? = null
) {
  /**
   * Converts this KSchedule to a Java SDK Schedule.
   */
  internal fun toJava(): Schedule {
    val builder = Schedule.newBuilder()
      .setAction(action.toJava())
      .setSpec(spec.toJava())
    policy?.let { builder.setPolicy(it.toJava()) }
    state?.let { builder.setState(it.toJava()) }
    return builder.build()
  }

  public companion object {
    /**
     * Create a KSchedule from a Java SDK Schedule.
     */
    @JvmStatic
    public fun fromJava(schedule: Schedule): KSchedule = KSchedule(
      action = KScheduleAction.fromJava(schedule.action),
      spec = KScheduleSpec.fromJava(schedule.spec),
      policy = schedule.policy?.let { KSchedulePolicy.fromJava(it) },
      state = schedule.state?.let { KScheduleState.fromJava(it) }
    )
  }
}
