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

import io.temporal.client.schedules.ScheduleListAction
import io.temporal.client.schedules.ScheduleListActionStartWorkflow

/**
 * Base class for an action a listed schedule can take.
 */
public sealed class KScheduleListAction {
  public companion object {
    /**
     * Create a KScheduleListAction from a Java SDK ScheduleListAction.
     */
    @JvmStatic
    public fun fromJava(action: ScheduleListAction): KScheduleListAction = when (action) {
      is ScheduleListActionStartWorkflow -> KScheduleListActionStartWorkflow.fromJava(action)
      else -> throw IllegalArgumentException("Unknown schedule list action type: ${action::class.java}")
    }
  }
}

/**
 * Action to start a workflow from a listed schedule.
 *
 * @property workflow The workflow type name.
 */
public data class KScheduleListActionStartWorkflow(
  val workflow: String
) : KScheduleListAction() {
  public companion object {
    /**
     * Create a KScheduleListActionStartWorkflow from a Java SDK ScheduleListActionStartWorkflow.
     */
    @JvmStatic
    public fun fromJava(action: ScheduleListActionStartWorkflow): KScheduleListActionStartWorkflow =
      KScheduleListActionStartWorkflow(
        workflow = action.workflow
      )
  }
}
