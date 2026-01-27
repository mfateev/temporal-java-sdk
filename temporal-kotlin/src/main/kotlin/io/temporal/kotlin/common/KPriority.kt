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

package io.temporal.kotlin.common

/**
 * Priority contains metadata that controls the relative ordering of task processing when tasks are
 * backed up in a queue. The affected queues depend on the server version.
 *
 * Priority is attached to workflows and activities. By default, activities and child workflows
 * inherit Priority from the workflow that created them, but may override fields when an activity is
 * started or modified.
 *
 * For all fields, the field not present or equal to zero/empty string means to inherit the value
 * from the calling workflow, or if there is no calling workflow, then use the default value.
 *
 * Example:
 * ```kotlin
 * val priority = KPriority(
 *     priorityKey = 1,  // Higher priority (1 is highest)
 *     fairnessKey = "tenant-123",
 *     fairnessWeight = 2.0f
 * )
 * ```
 *
 * @property priorityKey A priority key is a positive integer from 1 to n, where smaller integers
 *           correspond to higher priorities (tasks run sooner). The maximum priority value
 *           (minimum priority) is determined by server configuration, and defaults to 5.
 *           The default value when unset or 0 is calculated by (min+max)/2. With the default
 *           max of 5, and min of 1, that comes out to 3.
 * @property fairnessKey A short string used as a key for a fairness balancing mechanism.
 *           It may correspond to a tenant id, or to a fixed string like "high" or "low".
 *           The default is the empty string. Fairness keys are limited to 64 bytes.
 * @property fairnessWeight The fairness weight for a task. Weight values are clamped to the
 *           range [0.001, 1000]. The default weight of 1.0 will be used if not specified.
 */
public data class KPriority(
  val priorityKey: Int = 0,
  val fairnessKey: String? = null,
  val fairnessWeight: Float = 0f
) {
  public companion object {
    /**
     * Default priority instance with all default values.
     */
    @JvmField
    public val DEFAULT: KPriority = KPriority()
  }
}
