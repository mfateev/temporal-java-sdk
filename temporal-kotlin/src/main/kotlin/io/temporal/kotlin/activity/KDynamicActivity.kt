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

import io.temporal.kotlin.common.KEncodedValues

/**
 * Interface for implementing dynamic activities that can handle any activity type.
 *
 * Dynamic activities receive all arguments as [KEncodedValues] and can return any result.
 * The activity type is available via `Activity.getExecutionContext().info.activityType`.
 *
 * Dynamic activities are useful when:
 * - Activity types are determined at runtime
 * - You need a single implementation to handle multiple activity types
 * - Building generic activity routing/dispatching systems
 *
 * Example:
 * ```kotlin
 * class GenericActivity : KDynamicActivity {
 *     override fun execute(args: KEncodedValues): Any? {
 *         val activityType = Activity.getExecutionContext().info.activityType
 *         val input = args.get<String>(0)
 *
 *         return "$activityType processed: $input"
 *     }
 * }
 * ```
 *
 * To register a dynamic activity:
 * ```kotlin
 * val worker = KWorker(
 *     client,
 *     KWorkerOptions(
 *         taskQueue = "my-task-queue",
 *         dynamicActivity = GenericActivity()
 *     )
 * )
 * ```
 */
public interface KDynamicActivity {
  /**
   * Execute the activity with the provided arguments.
   *
   * This method is called when an activity task is received for any activity type
   * that doesn't have a specifically registered implementation.
   *
   * @param args The activity arguments as [KEncodedValues]
   * @return The activity result (must be serializable)
   */
  fun execute(args: KEncodedValues): Any?
}
