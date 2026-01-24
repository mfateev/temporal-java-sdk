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

package io.temporal.kotlin.internal.activity

import io.temporal.activity.Activity
import io.temporal.activity.DynamicActivity
import io.temporal.common.converter.EncodedValues
import io.temporal.failure.ApplicationFailure
import io.temporal.kotlin.common.KEncodedValues

/**
 * Dynamic activity handler that routes activity calls to a [KActivityRegistry].
 *
 * This handler is designed for testing scenarios where activity implementations
 * (including mocks) need to be registered at runtime. It implements [DynamicActivity]
 * which acts as a catch-all for any activity type not handled by other registrations.
 *
 * Note: For production use, prefer [KWorker.registerActivitiesImplementations] which uses TypedDynamicActivity
 * for better performance and type safety.
 *
 * Usage in tests:
 * ```kotlin
 * val registry = KActivityRegistry()
 * val handler = KDynamicActivityHandler(registry)
 * worker.registerActivitiesImplementations(handler)
 *
 * // Later, register mock implementations
 * registry.registerMockImplementation(mockActivity)
 * ```
 */
public class KDynamicActivityHandler(
  private val registry: KActivityRegistry
) : DynamicActivity {

  override fun execute(args: EncodedValues): Any? {
    val activityType = Activity.getExecutionContext().info.activityType

    // First check if the activity is registered in the registry
    if (registry.hasActivity(activityType)) {
      // Decode arguments based on registered method signature
      val decodedArgs = decodeArguments(activityType, args)
      return registry.execute(activityType, decodedArgs)
    }

    // Check if there's a dynamic fallback handler
    val fallback = registry.dynamicActivityFallback
    if (fallback != null) {
      return fallback.execute(KEncodedValues(args))
    }

    // No handler found - throw non-retryable failure for fast test feedback
    throw ApplicationFailure.newNonRetryableFailure(
      "Unknown activity type: $activityType. Known types: ${registry.getRegisteredTypes()}",
      "UNKNOWN_ACTIVITY_TYPE"
    )
  }

  private fun decodeArguments(activityType: String, args: EncodedValues): Array<Any?> {
    // For now, we pass the raw EncodedValues and let the registry handle decoding
    // This is a simplified implementation - the registry's execute method
    // expects decoded arguments, so we need to extract them from EncodedValues
    val count = args.size
    if (count == 0) {
      return emptyArray()
    }

    // Extract arguments as Object array - the types will be inferred at runtime
    return Array(count) { index ->
      args.get(index, Any::class.java)
    }
  }
}
