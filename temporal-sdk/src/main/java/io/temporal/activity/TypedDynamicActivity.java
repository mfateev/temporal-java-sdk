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

package io.temporal.activity;

import io.temporal.common.converter.EncodedValues;

/**
 * A dynamic activity handler for a specific activity type.
 *
 * <p>Unlike {@link DynamicActivity} which handles all unregistered activity types as a fallback,
 * TypedDynamicActivity handles a specific activity type and is registered alongside regular
 * activity implementations. Multiple TypedDynamicActivity instances can be registered on the same
 * worker, each handling a different activity type.
 *
 * <p>This is useful for:
 *
 * <ul>
 *   <li>Kotlin suspend activity support
 *   <li>Custom activity invocation strategies
 *   <li>Language-specific activity handling
 * </ul>
 *
 * <p>Use {@link Activity#getExecutionContext()} to query information about the activity execution.
 *
 * @see DynamicActivity
 */
public interface TypedDynamicActivity {

  /**
   * Execute the activity.
   *
   * @param args Encoded activity arguments from the workflow
   * @return Activity result (will be serialized back to the workflow)
   */
  Object execute(EncodedValues args);

  /**
   * Returns the activity type name this handler handles.
   *
   * @return The activity type name
   */
  String getActivityType();
}
