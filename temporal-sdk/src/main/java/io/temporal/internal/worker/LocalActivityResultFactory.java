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

package io.temporal.internal.worker;

/**
 * Factory methods for creating LocalActivityResult instances that require SDK-specific types. The
 * core LocalActivityResult class is in temporal-core.
 */
public final class LocalActivityResultFactory {

  private LocalActivityResultFactory() {}

  /**
   * Creates a LocalActivityResult for a completed execution.
   *
   * @param ahResult the activity handler result
   * @param attempt the attempt number
   * @return the result
   */
  public static io.temporal.internal.worker.LocalActivityResult completed(
      ActivityTaskHandler.Result ahResult, int attempt) {
    return new io.temporal.internal.worker.LocalActivityResult(
        ahResult.getActivityId(), attempt, ahResult.getTaskCompleted(), null, null, null);
  }

  /**
   * Creates a LocalActivityResult for a cancelled execution.
   *
   * @param ahResult the activity handler result
   * @param attempt the attempt number
   * @return the result
   */
  public static io.temporal.internal.worker.LocalActivityResult cancelled(
      ActivityTaskHandler.Result ahResult, int attempt) {
    return new io.temporal.internal.worker.LocalActivityResult(
        ahResult.getActivityId(), attempt, null, null, ahResult.getTaskCanceled(), null);
  }
}
