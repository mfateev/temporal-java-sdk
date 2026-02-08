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

package io.temporal.internal.statemachines;

import io.temporal.api.common.v1.SearchAttributes;
import io.temporal.api.failure.v1.Failure;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Callbacks for SDK-specific operations used by WorkflowStateMachines.
 *
 * <p>This interface abstracts SDK-specific functionality needed by the state machine orchestrator,
 * allowing it to be used by different SDK implementations (Java sync, Kotlin coroutines, etc.)
 * without direct dependencies on SDK-specific classes.
 */
public interface WorkflowStateMachinesSdkCallbacks {

  /**
   * Creates a canceled failure exception with the given message.
   *
   * @param message the cancellation message
   * @return an Exception representing the cancellation
   */
  Exception createCanceledFailure(String message);

  /**
   * Checks if the given protobuf Failure represents a benign application failure. Benign failures
   * don't count against workflow failure metrics.
   *
   * @param failure the protobuf Failure to check
   * @return true if this is a benign application failure
   */
  boolean isBenignApplicationFailure(@Nullable Failure failure);

  /**
   * Creates search attributes for version marker recording.
   *
   * @param newChangeId the new change ID being recorded
   * @param newVersion the version for the new change ID
   * @param existingVersions map of existing change IDs to their versions
   * @return the SearchAttributes to upsert, or null if search attributes should not be updated
   */
  @Nullable
  SearchAttributes createVersionMarkerSearchAttributes(
      String newChangeId, Integer newVersion, Map<String, Integer> existingVersions);

  /**
   * Gets the name of the version change search attribute.
   *
   * @return the search attribute name (e.g., "TemporalChangeVersion")
   */
  String getVersionChangeSearchAttributeName();

  /** Default no-op implementation for cases where SDK-specific callbacks are not needed. */
  WorkflowStateMachinesSdkCallbacks DEFAULT =
      new WorkflowStateMachinesSdkCallbacks() {
        @Override
        public Exception createCanceledFailure(String message) {
          return new RuntimeException(message);
        }

        @Override
        public boolean isBenignApplicationFailure(@Nullable Failure failure) {
          return false;
        }

        @Override
        @Nullable
        public SearchAttributes createVersionMarkerSearchAttributes(
            String newChangeId, Integer newVersion, Map<String, Integer> existingVersions) {
          return null;
        }

        @Override
        public String getVersionChangeSearchAttributeName() {
          return "TemporalChangeVersion";
        }
      };
}
