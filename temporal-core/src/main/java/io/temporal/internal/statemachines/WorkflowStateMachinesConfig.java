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

/**
 * Configuration interface for WorkflowStateMachines.
 *
 * <p>This interface abstracts the configuration options needed by the state machine orchestrator,
 * allowing it to be used by different SDK implementations (Java sync, Kotlin coroutines, etc.)
 * without direct dependencies on SDK-specific classes.
 */
public interface WorkflowStateMachinesConfig {

  /**
   * Whether to automatically upsert the TemporalChangeVersion search attribute when a version is
   * called.
   *
   * @return true if version search attributes should be upserted, false otherwise
   */
  boolean isEnableUpsertVersionSearchAttributes();

  /** Default configuration with all options at their default values. */
  WorkflowStateMachinesConfig DEFAULT =
      new WorkflowStateMachinesConfig() {
        @Override
        public boolean isEnableUpsertVersionSearchAttributes() {
          return true;
        }
      };
}
