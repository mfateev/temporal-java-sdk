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
 * Metric names used by WorkflowStateMachines for workflow completion events.
 *
 * <p>These constants are used to record metrics when workflows complete in various ways (success,
 * failure, cancellation, continue-as-new).
 */
public final class WorkflowMetrics {

  private static final String TEMPORAL_METRICS_PREFIX = "temporal_";

  /** Counter for successfully completed workflows. */
  public static final String WORKFLOW_COMPLETED_COUNTER =
      TEMPORAL_METRICS_PREFIX + "workflow_completed";

  /** Counter for canceled workflows. */
  public static final String WORKFLOW_CANCELED_COUNTER =
      TEMPORAL_METRICS_PREFIX + "workflow_canceled";

  /** Counter for failed workflows. */
  public static final String WORKFLOW_FAILED_COUNTER = TEMPORAL_METRICS_PREFIX + "workflow_failed";

  /** Counter for workflows that continue-as-new. */
  public static final String WORKFLOW_CONTINUE_AS_NEW_COUNTER =
      TEMPORAL_METRICS_PREFIX + "workflow_continue_as_new";

  private WorkflowMetrics() {}
}
