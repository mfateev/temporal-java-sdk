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
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.common.FailureUtils;
import io.temporal.internal.history.VersionMarkerUtils;
import java.util.Map;
import javax.annotation.Nullable;

/** SDK implementation of the callbacks needed by WorkflowStateMachines. */
public class WorkflowStateMachinesSdkCallbacksImpl implements WorkflowStateMachinesSdkCallbacks {

  public static final WorkflowStateMachinesSdkCallbacksImpl INSTANCE =
      new WorkflowStateMachinesSdkCallbacksImpl();

  private WorkflowStateMachinesSdkCallbacksImpl() {}

  @Override
  public Exception createCanceledFailure(String message) {
    return new CanceledFailure(message);
  }

  @Override
  public boolean isBenignApplicationFailure(@Nullable Failure failure) {
    return FailureUtils.isBenignApplicationFailure(failure);
  }

  @Override
  @Nullable
  public SearchAttributes createVersionMarkerSearchAttributes(
      String newChangeId, Integer newVersion, Map<String, Integer> existingVersions) {
    return VersionMarkerUtils.createVersionMarkerSearchAttributes(
        newChangeId, newVersion, existingVersions);
  }

  @Override
  public String getVersionChangeSearchAttributeName() {
    return VersionMarkerUtils.TEMPORAL_CHANGE_VERSION.getName();
  }
}
