/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.statemachines;

import io.temporal.api.common.v1.Payloads;
import java.util.Optional;

final class MutableSideEffectResult {

  private final Optional<Payloads> data;

  /**
   * Count of how many times handle was called since the last marker recorded. It is used to ensure
   * that an updated value is returned after the same exact number of times during a replay.
   */
  private final int accessCount;

  public MutableSideEffectResult(Optional<Payloads> data) {
    this.data = data;
    accessCount = 1;
  }

  private MutableSideEffectResult(Optional<Payloads> data, int accessCount) {
    this.data = data;
    this.accessCount = accessCount;
  }

  public Optional<Payloads> getData() {
    return data;
  }

  int getAccessCount() {
    return accessCount;
  }

  MutableSideEffectResult incrementAccessCount() {
    return new MutableSideEffectResult(data, accessCount + 1);
  }
}
