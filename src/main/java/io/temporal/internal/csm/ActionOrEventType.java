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

package io.temporal.internal.csm;

import io.temporal.api.enums.v1.EventType;

class ActionOrEventType<Action> {
  final Action action;
  final EventType eventType;

  public ActionOrEventType(Action action) {
    this.action = action;
    this.eventType = null;
  }

  public ActionOrEventType(EventType eventType) {
    this.eventType = eventType;
    this.action = null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ActionOrEventType<?> that = (ActionOrEventType<?>) o;
    return com.google.common.base.Objects.equal(action, that.action) && eventType == that.eventType;
  }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(action, eventType);
  }

  @Override
  public String toString() {
    if (action == null) {
      return eventType.toString();
    }
    return action.toString();
  }
}
