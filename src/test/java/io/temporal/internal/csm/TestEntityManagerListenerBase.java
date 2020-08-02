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

import io.temporal.api.history.v1.HistoryEvent;

abstract class TestEntityManagerListenerBase implements EntityManagerListener {

  boolean invoked;

  @Override
  public void start(HistoryEvent startWorkflowEvent) {}

  @Override
  public void signal(HistoryEvent signalEvent) {}

  @Override
  public void cancel(HistoryEvent cancelEvent) {}

  @Override
  public final void eventLoop() {
    if (invoked) {
      return;
    }
    invoked = true;
    eventLoopImpl();
  }

  protected abstract void eventLoopImpl();
}
