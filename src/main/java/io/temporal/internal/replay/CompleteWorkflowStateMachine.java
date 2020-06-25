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

package io.temporal.internal.replay;

import io.temporal.decision.v1.Decision;
import io.temporal.history.v1.HistoryEvent;
import java.util.Arrays;
import java.util.List;

final class CompleteWorkflowStateMachine implements DecisionStateMachine {

  private Decision decision;
  private final DecisionId id;

  public CompleteWorkflowStateMachine(DecisionId id, Decision decision) {
    this.id = id;
    this.decision = decision;
  }

  @Override
  public DecisionId getId() {
    return id;
  }

  @Override
  public List<Decision> getDecisions() {
    return Arrays.asList(decision);
  }

  @Override
  public void handleInitiationFailedEvent(HistoryEvent event) {
    decision = null;
  }

  @Override
  public boolean cancel(Runnable immediateCancellationCallback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleStartedEvent(HistoryEvent event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleCancellationEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleCancellationFailureEvent(HistoryEvent event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleCompletionEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleInitiatedEvent(HistoryEvent event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DecisionState getState() {
    return DecisionState.CREATED;
  }

  @Override
  public void handleCancellationInitiatedEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDone() {
    return decision != null;
  }

  @Override
  public void handleDecisionTaskStartedEvent() {}

  @Override
  public String toString() {
    return "CompleteWorkflowStateMachine [decision=" + decision + ", id=" + id + "]";
  }
}
