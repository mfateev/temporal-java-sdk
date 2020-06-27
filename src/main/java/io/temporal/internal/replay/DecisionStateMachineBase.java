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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class DecisionStateMachineBase implements DecisionStateMachine {

  protected DecisionState state = DecisionState.CREATED;

  protected List<String> stateHistory = new ArrayList<String>();

  private final DecisionId id;

  private final AtomicBoolean isReplay;

  private List<Decision> decisions = new ArrayList<>();

  public DecisionStateMachineBase(DecisionId id, AtomicBoolean isReplay) {
    this.id = id;
    this.isReplay = isReplay;
    stateHistory.add(state.toString());
  }

  public final void initIdempotently() {
    if (state == DecisionState.CREATED) {
      addDecision(newInitiateDecision());
    }
  }

  /** Used for unit testing. */
  protected DecisionStateMachineBase(DecisionId id, DecisionState state) {
    this.id = id;
    this.state = state;
    this.isReplay = new AtomicBoolean();
    stateHistory.add(state.toString());
  }

  protected abstract Decision newInitiateDecision();

  protected Decision newRequestCancelDecision() {
    throw new IllegalStateException();
  }

  @Override
  public DecisionState getState() {
    return state;
  }

  @Override
  public DecisionId getId() {
    return id;
  }

  @Override
  public boolean isDone() {
    return state == DecisionState.COMPLETED
        || state == DecisionState.COMPLETED_AFTER_CANCELLATION_DECISION_SENT;
  }

  @Override
  public List<Decision> takeDecisions() {
    List<Decision> result = decisions;
    decisions = new ArrayList<>();
    return result;
  }

  protected void addDecision(Decision decision) {
    if (!isReplay.get()) {
      decisions.add(decision);
    }
  }

  @Override
  public void handleDecisionTaskStartedEvent() {
    switch (state) {
      case CREATED:
        stateHistory.add("handleDecisionTaskStartedEvent");
        state = DecisionState.DECISION_SENT;
        stateHistory.add(state.toString());
        break;
      default:
    }
    decisions.clear();
  }

  @Override
  public boolean cancel(Runnable immediateCancellationCallback) {
    stateHistory.add("cancel");
    boolean result = false;
    switch (state) {
      case CREATED:
        state = DecisionState.CANCELED_BEFORE_INITIATED;
        if (immediateCancellationCallback != null) {
          immediateCancellationCallback.run();
        }
        addDecision(newRequestCancelDecision());
        break;
      case DECISION_SENT:
        failStateTransition();
        //        state = DecisionState.CANCELED_BEFORE_INITIATED;
        //        result = true;
        break;
      case INITIATED:
        state = DecisionState.CANCELED_AFTER_INITIATED;
        result = true;
        addDecision(newRequestCancelDecision());
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
    return result;
  }

  @Override
  public void handleInitiatedEvent(HistoryEvent event) {
    stateHistory.add("handleInitiatedEvent");
    switch (state) {
      case DECISION_SENT:
        state = DecisionState.INITIATED;
        break;
      case CANCELED_BEFORE_INITIATED:
        state = DecisionState.CANCELED_AFTER_INITIATED;
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public void handleInitiationFailedEvent(HistoryEvent event) {
    stateHistory.add("handleInitiationFailedEvent");
    switch (state) {
      case INITIATED:
      case DECISION_SENT:
      case CANCELED_BEFORE_INITIATED:
        state = DecisionState.COMPLETED;
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public void handleStartedEvent(HistoryEvent event) {
    stateHistory.add("handleStartedEvent");
  }

  @Override
  public void handleCompletionEvent() {
    stateHistory.add("handleCompletionEvent");
    switch (state) {
      case CANCELED_AFTER_INITIATED:
      case INITIATED:
        state = DecisionState.COMPLETED;
        break;
      case CANCELLATION_DECISION_SENT:
        state = DecisionState.COMPLETED_AFTER_CANCELLATION_DECISION_SENT;
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public void handleCancellationInitiatedEvent() {
    stateHistory.add("handleCancellationInitiatedEvent");
    switch (state) {
      case CANCELLATION_DECISION_SENT:
        // No state change
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public void handleCancellationFailureEvent(HistoryEvent event) {
    stateHistory.add("handleCancellationFailureEvent");
    switch (state) {
      case COMPLETED_AFTER_CANCELLATION_DECISION_SENT:
        state = DecisionState.COMPLETED;
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public void handleCancellationEvent() {
    stateHistory.add("handleCancellationEvent");
    switch (state) {
      case CANCELLATION_DECISION_SENT:
        state = DecisionState.COMPLETED;
        break;
      default:
        failStateTransition();
    }
    stateHistory.add(state.toString());
  }

  @Override
  public String toString() {
    return "DecisionStateMachineBase [id="
        + id
        + ", state="
        + state
        + ", isDone="
        + isDone()
        + ", stateHistory="
        + stateHistory
        + "]";
  }

  protected void failStateTransition() {
    throw new IllegalStateException("id=" + id + ", transitions=" + stateHistory);
  }
}
