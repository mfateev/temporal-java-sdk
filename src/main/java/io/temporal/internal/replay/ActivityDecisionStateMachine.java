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
import io.temporal.decision.v1.RequestCancelActivityTaskDecisionAttributes;
import io.temporal.decision.v1.ScheduleActivityTaskDecisionAttributes;
import io.temporal.enums.v1.DecisionType;
import io.temporal.history.v1.HistoryEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class ActivityDecisionStateMachine extends DecisionStateMachineBase {

  private ScheduleActivityTaskDecisionAttributes scheduleAttributes;
  private long scheduledEventId;
  private final AtomicLong nextEventId;
  private List<Decision> decisions = new ArrayList<>();

  public ActivityDecisionStateMachine(
      DecisionId id,
      AtomicLong nextEventId,
      AtomicBoolean isReplay,
      ScheduleActivityTaskDecisionAttributes scheduleAttributes) {
    super(id, isReplay);
    this.scheduledEventId = id.getDecisionEventId();
    this.nextEventId = nextEventId;
    this.scheduleAttributes = scheduleAttributes;
  }

  @Override
  public void handleDecisionTaskStartedEvent() {
    switch (state) {
      case CANCELED_AFTER_INITIATED:
        stateHistory.add("handleDecisionTaskStartedEvent");
        state = DecisionState.CANCELLATION_DECISION_SENT;
        stateHistory.add(state.toString());
        break;
      default:
        super.handleDecisionTaskStartedEvent();
    }
  }

  @Override
  public void handleCancellationFailureEvent(HistoryEvent event) {
    switch (state) {
      case CANCELLATION_DECISION_SENT:
        stateHistory.add("handleCancellationFailureEvent");
        state = DecisionState.INITIATED;
        stateHistory.add(state.toString());
        break;
      default:
        super.handleCancellationFailureEvent(event);
    }
    //    nextEventId.incrementAndGet();
  }

  @Override
  protected Decision newRequestCancelDecision() {
    return Decision.newBuilder()
        .setRequestCancelActivityTaskDecisionAttributes(
            RequestCancelActivityTaskDecisionAttributes.newBuilder()
                .setScheduledEventId(scheduledEventId))
        .setDecisionType(DecisionType.DECISION_TYPE_REQUEST_CANCEL_ACTIVITY_TASK)
        .build();
  }

  @Override
  protected Decision newInitiateDecision() {
    scheduledEventId = getId().getDecisionEventId();
    return Decision.newBuilder()
        .setScheduleActivityTaskDecisionAttributes(scheduleAttributes)
        .setDecisionType(DecisionType.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK)
        .build();
  }
}
