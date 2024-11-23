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

package io.temporal.internal.replay;

import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitHistoryIterator implements WorkflowHistoryIterator {

  private static final Logger log = LoggerFactory.getLogger(SplitHistoryIterator.class);

  private MultiIterator.SplitIterator sourceIterator;
  private final int splitIndex;
  private long lastEventId;
  private HistoryEvent nextElement;
  private boolean hasNextComputed;
  // Mapping of scheduled event ids
  // Original event id -> split history event id
  private Map<Long, Long> scheduledEventIds = new HashMap<>();
  // Original event id -> (splitIndex -> index of the result for this split)
  private Map<Long, Map<Integer, Integer>> scheduledEventInputPayloadIndex = new HashMap<>();
  private long previousStartedEventId;

  // Mapping of workflow task started event ids
  // Original event id -> split history event id
  private Map<Long, Long> workflowTaskStartedEventIds = new HashMap<>();

  public SplitHistoryIterator(int splitIndex, MultiIterator.SplitIterator sourceIterator) {
    this.splitIndex = splitIndex;
    this.sourceIterator = sourceIterator;
  }

  public void reset(long previousStartedEventId) {
    log.info(
        "Resetting splitIndex={} previousStartedEventId={}", splitIndex, previousStartedEventId);
    this.previousStartedEventId = previousStartedEventId;
    nextElement = null;
    hasNextComputed = false;
    sourceIterator.reset();
  }

  public long getLastEventId() {
    return lastEventId;
  }

  public long getSplitPreviousStartedEventId() {
    if (previousStartedEventId == 0) {
      return 0;
    }
    return workflowTaskStartedEventIds.get(previousStartedEventId);
  }

  @Override
  public boolean hasNext() {
    if (!hasNextComputed) {
      computeNext();
    }
    return nextElement != null;
  }

  @Override
  public HistoryEvent next() {
    if (!hasNextComputed) {
      computeNext();
    }
    if (nextElement == null) {
      throw new NoSuchElementException("No more elements");
    }
    hasNextComputed = false;
    log.info(
        "SplitHistoryIterator.next splitIndex={} event={} eventType={}",
        splitIndex,
        nextElement.getEventId(),
        nextElement.getEventType());
    return nextElement;
  }

  private void computeNext() {
    if (sourceIterator == null) {
      throw new IllegalStateException("Source iterator is not set");
    }
    while (sourceIterator.hasNext()) {
      HistoryEvent candidate = sourceIterator.next();
      nextElement = copy(candidate);
      if (nextElement != null) {
        hasNextComputed = true;
        return;
      }
    }
    nextElement = null;
    hasNextComputed = true;
  }

  /**
   * @return null if event is not part of the split
   */
  private HistoryEvent copy(HistoryEvent event) {
    log.info(
        "copy splitIndex={} event={} type={}",
        splitIndex,
        event.getEventId(),
        event.getEventType());
    HistoryEvent.Builder result = event.toBuilder();
    SWITCH:
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        WorkflowExecutionStartedEventAttributes.Builder attr =
            result.getWorkflowExecutionStartedEventAttributesBuilder();
        Payloads payloads = attr.getInput();
        Payload payload = payloads.getPayloads(splitIndex);
        // Override runId to use a different seed for random.
        // This avoids activity and child workflow id collisions.
        String runId = attr.getOriginalExecutionRunId() + "/" + splitIndex;
        attr.setOriginalExecutionRunId(runId)
            .setFirstExecutionRunId(runId)
            .setInput(Payloads.newBuilder().addPayloads(payload).build());
        result.setWorkflowExecutionStartedEventAttributes(attr);
        break;
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
        ActivityTaskScheduledEventAttributes.Builder scheduledAttr =
            result.getActivityTaskScheduledEventAttributesBuilder();
        boolean coalesced = scheduledAttr.getHeader().getFieldsMap().containsKey("coalesced");
        // TODO(maxim): This doesn't handle situation when the coalesced activity contains
        // more then one scheduled activity from the same split
        if (coalesced) {
          int inputIndex = 0;
          //          for (Payload p : scheduledAttr.getInput().getPayloadsList()) {
          List<Payload> paylads = scheduledAttr.getInput().getPayloadsList();
          for (int i = 0; i < paylads.size(); i++) {
            Payload p = paylads.get(i);
            Map<String, ByteString> m = p.getMetadataMap();
            int splitIndex = Integer.parseInt(m.get("split").toString(StandardCharsets.UTF_8));
            if (splitIndex != this.splitIndex) {
              continue;
            }
            Map<Integer, Integer> innerMap =
                scheduledEventInputPayloadIndex.computeIfAbsent(
                    event.getEventId(), k -> new HashMap<>());

            // Add or update the value in the inner map
            innerMap.put(splitIndex, i);
            String activityId = m.get("activityId").toString(StandardCharsets.UTF_8);
            scheduledAttr.setActivityId(activityId);
            scheduledAttr.setInput(Payloads.newBuilder().addPayloads(p).build());
            result.setActivityTaskScheduledEventAttributes(scheduledAttr);
            break SWITCH;
          }
          return null; // coalesced activity doesn't contain this split input
        }
      // Intentionally pass through
      default:
        Payload summary = event.getUserMetadata().getSummary();
        if (summary.containsMetadata("split")) {
          int splitIndexFromEvent =
              Integer.parseInt(
                  summary.getMetadataOrThrow("split").toString(StandardCharsets.UTF_8));
          if (splitIndexFromEvent != splitIndex) {
            log.info(
                "Skipping splitIndex={} event={} type={} as it is not part of the split splitIndexFromEvent={}",
                splitIndex,
                event.getEventId(),
                event.getEventType(),
                splitIndexFromEvent);
            return null;
          }
        }
    }
    long eventId = lastEventId + 1;
    result.setEventId(eventId);
    saveScheduledId(event, eventId);
    if (!updateScheduledId(event, result)) {
      // Skip event as its schedule id is not part of the split
      log.info(
          "Skipping splitIndex={} event={} type={} as its schedule id is not part of the split",
          splitIndex,
          event.getEventId(),
          event.getEventType());
      return null;
    }
    if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED) {
      workflowTaskStartedEventIds.put(event.getEventId(), eventId);
    }
    log.info(
        "copy result: splitIndex={} event={} type={}",
        splitIndex,
        result.getEventId(),
        result.getEventType());
    lastEventId = eventId;
    return result.build();
  }

  private boolean updateScheduledId(HistoryEvent event, HistoryEvent.Builder result) {
    try {
      switch (event.getEventType()) {
        case EVENT_TYPE_WORKFLOW_TASK_STARTED:
          {
            WorkflowTaskStartedEventAttributes.Builder attr =
                result.getWorkflowTaskStartedEventAttributesBuilder();
            result.setWorkflowTaskStartedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
          {
            WorkflowTaskCompletedEventAttributes.Builder attr =
                result.getWorkflowTaskCompletedEventAttributesBuilder();
            result.setWorkflowTaskCompletedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_WORKFLOW_TASK_FAILED:
          {
            WorkflowTaskFailedEventAttributes.Builder attr =
                result.getWorkflowTaskFailedEventAttributesBuilder();
            result.setWorkflowTaskFailedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
          {
            WorkflowTaskTimedOutEventAttributes.Builder attr =
                result.getWorkflowTaskTimedOutEventAttributesBuilder();
            result.setWorkflowTaskTimedOutEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_FAILED:
          {
            ActivityTaskFailedEventAttributes.Builder failure =
                result.getActivityTaskFailedEventAttributesBuilder();
            result.setActivityTaskFailedEventAttributes(
                failure.setScheduledEventId(getScheduledEventId(failure.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_STARTED:
          {
            ActivityTaskStartedEventAttributes.Builder attr =
                result.getActivityTaskStartedEventAttributesBuilder();
            result.setActivityTaskStartedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
          {
            ActivityTaskCompletedEventAttributes.Builder attr =
                result.getActivityTaskCompletedEventAttributesBuilder();
            Map<Integer, Integer> sIndex =
                scheduledEventInputPayloadIndex.get(attr.getScheduledEventId());
            if (sIndex != null) {
              Payloads payloads = attr.getResult();
              Payload payload = payloads.getPayloads(sIndex.get(splitIndex));
              attr.setResult(Payloads.newBuilder().addPayloads(payload).build());
            }
            result.setActivityTaskCompletedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
          {
            ActivityTaskTimedOutEventAttributes.Builder attr =
                result.getActivityTaskTimedOutEventAttributesBuilder();
            result.setActivityTaskTimedOutEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
          {
            ActivityTaskCancelRequestedEventAttributes.Builder attr =
                result.getActivityTaskCancelRequestedEventAttributesBuilder();
            result.setActivityTaskCancelRequestedEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
          {
            ActivityTaskCanceledEventAttributes.Builder attr =
                result.getActivityTaskCanceledEventAttributesBuilder();
            result.setActivityTaskCanceledEventAttributes(
                attr.setScheduledEventId(getScheduledEventId(attr.getScheduledEventId())));
            break;
          }
        case EVENT_TYPE_TIMER_FIRED:
          {
            TimerFiredEventAttributes.Builder attr = result.getTimerFiredEventAttributesBuilder();
            result.setTimerFiredEventAttributes(
                attr.setStartedEventId(getScheduledEventId(attr.getStartedEventId())));
            break;
          }
        case EVENT_TYPE_TIMER_CANCELED:
          {
            TimerCanceledEventAttributes.Builder attr =
                result.getTimerCanceledEventAttributesBuilder();
            result.setTimerCanceledEventAttributes(
                attr.setStartedEventId(getScheduledEventId(attr.getStartedEventId())));
            break;
          }
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
          {
            ChildWorkflowExecutionStartedEventAttributes.Builder attr =
                result.getChildWorkflowExecutionStartedEventAttributesBuilder();
            result.setChildWorkflowExecutionStartedEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
          {
            ChildWorkflowExecutionCompletedEventAttributes.Builder attr =
                result.getChildWorkflowExecutionCompletedEventAttributesBuilder();
            result.setChildWorkflowExecutionCompletedEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
          {
            ChildWorkflowExecutionFailedEventAttributes.Builder attr =
                result.getChildWorkflowExecutionFailedEventAttributesBuilder();
            result.setChildWorkflowExecutionFailedEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
          {
            ChildWorkflowExecutionTimedOutEventAttributes.Builder attr =
                result.getChildWorkflowExecutionTimedOutEventAttributesBuilder();
            result.setChildWorkflowExecutionTimedOutEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
          {
            ChildWorkflowExecutionCanceledEventAttributes.Builder attr =
                result.getChildWorkflowExecutionCanceledEventAttributesBuilder();
            result.setChildWorkflowExecutionCanceledEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
        case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
          {
            StartChildWorkflowExecutionFailedEventAttributes.Builder attr =
                result.getStartChildWorkflowExecutionFailedEventAttributesBuilder();
            result.setStartChildWorkflowExecutionFailedEventAttributes(
                attr.setInitiatedEventId(getScheduledEventId(attr.getInitiatedEventId())));
            break;
          }
      }
      return true;
    } catch (IllegalStateException e) {
      // It is kind of hack to avoid checking if scheduledEventId is set in every case.
      return false;
    }
  }

  private void saveScheduledId(HistoryEvent event, long eventId) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
      // intentionally pass through
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      // intentionally pass through
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      // intentionally pass through
      case EVENT_TYPE_TIMER_STARTED:
        scheduledEventIds.put(event.getEventId(), eventId);
        break;
    }
  }

  private Long getScheduledEventId(long originalScheduledEventId) {
    Long scheduledEventId = scheduledEventIds.get(originalScheduledEventId);
    if (scheduledEventId == null) {
      throw new IllegalStateException(
          "No scheduled event id found for "
              + originalScheduledEventId
              + ", scheduledEventIds="
              + scheduledEventIds
              + ", splitIndex="
              + splitIndex);
    }
    return scheduledEventId;
  }

  @Override
  public void initDeadline(Deadline deadline) {
    /// TODO: ???
  }

  @Override
  public String toString() {
    return "SplitHistoryIterator{"
        + "splitIndex="
        + splitIndex
        + ", lastEventId="
        + lastEventId
        + ", scheduledEventIds="
        + scheduledEventIds
        + ", sourceIterator="
        + sourceIterator
        + '}';
  }
}
