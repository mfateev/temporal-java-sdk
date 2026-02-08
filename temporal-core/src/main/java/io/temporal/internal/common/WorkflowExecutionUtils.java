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

package io.temporal.internal.common;

import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.HistoryEventOrBuilder;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;

/**
 * Core workflow execution utilities used by state machines. These methods have no SDK dependencies
 * and can be shared across SDK implementations.
 */
public final class WorkflowExecutionUtils {

  private WorkflowExecutionUtils() {}

  public static boolean isWorkflowTaskClosedEvent(HistoryEventOrBuilder event) {
    return ((event != null)
        && (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT));
  }

  public static boolean isWorkflowExecutionClosedEvent(HistoryEventOrBuilder event) {
    return ((event != null)
        && (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
            || event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED));
  }

  public static boolean isActivityTaskClosedEvent(HistoryEvent event) {
    return ((event != null)
        && (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
            || event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED
            || event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED
            || event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT));
  }

  public static boolean isExternalWorkflowClosedEvent(HistoryEvent event) {
    return ((event != null)
        && (event.getEventType() == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
            || event.getEventType() == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
            || event.getEventType() == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
            || event.getEventType() == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
            || event.getEventType() == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT));
  }

  /** Command event is an event that is created to mirror a command issued by a workflow task */
  public static boolean isCommandEvent(HistoryEvent event) {
    EventType eventType = event.getEventType();
    switch (eventType) {
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_TIMER_STARTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
      case EVENT_TYPE_TIMER_CANCELED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_MARKER_RECORDED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
      case EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED:
      case EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
      case EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:
        return true;
      default:
        return false;
    }
  }

  /** Returns event that corresponds to a command. */
  public static EventType getEventTypeForCommand(CommandType commandType) {
    switch (commandType) {
      case COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
        return EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED;
      case COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
        return EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED;
      case COMMAND_TYPE_START_TIMER:
        return EventType.EVENT_TYPE_TIMER_STARTED;
      case COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED;
      case COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED;
      case COMMAND_TYPE_CANCEL_TIMER:
        return EventType.EVENT_TYPE_TIMER_CANCELED;
      case COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED;
      case COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED;
      case COMMAND_TYPE_RECORD_MARKER:
        return EventType.EVENT_TYPE_MARKER_RECORDED;
      case COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW;
      case COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED;
      case COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
        return EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED;
      case COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
        return EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES;
      case COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:
        return EventType.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED;
      case COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:
        return EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED;
      case COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION:
        return EventType.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED;
    }
    throw new IllegalArgumentException("Unknown commandType");
  }

  public static boolean isFullHistory(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    return workflowTask.getHistory() != null
        && workflowTask.getHistory().getEventsCount() > 0
        && workflowTask.getHistory().getEvents(0).getEventId() == 1;
  }
}
