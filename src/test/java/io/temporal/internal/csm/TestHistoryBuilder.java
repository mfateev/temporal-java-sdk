package io.temporal.internal.csm;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.util.Timestamps;
import io.temporal.api.command.v1.Command;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerCanceledEventAttributes;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.api.history.v1.TimerStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionSignaledEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskCompletedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskScheduledEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskStartedEventAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class TestHistoryBuilder {
  private long eventId;
  private final List<HistoryEvent> events = new ArrayList<>();
  private long workflowTaskScheduledEventId;
  private long previousStartedEventId;

  private List<HistoryEvent> build() {
    return new ArrayList<>(events);
  }

  TestHistoryBuilder add(EventType type) {
    add(type, null);
    return this;
  }

  long addGetEventId(EventType type) {
    add(type, null);
    return eventId;
  }

  TestHistoryBuilder add(EventType type, Object attributes) {
    events.add(newAttributes(type, attributes));
    return this;
  }

  TestHistoryBuilder addWorkflowTask() {
    addWorkflowTaskScheduledAndStarted();
    addWorkflowTaskCompleted();
    return this;
  }

  TestHistoryBuilder addWorkflowTaskCompleted() {
    add(EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, workflowTaskScheduledEventId);
    return this;
  }

  TestHistoryBuilder addWorkflowTaskScheduledAndStarted() {
    addWorkflowTaskScheduled();
    addWorkflowTaskStarted();
    return this;
  }

  TestHistoryBuilder addWorkflowTaskScheduled() {
    previousStartedEventId = workflowTaskScheduledEventId + 1;
    workflowTaskScheduledEventId = addGetEventId(EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED);
    return this;
  }

  TestHistoryBuilder addWorkflowTaskStarted() {
    add(EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED, workflowTaskScheduledEventId);
    return this;
  }

  public int getWorkflowTaskCount() {
    PeekingIterator<HistoryEvent> history = Iterators.peekingIterator(events.iterator());
    int result = 0;
    long started = 0;
    HistoryEvent event = null;
    while (true) {
      if (!history.hasNext()) {
        if (started != event.getEventId()) {
          throw new IllegalArgumentException(
              "The last event in the history is not WorkflowTaskStarted");
        }
        return result;
      }
      event = history.next();
      if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
          && (!history.hasNext()
              || history.peek().getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)) {
        started = event.getEventId();
        result++;
      }
    }
  }

  private int getWorkflowTaskCount(long upToEventId) {
    PeekingIterator<HistoryEvent> history = Iterators.peekingIterator(events.iterator());
    int result = 0;
    long started = 0;
    HistoryEvent event = null;
    while (true) {
      if (!history.hasNext()) {
        if (started != event.getEventId()) {
          throw new IllegalArgumentException(
              "The last event in the history is not WorkflowTaskStarted");
        }
        return result;
      }
      event = history.next();
      if (event.getEventId() > upToEventId) {
        return result;
      }
      if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
          && (!history.hasNext()
              || history.peek().getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)) {
        started = event.getEventId();
        result++;
      }
    }
  }

  List<Command> handleWorkflowTask(EntityManager manager) {
    return handleWorkflowTask(manager, Integer.MAX_VALUE);
  }

  public List<Command> handleWorkflowTask(EntityManager manager, int toTaskIndex) {

    List<HistoryEvent> events =
        this.events.subList((int) manager.getLastStartedEventId(), this.events.size());
    System.out.println("handleWorkflowTask:\n" + eventsToString(events));
    PeekingIterator<HistoryEvent> history = Iterators.peekingIterator(events.iterator());
    long previous = 0;
    long started = 3;
    manager.setStartedIds(previous, started);
    HistoryEvent event = null;
    int count =
        manager.getLastStartedEventId() > 0
            ? getWorkflowTaskCount(history.peek().getEventId() - 1)
            : 0;
    while (true) {
      if (!history.hasNext()) {
        if (started != event.getEventId()) {
          throw new IllegalArgumentException(
              "The last event in the history is not WorkflowTaskStarted");
        }
        throw new IllegalStateException("unreachable");
      }
      event = history.next();
      if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED) {
        if (!history.hasNext()
            || history.peek().getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
          previous = started;
          started = event.getEventId();
          manager.setStartedIds(previous, started);
          count++;
          if (count == toTaskIndex || !history.hasNext()) {
            manager.handleEvent(event);
            return manager.takeCommands();
          }
        } else if (history.hasNext()
            && (history.peek().getEventType() != EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
                && history.peek().getEventType() != EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT)) {
          throw new IllegalStateException(
              "Invalid history. Missing task completed, failed or timed out at "
                  + history.peek().getEventId());
        }
      }
      manager.handleEvent(event);
    }
  }

  private Object newAttributes(EventType type, long initialEventId) {
    switch (type) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        return WorkflowExecutionStartedEventAttributes.getDefaultInstance();
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        return WorkflowTaskScheduledEventAttributes.getDefaultInstance();
      case EVENT_TYPE_WORKFLOW_TASK_STARTED:
        return WorkflowTaskStartedEventAttributes.newBuilder().setScheduledEventId(initialEventId);
      case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
        return WorkflowTaskCompletedEventAttributes.newBuilder()
            .setScheduledEventId(initialEventId);
      case EVENT_TYPE_TIMER_FIRED:
        return TimerFiredEventAttributes.newBuilder().setStartedEventId(initialEventId);
      case EVENT_TYPE_TIMER_STARTED:
        return TimerStartedEventAttributes.getDefaultInstance();
      case EVENT_TYPE_TIMER_CANCELED:
        return TimerCanceledEventAttributes.newBuilder().setStartedEventId(initialEventId);
      case EVENT_TYPE_UNSPECIFIED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
      case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
      case EVENT_TYPE_WORKFLOW_TASK_FAILED:
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      case EVENT_TYPE_ACTIVITY_TASK_STARTED:
      case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
      case EVENT_TYPE_ACTIVITY_TASK_FAILED:
      case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
      case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
      case EVENT_TYPE_MARKER_RECORDED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
      case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case UNRECOGNIZED:
    }
    throw new IllegalArgumentException(type.name());
  }

  private HistoryEvent newAttributes(EventType type, Object attributes) {
    if (attributes == null) {
      attributes = newAttributes(type, 0);
    } else if (attributes instanceof Number) {
      attributes = newAttributes(type, ((Number) attributes).intValue());
    }
    if (attributes instanceof com.google.protobuf.GeneratedMessageV3.Builder) {
      attributes = ((com.google.protobuf.GeneratedMessageV3.Builder) attributes).build();
    }
    HistoryEvent.Builder result =
        HistoryEvent.newBuilder()
            .setEventTime(Timestamps.fromMillis(System.currentTimeMillis()))
            .setEventId(++eventId)
            .setEventType(type);
    if (attributes != null) {
      switch (type) {
        case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
          result.setWorkflowExecutionStartedEventAttributes(
              (WorkflowExecutionStartedEventAttributes) attributes);
          break;
        case EVENT_TYPE_WORKFLOW_TASK_STARTED:
          result.setWorkflowTaskStartedEventAttributes(
              (WorkflowTaskStartedEventAttributes) attributes);
          break;
        case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
          result.setWorkflowTaskCompletedEventAttributes(
              (WorkflowTaskCompletedEventAttributes) attributes);
          break;
        case EVENT_TYPE_TIMER_FIRED:
          result.setTimerFiredEventAttributes((TimerFiredEventAttributes) attributes);
          break;
        case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
          result.setWorkflowTaskScheduledEventAttributes(
              (WorkflowTaskScheduledEventAttributes) attributes);
          break;
        case EVENT_TYPE_TIMER_STARTED:
          result.setTimerStartedEventAttributes((TimerStartedEventAttributes) attributes);
          break;
        case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
          result.setWorkflowExecutionSignaledEventAttributes(
              (WorkflowExecutionSignaledEventAttributes) attributes);
          break;
        case EVENT_TYPE_TIMER_CANCELED:
          result.setTimerCanceledEventAttributes((TimerCanceledEventAttributes) attributes);
          break;

        case EVENT_TYPE_UNSPECIFIED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
        case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
        case EVENT_TYPE_WORKFLOW_TASK_FAILED:
        case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
        case EVENT_TYPE_ACTIVITY_TASK_STARTED:
        case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        case EVENT_TYPE_ACTIVITY_TASK_FAILED:
        case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
        case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
        case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        case EVENT_TYPE_MARKER_RECORDED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
        case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
        case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
        case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
        case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
        case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
        case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
        case UNRECOGNIZED:
          throw new IllegalArgumentException(type.name());
      }
    }
    return result.build();
  }

  public long getPreviousStartedEventId() {
    return previousStartedEventId;
  }

  public long getWorkflowTaskStartedEventId() {
    return workflowTaskScheduledEventId + 1;
  }

  public TestHistoryBuilder nextTask() {
    TestHistoryBuilder result = new TestHistoryBuilder();
    result.eventId = eventId;
    result.workflowTaskScheduledEventId = workflowTaskScheduledEventId;
    result.previousStartedEventId = workflowTaskScheduledEventId;
    result.addWorkflowTaskCompleted();
    return result;
  }

  @Override
  public String toString() {
    return "HistoryBuilder{"
        + "eventId="
        + eventId
        + ", workflowTaskScheduledEventId="
        + workflowTaskScheduledEventId
        + ", previousStartedEventId="
        + previousStartedEventId
        + ", events=\n    "
        + eventsToString(events)
        + '}';
  }

  static String eventsToString(List<HistoryEvent> events) {
    return "    "
        + events.stream()
            .map((event) -> event.getEventId() + ": " + event.getEventType())
            .collect(Collectors.joining("\n    "));
  }

  static void assertCommand(CommandType type, List<Command> commands) {
    assertEquals(1, commands.size());
    assertEquals(type, commands.get(0).getCommandType());
  }
}
