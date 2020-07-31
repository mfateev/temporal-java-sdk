package io.temporal.internal.csm;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.util.Timestamps;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.api.history.v1.TimerStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskCompletedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskScheduledEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskStartedEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.common.ProtobufTimeUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class EntityManagerTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;
  private HistoryEvent result;

  @Test
  public void testTimer() {
    TestEntityManagerListener listener = new TestTimerListener();
    manager = new EntityManager(listener);
    HistoryBuilder h = new HistoryBuilder();
    h.add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED);
    h.addWorkflowTask();
    //    System.out.println(h);
    //    h.handleEvents(manager);
    //    List<Command> commands = manager.takeCommands();
    //    assertEquals(1, commands.size());
    //    assertEquals(CommandType.COMMAND_TYPE_START_TIMER, commands.get(0).getCommandType());

    //    // Next workflow task
    //    {
    //      HistoryBuilder h2 = h.nextTask();
    //      System.out.println(h2);
    //      long timerStartedEventId = h2.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    //      h2.add(EventType.EVENT_TYPE_TIMER_FIRED, timerStartedEventId);
    //      h2.addWorkflowTask();
    //      System.out.println("To Process: " + h2);
    //      h2.handleEvents(manager);
    //      commands = manager.takeCommands();
    //      assertEquals(1, commands.size());
    //      assertEquals(
    //          CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
    // commands.get(0).getCommandType());
    //    }

    // Full replay
    h.addWorkflowTaskCompleted();
    long timerStartedEventId = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(EventType.EVENT_TYPE_TIMER_FIRED, timerStartedEventId);
    h.addWorkflowTask();
    listener = new TestTimerListener();
    manager = new EntityManager(listener);
    List<Command> commands = h.newExecutor().handleCached(manager);
    //    h.handleEvents(manager);
    //    commands = manager.takeCommands();

    assertEquals(1, commands.size());
    assertEquals(
        CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(0).getCommandType());
  }

  private static class TestTaskExecutor {
    private final List<HistoryEvent> events;

    private TestTaskExecutor(List<HistoryEvent> events) {
      this.events = events;
    }

    List<Command> handleCached(EntityManager manager) {
      PeekingIterator<HistoryEvent> history = Iterators.peekingIterator(events.iterator());
      if (!history.hasNext()
          || history.peek().getEventType() != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
        throw new IllegalArgumentException(
            "The first event in the history is not WorkflowExecutionStarted");
      }
      long previous = 0;
      long started = 3;
      manager.setStartedIds(previous, started);
      while (true) {
        HistoryEvent event = null;
        while (true) {
          if (!history.hasNext()) {
            if (started != event.getEventId()) {
              throw new IllegalArgumentException(
                  "The last event in the history is not WorkflowTaskStarted");
            }
            return manager.takeCommands();
          }
          event = history.next();
          if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
              && (!history.hasNext()
                  || history.peek().getEventType()
                      == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)) {
            previous = started;
            started = event.getEventId();
            manager.setStartedIds(previous, started);
          }
          manager.handleEvent(event);
        }
      }
    }
  }

  private static class HistoryBuilder {
    private long eventId;
    private final List<HistoryEvent> events = new ArrayList<>();
    private long workflowTaskScheduledEventId;
    private long previousStartedEventId;

    TestTaskExecutor newExecutor() {
      return new TestTaskExecutor(events);
    }

    private List<HistoryEvent> build() {
      return new ArrayList<>(events);
    }

    HistoryBuilder add(EventType type) {
      add(type, null);
      return this;
    }

    long addGetEventId(EventType type) {
      add(type, null);
      return eventId;
    }

    HistoryBuilder add(EventType type, Object attributes) {
      events.add(newAttributes(type, attributes));
      return this;
    }

    void addWorkflowTaskCompleted() {
      add(EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, workflowTaskScheduledEventId);
    }

    long addWorkflowTask() {
      previousStartedEventId = workflowTaskScheduledEventId + 1;
      workflowTaskScheduledEventId = addGetEventId(EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED);
      add(EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED, workflowTaskScheduledEventId);
      return eventId;
    }

    private Object newAttributes(EventType type, long initialEventId) {
      switch (type) {
        case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
          return WorkflowExecutionStartedEventAttributes.getDefaultInstance();
        case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
          return WorkflowTaskScheduledEventAttributes.getDefaultInstance();
        case EVENT_TYPE_WORKFLOW_TASK_STARTED:
          return WorkflowTaskStartedEventAttributes.newBuilder()
              .setScheduledEventId(initialEventId);
        case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
          return WorkflowTaskCompletedEventAttributes.newBuilder()
              .setScheduledEventId(initialEventId);
        case EVENT_TYPE_TIMER_FIRED:
          return TimerFiredEventAttributes.newBuilder().setStartedEventId(initialEventId);
        case EVENT_TYPE_TIMER_STARTED:
          return TimerStartedEventAttributes.getDefaultInstance();

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
        case EVENT_TYPE_TIMER_CANCELED:
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
          case EVENT_TYPE_TIMER_CANCELED:
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

    public HistoryBuilder nextTask() {
      HistoryBuilder result = new HistoryBuilder();
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
          + events.stream()
              .map((event) -> event.getEventId() + ": " + event.getEventType())
              .collect(Collectors.joining("\n    "))
          + '}';
    }
  }

  private static class TestEntityManagerListener implements EntityManagerListener {
    @Override
    public void start(HistoryEvent startWorkflowEvent) {}

    @Override
    public void signal(HistoryEvent signalEvent) {}

    @Override
    public void cancel(HistoryEvent cancelEvent) {}

    @Override
    public void eventLoop() {}
  }

  private class TestTimerListener extends TestEntityManagerListener {
    boolean timerCreated;

    @Override
    public void eventLoop() {
      if (!timerCreated) {
        timerCreated = true;
        manager.newTimer(
            StartTimerCommandAttributes.newBuilder()
                .setTimerId("timer1")
                .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                .build(),
            (firedEvent) -> {
              result = firedEvent;
              manager.newCompleteWorkflow(converter.toPayloads("result1"));
            });
      }
    }
  }
}
