package io.temporal.internal.csm;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.util.Timestamps;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskCompletedEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskScheduledEventAttributes;
import io.temporal.api.history.v1.WorkflowTaskStartedEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.common.ProtobufTimeUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class EntityManagerTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;
  private HistoryEvent result;

  @Test
  public void testTimer() {

    TestEntityManagerListener listener = new TestTimerListener();
    manager = new EntityManager(listener);
    manager.setStartedIds(0, 3);
    HistoryBuilder h = new HistoryBuilder();
    h.addWorkflowTask();
    h.handleEvents(manager);
    List<Command> commands = manager.takeCommands();
    assertEquals(1, commands.size());
    assertEquals(CommandType.COMMAND_TYPE_START_TIMER, commands.get(0).getCommandType());

    // Full replay
    h.addWorkflowTaskCompleted();
    int timerStartedEventId = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(
        EventType.EVENT_TYPE_TIMER_FIRED,
        TimerFiredEventAttributes.newBuilder()
            .setTimerId("timer1")
            .setStartedEventId(timerStartedEventId));
    h.addWorkflowTask();
    listener = new TestTimerListener();
    manager = new EntityManager(listener);
    h.handleEvents(manager);
    commands = manager.takeCommands();

    assertEquals(1, commands.size());
    assertEquals(
        CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(0).getCommandType());
  }

  private static class HistoryBuilder {
    private int eventId;
    private final List<HistoryEvent> events = new ArrayList<>();
    private int workflowTaskScheduledEventId;
    private int previousStartedEventId;

    void handleEvents(EntityManager manager) {
      List<HistoryEvent> history = build();
      manager.setStartedIds(getPreviousStartedEventId(), getWorkflowTaskStartedEventId());
      for (HistoryEvent event : history) {
        manager.handleEvent(event);
      }
    }

    private List<HistoryEvent> build() {
      return new ArrayList<>(events);
    }

    HistoryBuilder add(EventType type) {
      add(type, null);
      return this;
    }

    int addGetEventId(EventType type) {
      add(type, null);
      return eventId;
    }

    HistoryBuilder add(EventType type, Object attributes) {
      events.add(newEvent(type, attributes));
      return this;
    }

    void addWorkflowTaskCompleted() {
      add(
          EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          WorkflowTaskCompletedEventAttributes.newBuilder()
              .setScheduledEventId(workflowTaskScheduledEventId)
              .build());
    }

    int addWorkflowTask() {
      add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED);
      previousStartedEventId = workflowTaskScheduledEventId + 1;
      workflowTaskScheduledEventId = addGetEventId(EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED);
      add(
          EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
          WorkflowTaskStartedEventAttributes.newBuilder()
              .setScheduledEventId(workflowTaskScheduledEventId)
              .build());
      return eventId;
    }

    private HistoryEvent newEvent(EventType type) {
      switch (type) {
        case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
          return newEvent(type, WorkflowExecutionStartedEventAttributes.getDefaultInstance());
        case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
          return newEvent(type, WorkflowTaskScheduledEventAttributes.getDefaultInstance());
        case EVENT_TYPE_WORKFLOW_TASK_STARTED:
          return newEvent(type, WorkflowTaskStartedEventAttributes.getDefaultInstance());
        case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
          return newEvent(type, WorkflowTaskCompletedEventAttributes.getDefaultInstance());

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
        case EVENT_TYPE_TIMER_STARTED:
        case EVENT_TYPE_TIMER_FIRED:
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

    private HistoryEvent newEvent(EventType type, Object attributes) {
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

          case EVENT_TYPE_UNSPECIFIED:
          case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
          case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
          case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
          case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
          case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
          case EVENT_TYPE_WORKFLOW_TASK_FAILED:
          case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
          case EVENT_TYPE_ACTIVITY_TASK_STARTED:
          case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
          case EVENT_TYPE_ACTIVITY_TASK_FAILED:
          case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
          case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
          case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
          case EVENT_TYPE_TIMER_STARTED:
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
