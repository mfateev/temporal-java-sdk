package io.temporal.internal.csm;

import static io.temporal.internal.csm.TestHistoryBuilder.assertCommand;
import static org.junit.Assert.assertEquals;

import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionSignaledEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.workflow.Functions;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class TimerStateMachineTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;

  @Test
  public void testTimerFire() {
    class TestTimerFireListener extends TestEntityManagerListenerBase {

      @Override
      public void eventLoopImpl() {
        manager.newTimer(
            StartTimerCommandAttributes.newBuilder()
                .setTimerId("timer1")
                .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                .build(),
            (firedEvent) -> manager.newCompleteWorkflow(Optional.empty()));
      }
    }

    TestHistoryBuilder h = new TestHistoryBuilder();
    {
      TestEntityManagerListenerBase listener = new TestTimerFireListener();
      manager = new EntityManager(listener);
      h.add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED);
      h.addWorkflowTask();
      long timerStartedEventId = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
      h.add(
          EventType.EVENT_TYPE_TIMER_FIRED,
          TimerFiredEventAttributes.newBuilder()
              .setStartedEventId(timerStartedEventId)
              .setTimerId("timer1"));
      h.addWorkflowTaskScheduledAndStarted();
      assertEquals(2, h.getWorkflowTaskCount());
    }
    {
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);
      assertCommand(CommandType.COMMAND_TYPE_START_TIMER, commands);
    }
    System.out.println("PROCESSING TASK 2");
    {
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 2);
      assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
    }
    {
      TestEntityManagerListenerBase listener = new TestTimerFireListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 2);
      assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
    }
  }

  private String payloadToString(Payloads payloads) {
    return converter.fromPayloads(0, Optional.of(payloads), String.class, String.class);
  }

  @Test
  public void testImmediateTimerCancellation() {
    class TestTimerImmediateCancellationListener extends TestEntityManagerListenerBase {

      @Override
      public void eventLoopImpl() {
        Functions.Proc cancellationHandler =
            manager.newTimer(
                StartTimerCommandAttributes.newBuilder()
                    .setTimerId("timer1")
                    .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                    .build(),
                (firedEvent) ->
                    assertEquals(EventType.EVENT_TYPE_TIMER_CANCELED, firedEvent.getEventType()));
        manager.newTimer(
            StartTimerCommandAttributes.newBuilder()
                .setTimerId("timer2")
                .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                .build(),
            (firedEvent) -> manager.newCompleteWorkflow(converter.toPayloads("result1")));

        // Immediate cancellation
        cancellationHandler.apply();
      }
    }

    TestHistoryBuilder h = new TestHistoryBuilder();
    h.add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED);
    h.addWorkflowTask();
    long timerStartedEventId = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(EventType.EVENT_TYPE_TIMER_FIRED, timerStartedEventId);
    h.addWorkflowTaskScheduledAndStarted();
    {
      TestEntityManagerListenerBase listener = new TestTimerImmediateCancellationListener();
      manager = new EntityManager(listener);
      {
        List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);
        assertCommand(CommandType.COMMAND_TYPE_START_TIMER, commands);
        assertEquals("timer2", commands.get(0).getStartTimerCommandAttributes().getTimerId());
      }
      {
        List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
        assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
      }
    }
    {
      TestEntityManagerListenerBase listener = new TestTimerImmediateCancellationListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
    }
  }

  @Test
  public void testStartedTimerCancellation() {

    class TestTimerCancellationListener extends TestEntityManagerListenerBase {
      private Functions.Proc cancellationHandler;
      private String firedTimerId;

      public String getFiredTimerId() {
        return firedTimerId;
      }

      @Override
      public void eventLoopImpl() {
        cancellationHandler =
            manager.newTimer(
                StartTimerCommandAttributes.newBuilder()
                    .setTimerId("timer1")
                    .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                    .build(),
                (firedEvent) -> {
                  assertEquals(EventType.EVENT_TYPE_TIMER_CANCELED, firedEvent.getEventType());
                  manager.newCompleteWorkflow(converter.toPayloads("result1"));
                });
        manager.newTimer(
            StartTimerCommandAttributes.newBuilder()
                .setTimerId("timer2")
                .setStartToFireTimeout(ProtobufTimeUtils.ToProtoDuration(Duration.ofHours(1)))
                .build(),
            (firedEvent) -> {
              assertEquals(EventType.EVENT_TYPE_TIMER_FIRED, firedEvent.getEventType());
              firedTimerId = firedEvent.getTimerFiredEventAttributes().getTimerId();
            });
      }

      @Override
      public void signal(HistoryEvent signalEvent) {
        assertEquals(
            "signal1", signalEvent.getWorkflowExecutionSignaledEventAttributes().getSignalName());
        cancellationHandler.apply();
      }
    }

    TestHistoryBuilder h = new TestHistoryBuilder();
    h.add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED);
    h.addWorkflowTask();
    long timerStartedEventId1 = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    long timerStartedEventId2 = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
            WorkflowExecutionSignaledEventAttributes.newBuilder().setSignalName("signal1"))
        .addWorkflowTaskScheduled()
        .add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
            WorkflowExecutionSignaledEventAttributes.newBuilder().setSignalName("signal1"))
        .addWorkflowTaskStarted()
        .addWorkflowTaskCompleted()
        .add(EventType.EVENT_TYPE_TIMER_CANCELED, timerStartedEventId1)
        .add(
            EventType.EVENT_TYPE_TIMER_FIRED,
            TimerFiredEventAttributes.newBuilder()
                .setStartedEventId(timerStartedEventId2)
                .setTimerId("timer2"))
        .addWorkflowTaskScheduledAndStarted();
    {
      TestTimerCancellationListener listener = new TestTimerCancellationListener();
      manager = new EntityManager(listener);
      {
        List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);
        assertEquals(2, commands.size());
        assertEquals(CommandType.COMMAND_TYPE_START_TIMER, commands.get(0).getCommandType());
        assertEquals(CommandType.COMMAND_TYPE_START_TIMER, commands.get(1).getCommandType());
        assertEquals("timer1", commands.get(0).getStartTimerCommandAttributes().getTimerId());
        assertEquals("timer2", commands.get(1).getStartTimerCommandAttributes().getTimerId());
      }
      {
        List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 2);
        assertCommand(CommandType.COMMAND_TYPE_CANCEL_TIMER, commands);
      }
      {
        List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
        assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
        assertEquals("timer2", listener.getFiredTimerId());
      }
    }
    {
      TestTimerCancellationListener listener = new TestTimerCancellationListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
      assertEquals("timer2", listener.getFiredTimerId());
    }
  }
}
