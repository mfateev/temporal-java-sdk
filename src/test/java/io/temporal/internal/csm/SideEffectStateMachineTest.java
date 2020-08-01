package io.temporal.internal.csm;

import static io.temporal.internal.csm.MutableSideEffectStateMachine.MARKER_DATA_KEY;
import static io.temporal.internal.csm.SideEffectStateMachine.SIDE_EFFECT_MARKER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.temporal.api.command.v1.Command;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.common.converter.DataConverter;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class SideEffectStateMachineTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;

  @Test
  public void testOne() {
    class TestListener extends TestEntityManagerListenerBase {
      Optional<Payloads> result;

      @Override
      public void eventLoopImpl() {
        manager.sideEffect(
            () -> converter.toPayloads("m1Arg1", "m1Arg2"),
            (r) -> {
              result = r;
              manager.sideEffect(
                  () -> converter.toPayloads("m2Arg1"),
                  (rr) -> {
                    manager.newCompleteWorkflow(Optional.empty());
                  });
            });
      }
    }
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                MarkerRecordedEventAttributes.newBuilder()
                    .setMarkerName(SIDE_EFFECT_MARKER_NAME)
                    .putDetails(MARKER_DATA_KEY, converter.toPayloads("m1Arg1", "m1Arg2").get())
                    .build())
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                MarkerRecordedEventAttributes.newBuilder()
                    .setMarkerName(SIDE_EFFECT_MARKER_NAME)
                    .putDetails(MARKER_DATA_KEY, converter.toPayloads("m2Arg1").get())
                    .build())
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);

    {
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);
      assertEquals(3, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(1).getCommandType());
      Optional<Payloads> data1 =
          Optional.of(
              commands
                  .get(0)
                  .getRecordMarkerCommandAttributes()
                  .getDetailsMap()
                  .get(MARKER_DATA_KEY));
      assertEquals("m1Arg1", converter.fromPayloads(0, data1, String.class, String.class));
      assertEquals("m1Arg2", converter.fromPayloads(1, data1, String.class, String.class));

      Optional<Payloads> data2 =
          Optional.of(
              commands
                  .get(1)
                  .getRecordMarkerCommandAttributes()
                  .getDetailsMap()
                  .get(MARKER_DATA_KEY));
      assertEquals("m2Arg1", converter.fromPayloads(0, data2, String.class, String.class));

      assertEquals(
          CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(2).getCommandType());
    }
    {
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertTrue(commands.isEmpty());
    }
  }
}
