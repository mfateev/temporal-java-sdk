package io.temporal.internal.csm;

import static io.temporal.internal.csm.MutableSideEffectStateMachine.MARKER_DATA_KEY;
import static org.junit.Assert.assertEquals;

import io.temporal.api.command.v1.Command;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
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
            () -> converter.toPayloads("foo", "bar"),
            (r) -> {
              result = r;
              manager.sideEffect(
                  () -> converter.toPayloads("baz"),
                  (rr) -> {
                    manager.newCompleteWorkflow(Optional.empty());
                  });
            });
      }
    }

    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTaskScheduledAndStarted();

    TestEntityManagerListenerBase listener = new TestListener();
    manager = new EntityManager(listener);
    List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);

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
    assertEquals("foo", converter.fromPayloads(0, data1, String.class, String.class));
    assertEquals("bar", converter.fromPayloads(1, data1, String.class, String.class));

    Optional<Payloads> data2 =
        Optional.of(
            commands
                .get(1)
                .getRecordMarkerCommandAttributes()
                .getDetailsMap()
                .get(MARKER_DATA_KEY));
    assertEquals("baz", converter.fromPayloads(0, data2, String.class, String.class));

    assertEquals(
        CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(2).getCommandType());
  }
}
