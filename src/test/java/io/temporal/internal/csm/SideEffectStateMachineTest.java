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
    class TestSingleMSEListener extends TestEntityManagerListenerBase {
      boolean invoked;
      Optional<Payloads> result;

      @Override
      public void eventLoop() {
        if (invoked) {
          return;
        }
        invoked = true;
        manager.sideEffect(
            () -> converter.toPayloads("foo", "bar"),
            (r) -> {
              result = r;
              manager.newCompleteWorkflow(Optional.empty());
            });
      }
    }

    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTaskScheduledAndStarted();

    TestEntityManagerListenerBase listener = new TestSingleMSEListener();
    manager = new EntityManager(listener);
    List<Command> commands = h.handleWorkflowTask(manager);
    assertEquals(2, commands.size());
    assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
    Optional<Payloads> data =
        Optional.of(
            commands
                .get(0)
                .getRecordMarkerCommandAttributes()
                .getDetailsMap()
                .get(MARKER_DATA_KEY));
    assertEquals("foo", converter.fromPayloads(0, data, String.class, String.class));
    assertEquals("bar", converter.fromPayloads(1, data, String.class, String.class));
    assertEquals(
        CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(1).getCommandType());
  }
}
