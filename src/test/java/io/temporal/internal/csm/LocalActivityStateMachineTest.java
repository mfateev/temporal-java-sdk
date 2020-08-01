package io.temporal.internal.csm;

import static io.temporal.internal.csm.LocalActivityStateMachine.*;
import static io.temporal.internal.csm.MutableSideEffectStateMachine.MARKER_DATA_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.temporal.api.command.v1.Command;
import io.temporal.api.common.v1.ActivityType;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.worker.ActivityTaskHandler;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class LocalActivityStateMachineTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;

  @Test
  public void testLocalActivityStateMachine() {
    class TestListener extends TestEntityManagerListenerBase {
      Optional<Payloads> result;

      @Override
      public void eventLoopImpl() {
        ExecuteLocalActivityParameters parameters1 =
            new ExecuteLocalActivityParameters(
                PollActivityTaskQueueResponse.newBuilder()
                    .setActivityId("id1")
                    .setActivityType(ActivityType.newBuilder().setName("activity1")));
        manager.scheduleLocalActivityTask(
            parameters1, (r, f) -> manager.newCompleteWorkflow(Optional.empty()));

        ExecuteLocalActivityParameters parameters2 =
            new ExecuteLocalActivityParameters(
                PollActivityTaskQueueResponse.newBuilder()
                    .setActivityId("id2")
                    .setActivityType(ActivityType.newBuilder().setName("activity2")));
        ExecuteLocalActivityParameters parameters3 =
            new ExecuteLocalActivityParameters(
                PollActivityTaskQueueResponse.newBuilder()
                    .setActivityId("id3")
                    .setActivityType(ActivityType.newBuilder().setName("activity3")));
        manager.scheduleLocalActivityTask(
            parameters2,
            (r, f) -> manager.scheduleLocalActivityTask(parameters3, (rr, ff) -> result = rr));
      }
    }

    MarkerRecordedEventAttributes.Builder markerBuilder =
        MarkerRecordedEventAttributes.newBuilder()
            .setMarkerName(LOCAL_ACTIVITY_MARKER_NAME)
            .putDetails(MARKER_TIME_KEY, converter.toPayloads(System.currentTimeMillis()).get());
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_ACTIVITY_ID_KEY, converter.toPayloads("id2").get())
                    .build())
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_ACTIVITY_ID_KEY, converter.toPayloads("id3").get())
                    .build())
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_ACTIVITY_ID_KEY, converter.toPayloads("id1").get())
                    .build())
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);

    assertEquals(new TestHistoryBuilder.HistoryInfo(0, 3), h.getHistoryInfo(1));
    assertEquals(new TestHistoryBuilder.HistoryInfo(3, 8), h.getHistoryInfo(2));
    assertEquals(new TestHistoryBuilder.HistoryInfo(3, 8), h.getHistoryInfo());

    TestListener listener = new TestListener();
    manager = new EntityManager(listener);

    {
      h.handleWorkflowTask(manager, 1);
      List<ExecuteLocalActivityParameters> requests = manager.takeLocalActivityRequests();
      assertEquals(2, requests.size());
      assertEquals("id1", requests.get(0).getActivityTask().getActivityId());
      assertEquals("id2", requests.get(1).getActivityTask().getActivityId());

      Payloads result2 = converter.toPayloads("result2").get();
      ActivityTaskHandler.Result completionActivity2 =
          new ActivityTaskHandler.Result(
              "id2",
              RespondActivityTaskCompletedRequest.newBuilder().setResult(result2).build(),
              null,
              null,
              null);
      manager.handleLocalActivityCompletion(completionActivity2);
      requests = manager.takeLocalActivityRequests();
      assertEquals(1, requests.size());
      assertEquals("id3", requests.get(0).getActivityTask().getActivityId());

      Payloads result3 = converter.toPayloads("result3").get();
      ActivityTaskHandler.Result completionActivity3 =
          new ActivityTaskHandler.Result(
              "id3",
              RespondActivityTaskCompletedRequest.newBuilder().setResult(result3).build(),
              null,
              null,
              null);
      manager.handleLocalActivityCompletion(completionActivity3);
      requests = manager.takeLocalActivityRequests();
      assertTrue(requests.isEmpty());

      List<Command> commands = manager.takeCommands();
      assertEquals(2, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(1).getCommandType());
      Optional<Payloads> dataActivity2 =
          Optional.of(
              commands
                  .get(0)
                  .getRecordMarkerCommandAttributes()
                  .getDetailsMap()
                  .get(MARKER_DATA_KEY));
      assertEquals("result2", converter.fromPayloads(0, dataActivity2, String.class, String.class));
      Optional<Payloads> dataActivity3 =
          Optional.of(
              commands
                  .get(1)
                  .getRecordMarkerCommandAttributes()
                  .getDetailsMap()
                  .get(MARKER_DATA_KEY));
      assertEquals("result3", converter.fromPayloads(0, dataActivity3, String.class, String.class));
    }
    {
      h.handleWorkflowTask(manager, 2);
      List<ExecuteLocalActivityParameters> requests = manager.takeLocalActivityRequests();
      assertTrue(requests.isEmpty());

      Payloads result = converter.toPayloads("result1").get();
      ActivityTaskHandler.Result completionActivity1 =
          new ActivityTaskHandler.Result(
              "id1",
              RespondActivityTaskCompletedRequest.newBuilder().setResult(result).build(),
              null,
              null,
              null);
      manager.handleLocalActivityCompletion(completionActivity1);
      requests = manager.takeLocalActivityRequests();
      assertTrue(requests.isEmpty());
      List<Command> commands = manager.takeCommands();
      assertEquals(2, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      Optional<Payloads> data =
          Optional.of(
              commands
                  .get(0)
                  .getRecordMarkerCommandAttributes()
                  .getDetailsMap()
                  .get(MARKER_DATA_KEY));
      assertEquals("result1", converter.fromPayloads(0, data, String.class, String.class));
      assertEquals(
          CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(1).getCommandType());
      assertEquals(
          "result3", converter.fromPayloads(0, listener.result, String.class, String.class));
    }

    // Test full replay
    {
      listener = new TestListener();
      manager = new EntityManager(listener);

      h.handleWorkflowTask(manager);

      List<Command> commands = manager.takeCommands();
      assertTrue(commands.isEmpty());
    }
  }
}
