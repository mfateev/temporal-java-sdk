/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.csm;

import static io.temporal.internal.csm.MutableSideEffectStateMachine.MARKER_ID_KEY;
import static io.temporal.internal.csm.TestHistoryBuilder.assertCommand;
import static io.temporal.internal.csm.VersionStateMachine.MARKER_VERSION_KEY;
import static io.temporal.internal.csm.VersionStateMachine.VERSION_MARKER_NAME;
import static io.temporal.workflow.Workflow.DEFAULT_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Duration;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.common.converter.DataConverter;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class VersionStateMachineTest {

  private final DataConverter converter = DataConverter.getDefaultInstance();
  private EntityManager manager;

  @Test
  public void testOne() {
    final int maxSupported = 12;
    class TestListener extends TestEntityManagerListenerBase {
      @Override
      public void eventLoopImpl() {
        manager.getVersion(
            "id1",
            DEFAULT_VERSION,
            maxSupported,
            (v) -> manager.newCompleteWorkflow(converter.toPayloads(v)));
      }
    }
    /*
       1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
       2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
       3: EVENT_TYPE_WORKFLOW_TASK_STARTED
       4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
       5: EVENT_TYPE_MARKER_RECORDED
       6: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    */
    MarkerRecordedEventAttributes.Builder markerBuilder =
        MarkerRecordedEventAttributes.newBuilder()
            .setMarkerName(VERSION_MARKER_NAME)
            .putDetails(MARKER_ID_KEY, converter.toPayloads("id1").get());
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_VERSION_KEY, converter.toPayloads(maxSupported).get())
                    .build())
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);

    {
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);

      assertEquals(2, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      assertEquals(
          CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(1).getCommandType());
      Optional<Payloads> resultData =
          Optional.of(commands.get(1).getCompleteWorkflowExecutionCommandAttributes().getResult());
      assertEquals(
          maxSupported, (int) converter.fromPayloads(0, resultData, Integer.class, Integer.class));
    }
    {
      // Full replay
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertTrue(commands.toString(), commands.isEmpty());
    }
  }

  @Test
  public void testMultiple() {
    final int maxSupported = 13;
    class TestListener extends TestEntityManagerListenerBase {
      @Override
      public void eventLoopImpl() {
        manager.getVersion(
            "id1",
            DEFAULT_VERSION,
            maxSupported,
            (v1) ->
                manager.getVersion(
                    "id1",
                    DEFAULT_VERSION,
                    maxSupported + 10,
                    (v2) ->
                        manager.getVersion(
                            "id1",
                            DEFAULT_VERSION,
                            maxSupported + 100,
                            (v3) -> manager.newCompleteWorkflow(converter.toPayloads(v3)))));
      }
    }
    /*
      1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
      2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      3: EVENT_TYPE_WORKFLOW_TASK_STARTED
      4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      5: EVENT_TYPE_MARKER_RECORDED
      6: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    */
    MarkerRecordedEventAttributes.Builder markerBuilder =
        MarkerRecordedEventAttributes.newBuilder()
            .setMarkerName(VERSION_MARKER_NAME)
            .putDetails(MARKER_ID_KEY, converter.toPayloads("id1").get());
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_VERSION_KEY, converter.toPayloads(maxSupported).get())
                    .build())
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);

    {
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);

      assertEquals(2, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      int version =
          converter.fromPayloads(
              0,
              Optional.ofNullable(
                  commands
                      .get(0)
                      .getRecordMarkerCommandAttributes()
                      .getDetailsOrThrow(MARKER_VERSION_KEY)),
              Integer.class,
              Integer.class);
      assertEquals(maxSupported, version);
      assertEquals(
          CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands.get(1).getCommandType());
      Optional<Payloads> resultData =
          Optional.of(commands.get(1).getCompleteWorkflowExecutionCommandAttributes().getResult());
      assertEquals(
          maxSupported, (int) converter.fromPayloads(0, resultData, Integer.class, Integer.class));
    }
    {
      // Full replay
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertTrue(commands.isEmpty());
    }
  }

  /**
   * Tests that getVersion call returns DEFAULT version when there is no correspondent marker in the
   * history. It happens when getVersion call was added at the workflow place that already executed.
   */
  @Test
  public void testNewGetVersion() {
    final int maxSupported = 13;
    class TestListener extends TestEntityManagerListenerBase {
      final StringBuilder trace = new StringBuilder();

      @Override
      public void eventLoopImpl() {
        manager.getVersion(
            "id1",
            DEFAULT_VERSION,
            maxSupported,
            (v1) -> {
              trace.append(v1 + ", ");
              manager.getVersion(
                  "id1",
                  DEFAULT_VERSION,
                  maxSupported + 10,
                  (v2) -> {
                    trace.append(v2 + ", ");
                    manager.getVersion(
                        "id1",
                        DEFAULT_VERSION,
                        maxSupported + 100,
                        (v3) -> {
                          trace.append(v3);
                          manager.newTimer(
                              StartTimerCommandAttributes.newBuilder()
                                  .setStartToFireTimeout(
                                      Duration.newBuilder().setSeconds(100).build())
                                  .build(),
                              (e1) -> manager.newCompleteWorkflow(converter.toPayloads(v3)));
                        });
                  });
            });
      }
    }
    /*
      1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
      2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      3: EVENT_TYPE_WORKFLOW_TASK_STARTED
      4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      5: EVENT_TYPE_TIMER_STARTED
      6: EVENT_TYPE_TIMER_FIRED
      7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      8: EVENT_TYPE_WORKFLOW_TASK_STARTED
      9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      10: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    */
    MarkerRecordedEventAttributes.Builder markerBuilder =
        MarkerRecordedEventAttributes.newBuilder()
            .setMarkerName(VERSION_MARKER_NAME)
            .putDetails(MARKER_ID_KEY, converter.toPayloads("id1").get());
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask();
    long timerStartedEventId1 = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(
            EventType.EVENT_TYPE_TIMER_FIRED,
            TimerFiredEventAttributes.newBuilder()
                .setStartedEventId(timerStartedEventId1)
                .setTimerId("timer1"))
        .addWorkflowTask()
        .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);
    {
      // Full replay
      TestListener listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertTrue(commands.isEmpty());
      assertEquals(
          DEFAULT_VERSION + ", " + DEFAULT_VERSION + ", " + DEFAULT_VERSION,
          listener.trace.toString());
    }
  }

  @Test
  public void testRecordAcrossMultipleWorkflowTasks() {
    final int maxSupported = 133;
    class TestListener extends TestEntityManagerListenerBase {
      final StringBuilder trace = new StringBuilder();

      @Override
      public void eventLoopImpl() {
        manager.getVersion(
            "id1",
            DEFAULT_VERSION,
            maxSupported,
            (v1) -> {
              trace.append(v1 + ", ");
              manager.getVersion(
                  "id1",
                  DEFAULT_VERSION,
                  maxSupported - 10,
                  (v2) ->
                      manager.newTimer(
                          StartTimerCommandAttributes.newBuilder()
                              .setStartToFireTimeout(Duration.newBuilder().setSeconds(100).build())
                              .build(),
                          (e1) ->
                              manager.newTimer(
                                  StartTimerCommandAttributes.newBuilder()
                                      .setStartToFireTimeout(
                                          Duration.newBuilder().setSeconds(100).build())
                                      .build(),
                                  (e2) ->
                                      manager.getVersion(
                                          "id1",
                                          maxSupported - 3,
                                          maxSupported + 10,
                                          (v3) -> {
                                            trace.append(v3 + ", ");
                                            manager.getVersion(
                                                "id1",
                                                DEFAULT_VERSION,
                                                maxSupported + 100,
                                                (v4) -> {
                                                  trace.append(v4);
                                                  manager.newCompleteWorkflow(
                                                      converter.toPayloads(v3));
                                                });
                                          }))));
            });
      }
    }
    /*
      1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
      2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      3: EVENT_TYPE_WORKFLOW_TASK_STARTED
      4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      5: EVENT_TYPE_MARKER_RECORDED
      6: EVENT_TYPE_TIMER_STARTED
      7: EVENT_TYPE_TIMER_FIRED
      8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      9: EVENT_TYPE_WORKFLOW_TASK_STARTED
      10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      11: EVENT_TYPE_TIMER_STARTED
      12: EVENT_TYPE_TIMER_FIRED
      13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
      14: EVENT_TYPE_WORKFLOW_TASK_STARTED
      15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
      16: EVENT_TYPE_MARKER_RECORDED
      17: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    */
    MarkerRecordedEventAttributes.Builder markerBuilder =
        MarkerRecordedEventAttributes.newBuilder()
            .setMarkerName(VERSION_MARKER_NAME)
            .putDetails(MARKER_ID_KEY, converter.toPayloads("id1").get());
    TestHistoryBuilder h =
        new TestHistoryBuilder()
            .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .addWorkflowTask()
            .add(
                EventType.EVENT_TYPE_MARKER_RECORDED,
                markerBuilder
                    .putDetails(MARKER_VERSION_KEY, converter.toPayloads(maxSupported).get())
                    .build());
    long timerStartedEventId1 = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(
            EventType.EVENT_TYPE_TIMER_FIRED,
            TimerFiredEventAttributes.newBuilder()
                .setStartedEventId(timerStartedEventId1)
                .setTimerId("timer1"))
        .addWorkflowTask();
    long timerStartedEventId2 = h.addGetEventId(EventType.EVENT_TYPE_TIMER_STARTED);
    h.add(
            EventType.EVENT_TYPE_TIMER_FIRED,
            TimerFiredEventAttributes.newBuilder()
                .setStartedEventId(timerStartedEventId2)
                .setTimerId("timer2"))
        .addWorkflowTask()
        .add(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED);
    {
      TestEntityManagerListenerBase listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 1);

      assertEquals(2, commands.size());
      assertEquals(CommandType.COMMAND_TYPE_RECORD_MARKER, commands.get(0).getCommandType());
      int version =
          converter.fromPayloads(
              0,
              Optional.ofNullable(
                  commands
                      .get(0)
                      .getRecordMarkerCommandAttributes()
                      .getDetailsOrThrow(MARKER_VERSION_KEY)),
              Integer.class,
              Integer.class);
      assertEquals(maxSupported, version);
      assertEquals(CommandType.COMMAND_TYPE_START_TIMER, commands.get(1).getCommandType());
    }
    {
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 2);
      assertCommand(CommandType.COMMAND_TYPE_START_TIMER, commands);
    }
    {
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager, 3);
      assertCommand(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, commands);
      Optional<Payloads> resultData =
          Optional.of(commands.get(0).getCompleteWorkflowExecutionCommandAttributes().getResult());
      assertEquals(
          maxSupported, (int) converter.fromPayloads(0, resultData, Integer.class, Integer.class));
    }
    {
      // Full replay
      TestListener listener = new TestListener();
      manager = new EntityManager(listener);
      List<Command> commands = h.handleWorkflowTaskTakeCommands(manager);
      assertTrue(commands.isEmpty());
      assertEquals(
          maxSupported + ", " + maxSupported + ", " + maxSupported, listener.trace.toString());
    }
  }
}
