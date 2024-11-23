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

package io.temporal.internal.replay;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.m3.tally.Scope;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.CompleteWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.*;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.sdk.v1.UserMetadata;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponse;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.worker.LocalActivityDispatcher;
import io.temporal.internal.worker.SingleWorkerOptions;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoalescingWorkflowRunTaskHandlerImpl implements WorkflowRunTaskHandler {

  private static final Logger log =
      LoggerFactory.getLogger(CoalescingWorkflowRunTaskHandlerImpl.class);
  private final WorkflowRunTaskHandler handler;
  private final List<WorkflowRunTaskHandler> handlers = new ArrayList<>();
  private MultiIterator multiIterator;
  private int completionCount;
  List<Payload> resultPayloads = new ArrayList<>();

  public CoalescingWorkflowRunTaskHandlerImpl(
      String namespace,
      ReplayWorkflowFactory workflowFactory,
      PollWorkflowTaskQueueResponse workflowTask,
      SingleWorkerOptions workerOptions,
      Scope metricsScope,
      LocalActivityDispatcher localActivityDispatcher,
      GetSystemInfoResponse.Capabilities capabilities)
      throws Exception {
    HistoryEvent event = workflowTask.getHistory().getEvents(0);
    if (event.getEventType() != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
      throw new IllegalArgumentException("First event is not workflow task started: " + event);
    }
    Payload s = event.getUserMetadata().getSummary();
    String summary = s.getData().toString(StandardCharsets.UTF_8);
    WorkflowType workflowType = workflowTask.getWorkflowType();
    WorkflowExecution workflowExecution = workflowTask.getWorkflowExecution();
    if ("\"bundle\"".equals(summary)) {
      handler = null;
      WorkflowExecutionStartedEventAttributes started =
          event.getWorkflowExecutionStartedEventAttributes();
      multiIterator = new MultiIterator(started.getInput().getPayloadsCount());
      // Assumption that a coalesced workflow must have a single input.
      for (int i = 0; i < started.getInput().getPayloadsCount(); i++) {
        ReplayWorkflow workflow = workflowFactory.getWorkflow(workflowType, workflowExecution);
        handlers.add(
            new ReplayWorkflowRunTaskHandler(
                namespace,
                workflow,
                workflowTask,
                workerOptions,
                metricsScope,
                localActivityDispatcher,
                capabilities));
        resultPayloads.add(null);
      }
    } else {
      ReplayWorkflow workflow = workflowFactory.getWorkflow(workflowType, workflowExecution);
      handler =
          new ReplayWorkflowRunTaskHandler(
              namespace,
              workflow,
              workflowTask,
              workerOptions,
              metricsScope,
              localActivityDispatcher,
              capabilities);
    }
  }

  @Override
  public WorkflowTaskResult handleWorkflowTask(
      PollWorkflowTaskQueueResponse workflowTask, WorkflowHistoryIterator historyIterator)
      throws Throwable {
    if (handler != null) {
      return handler.handleWorkflowTask(workflowTask, historyIterator);
    }
    multiIterator.setSourceIterator(historyIterator, workflowTask.getPreviousStartedEventId());

    List<PollWorkflowTaskQueueResponse> tasks = new ArrayList<>();
    {
      // Ugly hack that requires scanning the whole history to find the last event id and
      // previousStartedEventId
      for (int i = 0; i < handlers.size(); i++) {
        SplitHistoryIterator iterator = multiIterator.getStartedIdIterator(i);
        while (iterator.hasNext()) {
          iterator.next();
        }
        long splitPreviousStartedEventId = iterator.getSplitPreviousStartedEventId();
        PollWorkflowTaskQueueResponse.Builder task =
            workflowTask.toBuilder()
                .setStartedEventId(iterator.getLastEventId())
                .setPreviousStartedEventId(splitPreviousStartedEventId);
        PollWorkflowTaskQueueResponse tt = task.build();
        tasks.add(task.build());
      }
    }
    List<WorkflowTaskResult> results = new ArrayList<>();
    for (int i = 0; i < handlers.size(); i++) {
      WorkflowRunTaskHandler h = handlers.get(i);
      WorkflowHistoryIterator iterator = multiIterator.getIterator(i);
      WorkflowTaskResult result = h.handleWorkflowTask(tasks.get(i), iterator);
      results.add(result);
    }
    return coalesceResults(results);
  }

  @Override
  public QueryResult handleDirectQueryWorkflowTask(
      PollWorkflowTaskQueueResponse workflowTask, WorkflowHistoryIterator historyIterator)
      throws Throwable {
    if (handler != null) {
      return handler.handleDirectQueryWorkflowTask(workflowTask, historyIterator);
    }
    throw new UnsupportedOperationException("Coalescing workflow doesn't support queries yet");
  }

  @Override
  public void resetStartedEvenId(Long eventId) {
    if (handler != null) {
      handler.resetStartedEvenId(eventId);
    }
    throw new UnsupportedOperationException("Coalescing workflow doesn't support reset yet");
  }

  @Override
  public void close() {
    if (handler != null) {
      handler.close();
    } else {
      for (WorkflowRunTaskHandler h : handlers) {
        h.close();
      }
    }
  }

  // index is to the first list. The element is appended to the inner list.
  private static void insertAtIndex(List<List<Command>> list, int index, Command command) {
    while (list.size() <= index) {
      list.add(new ArrayList<>());
    }
    list.get(index).add(command);
  }

  /** Coalesces results of multiple split workflows into a single result. */
  private WorkflowTaskResult coalesceResults(List<WorkflowTaskResult> results) {
    log.info("coalesceResults begin");
    for (WorkflowTaskResult result : results) {
      log.debug(
          "Coalescing results {}",
          WorkflowExecutionUtils.prettyPrintCommands(result.getCommands()));
    }
    List<Command> coalescedCommands = new ArrayList<>();

    // Current batch of commands to be coalesced
    String scheduleActivityTypeName = null;
    Command scheduleActivityCommand = null;
    List<Payload> scheduleActivityInputs = new ArrayList<>();

    boolean forceTask = false;
    Set<Integer> sdkFlags = new HashSet<>();
    for (int i = 0; i < results.size(); i++) {
      WorkflowTaskResult result = results.get(i);
      forceTask = forceTask || result.isForceWorkflowTask();
      sdkFlags.addAll(result.getSdkFlags());
      List<Command> commands = result.getCommands();
      for (Command command : commands) {
        if (WorkflowExecutionUtils.isWorkflowExecutionCompleteCommand(command)) {
          if (coalesceWorkflowCompleteCommand(command, i, coalescedCommands)) break;
        } else if (command.getCommandType() == CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK) {
          String typeName =
              command.getScheduleActivityTaskCommandAttributes().getActivityType().getName();
          if (scheduleActivityTypeName == null || scheduleActivityTypeName.equals(typeName)) {
            addScheduleActivityInput(command, i, scheduleActivityInputs);
            if (scheduleActivityCommand == null) {
              scheduleActivityCommand = command;
            }
            scheduleActivityTypeName = typeName;
          } else {
            coalesceScheduleActivity(
                scheduleActivityCommand, scheduleActivityInputs, coalescedCommands);

            scheduleActivityCommand = command;
            scheduleActivityTypeName = typeName;
            scheduleActivityInputs.clear();
            addScheduleActivityInput(command, i, scheduleActivityInputs);
          }
        } else {
          UserMetadata metadata = splitIdToUserMetadata(i);
          Command.Builder commandBuilder = command.toBuilder().setUserMetadata(metadata);
          coalescedCommands.add(commandBuilder.build());
        }
      }
    }
    if (scheduleActivityInputs.size() > 0) {
      coalesceScheduleActivity(scheduleActivityCommand, scheduleActivityInputs, coalescedCommands);
    }
    return WorkflowTaskResult.newBuilder(results.get(0)).setCommands(coalescedCommands).build();
  }

  private static void addScheduleActivityInput(
      Command command, int i, List<Payload> scheduleActivityInputs) {
    ByteString index = ByteString.copyFromUtf8(Integer.toString(i));
    ByteString activityId =
        ByteString.copyFromUtf8(command.getScheduleActivityTaskCommandAttributes().getActivityId());
    // Only the first argument is used
    Payload input =
        command.getScheduleActivityTaskCommandAttributes().getInput().getPayloads(0).toBuilder()
            .putMetadata("split", index)
            .putMetadata("activityId", activityId)
            .build();
    scheduleActivityInputs.add(input);
  }

  private static void coalesceScheduleActivity(
      Command firstInTheBatch,
      List<Payload> scheduleActivityInputs,
      List<Command> coalescedCommands) {
    if (scheduleActivityInputs.size() == 1) {
      Payload payload = scheduleActivityInputs.get(0);
      int splitId = getSplitId(payload);
      UserMetadata metadata = splitIdToUserMetadata(splitId);
      Command.Builder commandBuilder = firstInTheBatch.toBuilder().setUserMetadata(metadata);
      coalescedCommands.add(commandBuilder.build());
      return;
    }
    // Coalesce all commands of the same type
    Command c =
        firstInTheBatch.toBuilder()
            .setScheduleActivityTaskCommandAttributes(
                firstInTheBatch.getScheduleActivityTaskCommandAttributes().toBuilder()
                    .setHeader(
                        Header.newBuilder().putFields("bundle", Payload.newBuilder().build()))
                    .setInput(Payloads.newBuilder().addAllPayloads(scheduleActivityInputs)))
            .build();
    coalescedCommands.add(c);
  }

  private static UserMetadata splitIdToUserMetadata(int splitId) {
    Payload summary =
        Payload.newBuilder()
            .putMetadata(
                "split", ByteString.copyFrom(Integer.toString(splitId), StandardCharsets.UTF_8))
            .build();
    UserMetadata metadata = UserMetadata.newBuilder().setSummary(summary).build();
    return metadata;
  }

  private static int getSplitId(Payload payload) {
    Map<String, ByteString> metadataMap = payload.getMetadataMap();
    String splitS = metadataMap.get("split").toStringUtf8();
    int splitId = Integer.parseInt(splitS);
    return splitId;
  }

  private boolean coalesceWorkflowCompleteCommand(
      Command command, int i, List<Command> coalescedCommands) {
    Payload r;
    if (command.getCommandType() == CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION) {
      // Only one result is supported
      r = command.getCompleteWorkflowExecutionCommandAttributes().getResult().getPayloads(0);
    } else {
      try {
        r =
            Payload.parseFrom(
                WorkflowExecutionUtils.prettyPrintObject(command).getBytes(StandardCharsets.UTF_8));

      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
    resultPayloads.set(i, r);
    // All completion commands are merged into a single completion command
    if (++completionCount == handlers.size()) {
      // One result per split
      Payloads.Builder splitResults = Payloads.newBuilder();
      splitResults.addAllPayloads(resultPayloads);
      CompleteWorkflowExecutionCommandAttributes attr =
          CompleteWorkflowExecutionCommandAttributes.newBuilder()
              .setResult(splitResults.build())
              .build();
      Command c =
          Command.newBuilder()
              .setCommandType(CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION)
              .setCompleteWorkflowExecutionCommandAttributes(attr)
              .build();
      coalescedCommands.add(c);
      return true;
    }
    return false;
  }
}
