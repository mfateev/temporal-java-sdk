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
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.sdk.v1.UserMetadata;
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.worker.LocalActivityDispatcher;
import io.temporal.internal.worker.SingleWorkerOptions;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CoalescingWorkflowRunTaskHandlerImpl implements WorkflowRunTaskHandler {

  private final WorkflowRunTaskHandler handler;
  private final List<WorkflowRunTaskHandler> handlers = new ArrayList<>();
  // One result per split
  private final Payloads.Builder splitResults = Payloads.newBuilder();
  private int completionCount;

  public CoalescingWorkflowRunTaskHandlerImpl(
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponseOrBuilder workflowTask,
      SingleWorkerOptions workerOptions,
      Scope metricsScope,
      LocalActivityDispatcher localActivityDispatcher,
      GetSystemInfoResponse.Capabilities capabilities) {
    HistoryEvent event = workflowTask.getHistory().getEvents(0);
    if (event.getEventType() != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
      throw new IllegalArgumentException("First event is not workflow task started: " + event);
    }
    if (event.getUserMetadata().getSummary().containsMetadata("coalesced")) {
      handler = null;
      WorkflowExecutionStartedEventAttributes started =
          event.getWorkflowExecutionStartedEventAttributes();
      // Assumption that a coalesced workflow must have a single input.
      for (int i = 0; i < started.getInput().getPayloadsCount(); i++) {
        handlers.add(
            new ReplayWorkflowRunTaskHandler(
                namespace,
                workflow,
                workflowTask,
                workerOptions,
                metricsScope,
                localActivityDispatcher,
                capabilities));
      }
    } else {
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
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, WorkflowHistoryIterator historyIterator)
      throws Throwable {
    if (handler != null) {
      return handler.handleWorkflowTask(workflowTask, historyIterator);
    }
    List<WorkflowTaskResult> results = new ArrayList<>();
    CoalescedWorkflowHistoryIterators iterators =
        new CoalescedWorkflowHistoryIterators(results.size(), historyIterator);

    for (int i = 0; i < handlers.size(); i++) {
      WorkflowRunTaskHandler h = handlers.get(0);
      results.add(h.handleWorkflowTask(workflowTask, iterators.get(i)));
    }
    return coalesceResults(results);
  }

  @Override
  public QueryResult handleDirectQueryWorkflowTask(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, WorkflowHistoryIterator historyIterator)
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

  private WorkflowTaskResult coalesceResults(List<WorkflowTaskResult> results) {
    List<Command> coalescedCommands = new ArrayList<>();
    boolean forceTask = false;
    Set<Integer> sdkFlags = new HashSet<>();
    for (int i = 0; i < results.size(); i++) {
      WorkflowTaskResult result = results.get(i);
      forceTask = forceTask || result.isForceWorkflowTask();
      sdkFlags.addAll(result.getSdkFlags());
      List<Command> commands = result.getCommands();
      for (Command command : commands) {
        Payload r;
        if (WorkflowExecutionUtils.isWorkflowExecutionCompleteCommand(command)) {
          if (command.getCommandType() == CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION) {
            // Only one result is supported
            r = command.getCompleteWorkflowExecutionCommandAttributes().getResult().getPayloads(0);
          } else {
            try {
              r =
                  Payload.parseFrom(
                      WorkflowExecutionUtils.prettyPrintObject(command)
                          .getBytes(StandardCharsets.UTF_8));

            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException(e);
            }
          }
          splitResults.setPayloads(i, r);
          // Only one completion command is allowed
          if (++completionCount == handlers.size()) {
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
          }
        } else {
          Payload summary =
              Payload.newBuilder()
                  .putMetadata(
                      "split", ByteString.copyFrom(Integer.toString(i), StandardCharsets.UTF_8))
                  .build();
          UserMetadata metadata = UserMetadata.newBuilder().setSummary(summary).build();
          coalescedCommands.add(command.toBuilder().setUserMetadata(metadata).build());
        }
      }
    }
    // TODO(maxim): Figure out the final command. And may be query later.
    return WorkflowTaskResult.newBuilder()
        .setCommands(coalescedCommands)
        .setForceWorkflowTask(forceTask)
        .setSdkFlags(new ArrayList<>(sdkFlags))
        .build();
  }
}
