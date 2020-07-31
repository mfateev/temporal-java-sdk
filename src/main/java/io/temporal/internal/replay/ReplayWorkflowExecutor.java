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

package io.temporal.internal.replay;

import static io.temporal.internal.metrics.MetricsTag.METRICS_TAGS_CALL_OPTIONS_KEY;
import static io.temporal.worker.WorkflowErrorPolicy.FailWorkflow;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import io.grpc.Status;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.enums.v1.QueryResultType;
import io.temporal.api.history.v1.History;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionCancelRequestedEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionSignaledEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.api.query.v1.WorkflowQueryResult;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.common.converter.DataConverter;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.common.GrpcRetryer;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.common.RpcRetryOptions;
import io.temporal.internal.csm.CommandsManagerListener;
import io.temporal.internal.csm.EntityManager;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.internal.replay.HistoryHelper.WorkflowTaskEvents;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.internal.worker.WorkflowExecutionException;
import io.temporal.internal.worker.WorkflowTaskWithHistoryIterator;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.Functions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Implements workflow executor that relies on replay of a workflow code. An instance of this class
 * is created per cached workflow run.
 */
class ReplayWorkflowExecutor implements WorkflowExecutor {

  private static final int MAXIMUM_PAGE_SIZE = 10000;

  private final ReplayWorkflowContextImpl context;

  private final WorkflowServiceStubs service;

  private final ReplayWorkflow workflow;

  private boolean cancelRequested;

  private boolean completed;

  private WorkflowExecutionException failure;

  private long wakeUpTime;

  private Consumer<Exception> timerCancellationHandler;

  private final Scope metricsScope;

  private final Timestamp wfStartTime;
  private final WorkflowExecutionStartedEventAttributes startedEvent;

  private final Lock lock = new ReentrantLock();

  private final Map<String, WorkflowQueryResult> queryResults = new HashMap<>();

  private final DataConverter converter;

  private final EntityManager entityManager;

  private final HistoryEvent firstEvent;

  ReplayWorkflowExecutor(
      WorkflowServiceStubs service,
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponse.Builder workflowTask,
      SingleWorkerOptions options,
      Scope metricsScope) {
    this.service = service;
    this.workflow = workflow;
    firstEvent = workflowTask.getHistory().getEvents(0);

    if (!firstEvent.hasWorkflowExecutionStartedEventAttributes()) {
      throw new IllegalArgumentException(
          "First event in the history is not WorkflowExecutionStarted");
    }
    startedEvent = firstEvent.getWorkflowExecutionStartedEventAttributes();
    this.entityManager = new EntityManager(new CommandsManagerListenerImpl());
    this.metricsScope = metricsScope;
    this.converter = options.getDataConverter();

    wfStartTime = firstEvent.getEventTime();

    context =
        new ReplayWorkflowContextImpl(
            entityManager,
            namespace,
            startedEvent,
            workflowTask.getWorkflowExecution(),
            Timestamps.toMillis(firstEvent.getEventTime()),
            options,
            metricsScope);
  }

  Lock getLock() {
    return lock;
  }

  private void handleWorkflowExecutionStarted(HistoryEvent event) {
    workflow.start(event, context);
  }

  private void handleEvent(HistoryEvent event) {
    entityManager.handleEvent(event);
  }

  private void eventLoop() {
    if (completed) {
      return;
    }
    try {
      completed = workflow.eventLoop();
    } catch (Error e) {
      throw e;
    } catch (WorkflowExecutionException e) {
      failure = e;
      completed = true;
    } catch (CanceledFailure e) {
      if (!cancelRequested) {
        failure = workflow.mapUnexpectedException(e);
      }
      completed = true;
    } catch (Throwable e) {
      // can cast as Error is caught above.
      failure = workflow.mapUnexpectedException(e);
      completed = true;
    }
    if (completed) {
      completeWorkflow();
    }
  }

  private void mayBeCompleteWorkflow() {
    if (completed) {
      completeWorkflow();
    }
  }

  private void completeWorkflow() {
    if (failure != null) {
      entityManager.newFailWorkflow(failure.getFailure());
      metricsScope.counter(MetricsType.WORKFLOW_FAILED_COUNTER).inc(1);
    } else if (cancelRequested) {
      entityManager.newCancelWorkflow();
      metricsScope.counter(MetricsType.WORKFLOW_CANCELLED_COUNTER).inc(1);
    } else {
      ContinueAsNewWorkflowExecutionCommandAttributes attributes =
          context.getContinueAsNewOnCompletion();
      if (attributes != null) {
        entityManager.newContinueAsNewWorkflow(attributes);
        metricsScope.counter(MetricsType.WORKFLOW_CONTINUE_AS_NEW_COUNTER).inc(1);
      } else {
        Optional<Payloads> workflowOutput = workflow.getOutput();
        entityManager.newCompleteWorkflow(workflowOutput);
        metricsScope.counter(MetricsType.WORKFLOW_COMPLETED_COUNTER).inc(1);
      }
    }

    com.uber.m3.util.Duration d =
        ProtobufTimeUtils.ToM3Duration(
            Timestamps.fromMillis(System.currentTimeMillis()), wfStartTime);
    metricsScope.timer(MetricsType.WORKFLOW_E2E_LATENCY).record(d);
  }

  private void handleWorkflowExecutionCancelRequested(HistoryEvent event) {
    WorkflowExecutionCancelRequestedEventAttributes attributes =
        event.getWorkflowExecutionCancelRequestedEventAttributes();
    context.setCancelRequested(true);
    String cause = attributes.getCause();
    workflow.cancel(cause);
    cancelRequested = true;
  }

  private void handleWorkflowExecutionSignaled(HistoryEvent event) {
    WorkflowExecutionSignaledEventAttributes signalAttributes =
        event.getWorkflowExecutionSignaledEventAttributes();
    if (completed) {
      throw new IllegalStateException("Signal received after workflow is closed.");
    }
    Optional<Payloads> input =
        signalAttributes.hasInput() ? Optional.of(signalAttributes.getInput()) : Optional.empty();
    this.workflow.handleSignal(signalAttributes.getSignalName(), input, event.getEventId());
  }

  @Override
  public void handleWorkflowTask(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    lock.lock();
    try {
      queryResults.clear();
      handleWorkflowTaskImpl(workflowTask, null);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public WorkflowTaskResult getResult() {
    lock.lock();
    try {
      List<Command> commands = entityManager.takeCommands();
      return new WorkflowTaskResult(commands, queryResults, completed);
    } finally {
      lock.unlock();
    }
  }

  // Returns boolean to indicate whether we need to force create new workflow task for local
  // activity heartbeating.
  private void handleWorkflowTaskImpl(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, Functions.Proc legacyQueryCallback) {
    Stopwatch sw = metricsScope.timer(MetricsType.WORKFLOW_TASK_REPLAY_LATENCY).start();
    boolean timerStopped = false;
    try {
      entityManager.setStartedIds(
          workflowTask.getPreviousStartedEventId(), workflowTask.getStartedEventId());
      WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator =
          new WorkflowTaskWithHistoryIteratorImpl(
              workflowTask,
              ProtobufTimeUtils.ToJavaDuration(startedEvent.getWorkflowTaskTimeout()));
      Iterator<HistoryEvent> iterator = workflowTaskWithHistoryIterator.getHistory();
      while (iterator.hasNext()) {
        HistoryEvent event = iterator.next();
        handleEvent(event);
      }
    } catch (Error e) {
      WorkflowImplementationOptions implementationOptions =
          this.workflow.getWorkflowImplementationOptions();
      if (implementationOptions.getWorkflowErrorPolicy() == FailWorkflow) {
        // fail workflow
        failure = workflow.mapError(e);
        completed = true;
        completeWorkflow();
      } else {
        metricsScope.counter(MetricsType.WORKFLOW_TASK_NO_COMPLETION_COUNTER).inc(1);
        // fail workflow task, not a workflow
        throw e;
      }
    } finally {
      if (!timerStopped) {
        sw.stop();
      }
      executeQueries(workflowTask.getQueriesMap());
      if (legacyQueryCallback != null) {
        legacyQueryCallback.apply();
      }
      if (completed) {
        close();
      }
    }
  }

  @Override
  public void handleLocalActivityCompletion(ActivityTaskHandler.Result laCompletion) {
    lock.lock();
    try {
      entityManager.handleLocalActivityCompletion(laCompletion);
    } finally {
      lock.unlock();
    }
  }

  private void executeQueries(Map<String, WorkflowQuery> queries) {
    for (Map.Entry<String, WorkflowQuery> entry : queries.entrySet()) {
      WorkflowQuery query = entry.getValue();
      try {
        Optional<Payloads> queryResult = workflow.query(query);
        WorkflowQueryResult.Builder result =
            WorkflowQueryResult.newBuilder()
                .setResultType(QueryResultType.QUERY_RESULT_TYPE_ANSWERED);
        if (queryResult.isPresent()) {
          result.setAnswer(queryResult.get());
        }
        queryResults.put(entry.getKey(), result.build());
      } catch (Exception e) {
        String stackTrace = Throwables.getStackTraceAsString(e);
        queryResults.put(
            entry.getKey(),
            WorkflowQueryResult.newBuilder()
                .setResultType(QueryResultType.QUERY_RESULT_TYPE_FAILED)
                .setErrorMessage(e.getMessage())
                .setAnswer(converter.toPayloads(stackTrace).get())
                .build());
      }
    }
  }

  private Iterator<WorkflowTaskEvents> getWorkflowTaskEventsIterator(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    HistoryHelper historyHelper = newHistoryHelper(workflowTask);
    Iterator<WorkflowTaskEvents> iterator = historyHelper.getIterator();
    if (entityManager.getLastStartedEventId() > 0
        && entityManager.getLastStartedEventId() != historyHelper.getPreviousStartedEventId()
        && workflowTask.getHistory().getEventsCount() > 0) {
      throw new IllegalStateException(
          String.format(
              "ReplayWorkflowExecutor processed up to event id %d. History's previous started event id is %d",
              entityManager.getLastStartedEventId(), historyHelper.getPreviousStartedEventId()));
    }
    return iterator;
  }

  private HistoryHelper newHistoryHelper(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator =
        new WorkflowTaskWithHistoryIteratorImpl(
            workflowTask, ProtobufTimeUtils.ToJavaDuration(startedEvent.getWorkflowTaskTimeout()));
    return new HistoryHelper(
        workflowTaskWithHistoryIterator, context.getReplayCurrentTimeMilliseconds());
  }

  @Override
  public void close() {
    lock.lock();
    try {
      workflow.close();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<Payloads> handleQueryWorkflowTask(
      PollWorkflowTaskQueueResponseOrBuilder response, WorkflowQuery query) {
    lock.lock();
    try {
      AtomicReference<Optional<Payloads>> result = new AtomicReference<>();
      handleWorkflowTaskImpl(response, () -> result.set(workflow.query(query)));
      return result.get();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<ExecuteLocalActivityParameters> getLocalActivityRequests() {
    lock.lock();
    try {
      return entityManager.takeLocalActivityRequests();
    } finally {
      lock.unlock();
    }
  }

  private class WorkflowTaskWithHistoryIteratorImpl implements WorkflowTaskWithHistoryIterator {

    private final Duration retryServiceOperationInitialInterval = Duration.ofMillis(200);
    private final Duration retryServiceOperationMaxInterval = Duration.ofSeconds(4);
    private final Duration paginationStart = Duration.ofMillis(System.currentTimeMillis());
    private Duration workflowTaskTimeout;

    private final PollWorkflowTaskQueueResponseOrBuilder task;
    private Iterator<HistoryEvent> current;
    private ByteString nextPageToken;

    WorkflowTaskWithHistoryIteratorImpl(
        PollWorkflowTaskQueueResponseOrBuilder task, Duration workflowTaskTimeout) {
      this.task = Objects.requireNonNull(task);
      this.workflowTaskTimeout = Objects.requireNonNull(workflowTaskTimeout);

      History history = task.getHistory();
      current = history.getEventsList().iterator();
      nextPageToken = task.getNextPageToken();
    }

    @Override
    public PollWorkflowTaskQueueResponseOrBuilder getWorkflowTask() {
      lock.lock();
      try {
        return task;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Iterator<HistoryEvent> getHistory() {
      return new Iterator<HistoryEvent>() {
        @Override
        public boolean hasNext() {
          return current.hasNext() || !nextPageToken.isEmpty();
        }

        @Override
        public HistoryEvent next() {
          if (current.hasNext()) {
            return current.next();
          }

          Duration passed = Duration.ofMillis(System.currentTimeMillis()).minus(paginationStart);
          Duration expiration = workflowTaskTimeout.minus(passed);
          if (expiration.isZero() || expiration.isNegative()) {
            throw Status.DEADLINE_EXCEEDED
                .withDescription(
                    "getWorkflowExecutionHistory pagination took longer than workflow task timeout")
                .asRuntimeException();
          }
          RpcRetryOptions retryOptions =
              RpcRetryOptions.newBuilder()
                  .setExpiration(expiration)
                  .setInitialInterval(retryServiceOperationInitialInterval)
                  .setMaximumInterval(retryServiceOperationMaxInterval)
                  .build();

          GetWorkflowExecutionHistoryRequest request =
              GetWorkflowExecutionHistoryRequest.newBuilder()
                  .setNamespace(context.getNamespace())
                  .setExecution(task.getWorkflowExecution())
                  .setMaximumPageSize(MAXIMUM_PAGE_SIZE)
                  .setNextPageToken(nextPageToken)
                  .build();

          try {
            GetWorkflowExecutionHistoryResponse r =
                GrpcRetryer.retryWithResult(
                    retryOptions,
                    () ->
                        service
                            .blockingStub()
                            .withOption(METRICS_TAGS_CALL_OPTIONS_KEY, metricsScope)
                            .getWorkflowExecutionHistory(request));
            current = r.getHistory().getEventsList().iterator();
            nextPageToken = r.getNextPageToken();
          } catch (Exception e) {
            throw new Error(e);
          }
          return current.next();
        }
      };
    }
  }

  private class CommandsManagerListenerImpl implements CommandsManagerListener {
    @Override
    public void start(HistoryEvent startWorkflowEvent) {
      workflow.start(startWorkflowEvent, context);
    }

    @Override
    public void eventLoop() {
      ReplayWorkflowExecutor.this.eventLoop();
    }

    @Override
    public void signal(HistoryEvent signalEvent) {
      handleWorkflowExecutionSignaled(signalEvent);
    }

    @Override
    public void cancel(HistoryEvent cancelEvent) {
      handleWorkflowExecutionCancelRequested(cancelEvent);
    }
  }
}
