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
import io.temporal.internal.common.OptionsUtils;
import io.temporal.internal.common.RpcRetryOptions;
import io.temporal.internal.csm.CommandsManager;
import io.temporal.internal.csm.CommandsManagerListener;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.internal.replay.HistoryHelper.WorkflowTaskEvents;
import io.temporal.internal.worker.LocalActivityWorker;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.internal.worker.WorkflowExecutionException;
import io.temporal.internal.worker.WorkflowTaskWithHistoryIterator;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.Functions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
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

  /**
   * The eventId of the last event in the history which is expected to be startedEventId unless it
   * is replay from a JSON file.
   */
  private final long workflowTaskStartedEventId;

  /** The eventId of the started event of the last successfully executed workflow task. */
  private final long previousStartedEventId;

  private boolean cancelRequested;

  private boolean completed;

  private WorkflowExecutionException failure;

  private long wakeUpTime;

  private Consumer<Exception> timerCancellationHandler;

  private final Scope metricsScope;

  private final long wfStartTimeNanos;

  private final WorkflowExecutionStartedEventAttributes startedEvent;

  private final Lock lock = new ReentrantLock();

  private final Consumer<HistoryEvent> localActivityCompletionSink;

  private final Map<String, WorkflowQueryResult> queryResults = new HashMap<>();

  private final DataConverter converter;

  private final CommandsManager commandsManager;

  private final HistoryEvent firstEvent;

  ReplayWorkflowExecutor(
      WorkflowServiceStubs service,
      String namespace,
      ReplayWorkflow workflow,
      PollWorkflowTaskQueueResponse.Builder workflowTask,
      SingleWorkerOptions options,
      Scope metricsScope,
      BiFunction<LocalActivityWorker.Task, Duration, Boolean> laTaskPoller) {
    this.service = service;
    this.workflow = workflow;
    firstEvent = workflowTask.getHistory().getEvents(0);
    workflowTaskStartedEventId = workflowTask.getStartedEventId();
    previousStartedEventId = workflowTask.getPreviousStartedEventId();

    if (!firstEvent.hasWorkflowExecutionStartedEventAttributes()) {
      throw new IllegalArgumentException(
          "First event in the history is not WorkflowExecutionStarted");
    }
    startedEvent = firstEvent.getWorkflowExecutionStartedEventAttributes();
    this.commandsManager =
        new CommandsManager(
            previousStartedEventId, workflowTaskStartedEventId, new CommandsManagerListenerImpl());
    this.metricsScope = metricsScope;
    this.converter = options.getDataConverter();

    wfStartTimeNanos = firstEvent.getTimestamp();

    context =
        new ReplayWorkflowContextImpl(
            commandsManager,
            namespace,
            startedEvent,
            workflowTask.getWorkflowExecution(),
            Duration.ofNanos(firstEvent.getTimestamp()).toMillis(),
            options,
            metricsScope,
            laTaskPoller,
            this);

    localActivityCompletionSink =
        historyEvent -> {
          lock.lock();
          try {
            handleEvent(historyEvent);
          } finally {
            lock.unlock();
          }
        };
  }

  Lock getLock() {
    return lock;
  }

  private void handleWorkflowExecutionStarted(HistoryEvent event) {
    workflow.start(event, context);
  }

  private void handleEvent(HistoryEvent event) {
    System.out.println("handleEvent " + event.getEventId() + ": " + event.getEventType());
    commandsManager.handleEvent(event);
  }

  private void eventLoop() {
    System.out.println("EVENT LOOP");

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
  }

  private void mayBeCompleteWorkflow() {
    if (completed) {
      completeWorkflow();
    }
  }

  private void completeWorkflow() {
    if (failure != null) {
      commandsManager.newFailWorkflow(failure.getFailure());
      metricsScope.counter(MetricsType.WORKFLOW_FAILED_COUNTER).inc(1);
    } else if (cancelRequested) {
      commandsManager.newCancelWorkflow();
      metricsScope.counter(MetricsType.WORKFLOW_CANCELLED_COUNTER).inc(1);
    } else {
      ContinueAsNewWorkflowExecutionCommandAttributes attributes =
          context.getContinueAsNewOnCompletion();
      if (attributes != null) {
        commandsManager.newContinueAsNewWorkflow(attributes);
        metricsScope.counter(MetricsType.WORKFLOW_CONTINUE_AS_NEW_COUNTER).inc(1);
      } else {
        Optional<Payloads> workflowOutput = workflow.getOutput();
        commandsManager.newCompleteWorkflow(workflowOutput);
        metricsScope.counter(MetricsType.WORKFLOW_COMPLETED_COUNTER).inc(1);
      }
    }

    long nanoTime = TimeUnit.NANOSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    com.uber.m3.util.Duration d = com.uber.m3.util.Duration.ofNanos(nanoTime - wfStartTimeNanos);
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
  public WorkflowTaskResult handleWorkflowTask(PollWorkflowTaskQueueResponseOrBuilder workflowTask)
      throws Throwable {
    lock.lock();
    try {
      queryResults.clear();
      boolean forceCreateNewWorkflowTask = handleWorkflowTaskImpl(workflowTask, null);
      List<Command> commands = commandsManager.takeCommands();
      System.out.println("Completed workflow task with commands: " + commands);

      return new WorkflowTaskResult(commands, queryResults, forceCreateNewWorkflowTask, completed);
    } finally {
      lock.unlock();
    }
  }

  // Returns boolean to indicate whether we need to force create new workflow task for local
  // activity heartbeating.
  private boolean handleWorkflowTaskImpl(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, Functions.Proc legacyQueryCallback)
      throws Throwable {
    System.out.println(
        "handleWorkflowTaskImpl workflowType=" + workflowTask.getWorkflowType().getName());
    boolean forceCreateNewWorkflowTask = false;
    Stopwatch sw = metricsScope.timer(MetricsType.WORKFLOW_TASK_REPLAY_LATENCY).start();
    boolean timerStopped = false;
    try {
      long startTime = System.currentTimeMillis();
      WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator =
          new WorkflowTaskWithHistoryIteratorImpl(
              workflowTask, Duration.ofSeconds(startedEvent.getWorkflowTaskTimeoutSeconds()));
      Iterator<HistoryEvent> iterator = workflowTaskWithHistoryIterator.getHistory();
      //      handleWorkflowExecutionStarted(firstEvent);
      //      while (iterator.hasNext()) {
      //        if (!timerStopped && !taskEvents.isReplay()) {
      //          sw.stop();
      //          timerStopped = true;
      //        }

      // Markers must be cached first as their data is needed when processing events.
      // TODO(maxim): Markers
      //        for (HistoryEvent event : taskEvents.getMarkers()) {
      //          if (!event
      //              .getMarkerRecordedEventAttributes()
      //              .getMarkerName()
      //              .equals(ReplayClockContext.LOCAL_ACTIVITY_MARKER_NAME)) {
      //            handleEvent(event);
      //          }
      //        }

      //        System.out.println("\n\nTASK_EVENTS: " + taskEvents);
      while (iterator.hasNext()) {
        HistoryEvent event = iterator.next();
        handleEvent(event);
      }
      //        eventLoop();
      //        forceCreateNewWorkflowTask =
      //            processEventLoop(
      //                startTime,
      //                startedEvent.getWorkflowTaskTimeoutSeconds(),
      //                taskEvents,
      //                workflowTask.hasQuery());

      mayBeCompleteWorkflow();
      //      }
      return false; // forceCreateNewWorkflowTask;
    } catch (Error e) {
      WorkflowImplementationOptions implementationOptions =
          this.workflow.getWorkflowImplementationOptions();
      if (implementationOptions.getWorkflowErrorPolicy() == FailWorkflow) {
        // fail workflow
        failure = workflow.mapError(e);
        completed = true;
        completeWorkflow();
        return false;
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
      System.out.println(
          "done handleWorkflowTaskImpl workflowType=" + workflowTask.getWorkflowType().getName());
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
    if (commandsManager.getLastStartedEventId() > 0
        && commandsManager.getLastStartedEventId() != historyHelper.getPreviousStartedEventId()
        && workflowTask.getHistory().getEventsCount() > 0) {
      throw new IllegalStateException(
          String.format(
              "ReplayWorkflowExecutor processed up to event id %d. History's previous started event id is %d",
              commandsManager.getLastStartedEventId(), historyHelper.getPreviousStartedEventId()));
    }
    return iterator;
  }

  private HistoryHelper newHistoryHelper(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator =
        new WorkflowTaskWithHistoryIteratorImpl(
            workflowTask, Duration.ofSeconds(startedEvent.getWorkflowTaskTimeoutSeconds()));
    return new HistoryHelper(
        workflowTaskWithHistoryIterator, context.getReplayCurrentTimeMilliseconds());
  }

  private boolean processEventLoop(
      long startTime, int workflowTaskTimeoutSecs, WorkflowTaskEvents taskEvents, boolean isQuery)
      throws Throwable {
    eventLoop();

    if (taskEvents.isReplay() || isQuery) {
      return replayLocalActivities(taskEvents);
    } else {
      return executeLocalActivities(startTime, workflowTaskTimeoutSecs);
    }
  }

  private boolean replayLocalActivities(WorkflowTaskEvents taskEvents) throws Throwable {
    List<HistoryEvent> localActivityMarkers = new ArrayList<>();
    for (HistoryEvent event : taskEvents.getMarkers()) {
      if (event
          .getMarkerRecordedEventAttributes()
          .getMarkerName()
          .equals(ReplayClockContext.LOCAL_ACTIVITY_MARKER_NAME)) {
        localActivityMarkers.add(event);
      }
    }

    if (localActivityMarkers.isEmpty()) {
      return false;
    }

    int processed = 0;
    while (context.numPendingLaTasks() > 0) {
      int numTasks = context.numPendingLaTasks();
      for (HistoryEvent event : localActivityMarkers) {
        handleEvent(event);
      }

      eventLoop();

      processed += numTasks;
      if (processed == localActivityMarkers.size()) {
        return false;
      }
    }
    return false;
  }

  // Return whether we would need a new workflow task immediately.
  private boolean executeLocalActivities(long startTime, int workflowTaskTimeoutSecs) {
    Duration maxProcessingTime = Duration.ofSeconds((long) (0.8 * workflowTaskTimeoutSecs));

    while (context.numPendingLaTasks() > 0) {
      Duration processingTime = Duration.ofMillis(System.currentTimeMillis() - startTime);
      Duration maxWaitAllowed = maxProcessingTime.minus(processingTime);

      boolean started = context.startUnstartedLaTasks(maxWaitAllowed);
      if (!started) {
        // We were not able to send the current batch of la tasks before deadline.
        // Return true to indicate that we need a new workflow task immediately.
        return true;
      }

      try {
        context.awaitTaskCompletion(maxWaitAllowed);
      } catch (InterruptedException e) {
        return true;
      }

      eventLoop();

      if (context.numPendingLaTasks() == 0) {
        return false;
      }

      // Break local activity processing loop if we almost reach workflow task timeout.
      processingTime = Duration.ofMillis(System.currentTimeMillis() - startTime);
      if (processingTime.compareTo(maxProcessingTime) > 0) {
        return true;
      }
    }
    return false;
  }

  int getWorkflowTaskTimeoutSeconds() {
    return startedEvent.getWorkflowTaskTimeoutSeconds();
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
      PollWorkflowTaskQueueResponseOrBuilder response, WorkflowQuery query) throws Throwable {
    lock.lock();
    try {
      AtomicReference<Optional<Payloads>> result = new AtomicReference<>();
      handleWorkflowTaskImpl(response, () -> result.set(workflow.query(query)));
      return result.get();
    } finally {
      lock.unlock();
    }
  }

  public Consumer<HistoryEvent> getLocalActivityCompletionSink() {
    return localActivityCompletionSink;
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
      System.out.println("WORKFLOW START");
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
