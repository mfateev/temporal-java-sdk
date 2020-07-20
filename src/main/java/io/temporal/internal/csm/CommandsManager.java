package io.temporal.internal.csm;

import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.RecordMarkerCommandAttributes;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.command.v1.UpsertWorkflowSearchAttributesCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.TimerCanceledEventAttributes;
import io.temporal.api.history.v1.TimerFiredEventAttributes;
import io.temporal.api.history.v1.WorkflowExecutionCancelRequestedEventAttributes;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.Functions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

public final class CommandsManager {

  private final Functions.Proc1<HistoryEvent> signalCallback;
  private final Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancelCallback;
  private Functions.Proc1<NewCommand> sink;

  private final Map<Long, CommandsBase> commands = new HashMap<>();

  /** Commands generated by the currently processed workflow task. */
  private final List<NewCommand> newCommands = new ArrayList<>();

  private long lastStartedEventId;

  public CommandsManager(
      Functions.Proc1<HistoryEvent> signalCallback,
      Functions.Proc1<WorkflowExecutionCancelRequestedEventAttributes> cancelCallback) {
    sink = (command) -> newCommands.add(command);
    this.signalCallback = Objects.requireNonNull(signalCallback);
    this.cancelCallback = Objects.requireNonNull(cancelCallback);
  }

  public final void handleEvent(HistoryEvent event) {
    Long initialCommandEventId = getInitialCommandEventId(event);
    CommandsBase c = commands.get(initialCommandEventId);
    if (c != null) {
      c.handleEvent(event);
      if (c.isFinalState()) {
        commands.remove(initialCommandEventId);
      }
    } else {
      handleNonStatefulEvent(event);
    }
  }

  private void handleNonStatefulEvent(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
        signalCallback.apply(event);
        break;
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        cancelCallback.apply(event.getWorkflowExecutionCancelRequestedEventAttributes());
        break;
      default:
        // TODO(maxim)
    }
  }

  public long getLastStartedEventId() {
    return lastStartedEventId;
  }

  public void setLastStartedEventId(long lastStartedEventId) {
    this.lastStartedEventId = lastStartedEventId;
  }

  public List<Command> takeCommands() {
    // Account for workflow task completed
    long commandEventId = lastStartedEventId + 2;
    List<Command> result = new ArrayList<>(newCommands.size());
    for (NewCommand newCommand : newCommands) {
      Optional<Command> command = newCommand.getCommand();
      if (command.isPresent()) {
        result.add(command.get());
        newCommand.setInitialCommandEventId(commandEventId++);
        commands.put(commandEventId, newCommand.getCommands());
        commandEventId++;
      }
    }
    commands.clear();
    return result;
  }

  /**
   * @param attributes attributes used to schedule an activity
   * @param completionCallback one of ActivityTaskCompletedEvent, ActivityTaskFailedEvent,
   *     ActivityTaskTimedOutEvent, ActivityTaskCanceledEvents
   * @return an instance of ActivityCommands
   */
  public ActivityCommands newActivity(
      ScheduleActivityTaskCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    return ActivityCommands.newInstance(attributes, completionCallback, sink);
  }

  /**
   * Creates a new timer state machine
   *
   * @param attributes timer command attributes
   * @param completionCallback invoked when timer fires or reports cancellation
   * @return cancellation callback that should be invoked to initiate timer cancellation
   */
  public Functions.Proc newTimer(
      StartTimerCommandAttributes attributes,
      Functions.Proc2<TimerFiredEventAttributes, TimerCanceledEventAttributes> completionCallback) {
    TimerCommands timer = TimerCommands.newInstance(attributes, completionCallback, sink);
    return () -> timer.cancel();
  }

  /**
   * Creates a new child state machine
   *
   * @param attributes child workflow start command attributes
   * @param completionCallback invoked when child reports completion or failure. The following types
   *     of events can be passed to the callback: StartChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionCompletedEvent, ChildWorkflowExecutionFailedEvent,
   *     ChildWorkflowExecutionTimedOutEvent, ChildWorkflowExecutionCanceledEvent,
   *     ChildWorkflowExecutionTerminatedEvent.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public Functions.Proc1<ChildWorkflowCancellationType> newChildWorkflow(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    ChildWorkflowCommands child =
        ChildWorkflowCommands.newInstance(attributes, completionCallback, sink);
    return (cancellationType) -> child.cancel(cancellationType);
  }

  /**
   * @param attributes
   * @param completionCallback invoked when signal delivery completes of fails. The following types
   *     of events can be passed to the callback: ExternalWorkflowExecutionSignaledEvent,
   *     SignalExternalWorkflowExecutionFailedEvent>
   */
  public Functions.Proc newSignalExternal(
      SignalExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc2<HistoryEvent, Boolean> completionCallback) {
    return SignalExternalCommands.newInstance(attributes, completionCallback, sink);
  }

  /**
   * @param attributes attributes to use to cancel external worklfow
   * @param completionCallback one of ExternalWorkflowExecutionCancelRequestedEvent,
   *     RequestCancelExternalWorkflowExecutionFailedEvent
   */
  public void newCancelExternal(
      RequestCancelExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc1<HistoryEvent> completionCallback) {
    CancelExternalCommands.newInstance(attributes, completionCallback, sink);
  }

  public void newUpsertSearchAttributes(
      UpsertWorkflowSearchAttributesCommandAttributes attributes) {
    UpsertSearchAttributesCommands.newInstance(attributes, sink);
  }

  public void newMarker(RecordMarkerCommandAttributes attributes) {
    MarkerCommands.newInstance(attributes, sink);
  }

  public void newCompleteWorkflow(Optional<Payloads> workflowOutput) {
    CompleteWorkflowCommands.newInstance(workflowOutput, sink);
  }

  public void newFailWorkflow(Failure failure) {
    FailWorkflowCommands.newInstance(failure, sink);
  }

  public void newCancelWorkflow() {
    CancelWorkflowCommands.newInstance(
        CancelWorkflowExecutionCommandAttributes.getDefaultInstance(), sink);
  }

  public void newContinueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    ContinueAsNewWorkflowCommands.newInstance(attributes, sink);
  }

  private long getInitialCommandEventId(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_ACTIVITY_TASK_STARTED:
        return event.getActivityTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        return event.getActivityTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_FAILED:
        return event.getActivityTaskFailedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        return event.getActivityTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        return event.getActivityTaskCancelRequestedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_REQUEST_CANCEL_ACTIVITY_TASK_FAILED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
      case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        return event.getActivityTaskCanceledEventAttributes().getScheduledEventId();
      case EVENT_TYPE_TIMER_FIRED:
        return event.getTimerFiredEventAttributes().getStartedEventId();
      case EVENT_TYPE_CANCEL_TIMER_FAILED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
      case EVENT_TYPE_TIMER_CANCELED:
        return event.getTimerCanceledEventAttributes().getStartedEventId();
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getRequestCancelExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        return event
            .getExternalWorkflowExecutionCancelRequestedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getStartChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
        return event.getChildWorkflowExecutionStartedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
        return event.getChildWorkflowExecutionCompletedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
        return event.getChildWorkflowExecutionCanceledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
        return event.getChildWorkflowExecutionTimedOutEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
        return event.getChildWorkflowExecutionTerminatedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getSignalExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
        return event.getExternalWorkflowExecutionSignaledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_WORKFLOW_TASK_STARTED:
        return event.getWorkflowTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
        return event.getWorkflowTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
        return event.getWorkflowTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      case EVENT_TYPE_TIMER_STARTED:
      case EVENT_TYPE_MARKER_RECORDED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
      case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
      case EVENT_TYPE_WORKFLOW_TASK_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
        return event.getEventId();
      case UNRECOGNIZED:
      case EVENT_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
    throw new IllegalStateException("unreachable");
  }

  public Long currentTimeMillis() {
    throw new UnsupportedOperationException("not implemented");
  }

  public UUID randomUUID() {
    throw new UnsupportedOperationException("not implemented");
  }

  public Random newRandom() {
    throw new UnsupportedOperationException("not implemented");
  }
}
