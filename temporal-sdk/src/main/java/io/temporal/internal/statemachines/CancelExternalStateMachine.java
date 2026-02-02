package io.temporal.internal.statemachines;

import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.workflow.CancelExternalWorkflowException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class CancelExternalStateMachine
    extends EntityStateMachineInitialCommand<
        CancelExternalStateMachine.State,
        CancelExternalStateMachine.ExplicitEvent,
        CancelExternalStateMachine> {

  private final RequestCancelExternalWorkflowExecutionCommandAttributes requestCancelAttributes;

  private final BiConsumer<Void, RuntimeException> completionCallback;

  /**
   * @param attributes attributes to use to cancel external workflow
   * @param completionCallback one of ExternalWorkflowExecutionCancelRequestedEvent,
   *     RequestCancelExternalWorkflowExecutionFailedEvent
   * @param commandSink sink to send commands
   */
  public static void newInstance(
      RequestCancelExternalWorkflowExecutionCommandAttributes attributes,
      BiConsumer<Void, RuntimeException> completionCallback,
      Consumer<CancellableCommand> commandSink,
      Consumer<StateMachine> stateMachineSink) {
    new CancelExternalStateMachine(attributes, completionCallback, commandSink, stateMachineSink);
  }

  private CancelExternalStateMachine(
      RequestCancelExternalWorkflowExecutionCommandAttributes requestCancelAttributes,
      BiConsumer<Void, RuntimeException> completionCallback,
      Consumer<CancellableCommand> commandSink,
      Consumer<StateMachine> stateMachineSink) {
    super(STATE_MACHINE_DEFINITION, commandSink, stateMachineSink);
    this.requestCancelAttributes = requestCancelAttributes;
    this.completionCallback = completionCallback;
    explicitEvent(ExplicitEvent.SCHEDULE);
  }

  enum ExplicitEvent {
    SCHEDULE
  }

  enum State {
    CREATED,
    REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
    REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
    CANCEL_REQUESTED,
    REQUEST_CANCEL_FAILED,
  }

  public static final StateMachineDefinition<State, ExplicitEvent, CancelExternalStateMachine>
      STATE_MACHINE_DEFINITION =
          StateMachineDefinition.<State, ExplicitEvent, CancelExternalStateMachine>newInstance(
                  "CancelExternal",
                  State.CREATED,
                  State.CANCEL_REQUESTED,
                  State.REQUEST_CANCEL_FAILED)
              .add(
                  State.CREATED,
                  ExplicitEvent.SCHEDULE,
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
                  CancelExternalStateMachine::createCancelExternalCommand)
              .add(
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
                  CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED)
              .add(
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_CREATED,
                  EventType.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
                  EntityStateMachineInitialCommand::setInitialCommandEventId)
              .add(
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
                  EventType.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
                  State.CANCEL_REQUESTED,
                  CancelExternalStateMachine::notifyCompleted)
              .add(
                  State.REQUEST_CANCEL_EXTERNAL_COMMAND_RECORDED,
                  EventType.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
                  State.REQUEST_CANCEL_FAILED,
                  CancelExternalStateMachine::notifyFailed);

  private void createCancelExternalCommand() {
    addCommand(
        Command.newBuilder()
            .setCommandType(CommandType.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION)
            .setRequestCancelExternalWorkflowExecutionCommandAttributes(requestCancelAttributes)
            .build());
  }

  private void notifyCompleted() {
    completionCallback.accept(null, null);
  }

  private void notifyFailed() {
    WorkflowExecution execution =
        WorkflowExecution.newBuilder()
            .setWorkflowId(requestCancelAttributes.getWorkflowId())
            .setRunId(requestCancelAttributes.getRunId())
            .build();
    completionCallback.accept(
        null,
        new CancelExternalWorkflowException(
            "Workflow not found: " + execution, execution, "", null));
  }
}
