package io.temporal.internal.replay;

import io.temporal.api.common.v1.Payloads;
import io.temporal.api.query.v1.WorkflowQuery;
import java.io.Closeable;
import java.util.Optional;

/**
 * Minimal interface that the core replay handler needs from the workflow executor. Query execution
 * and close are the only executor methods called directly by the handler; state machine callbacks
 * (start, eventLoop, signal, update, cancel) go through {@link
 * io.temporal.internal.statemachines.StatesMachinesCallback}.
 */
public interface ReplayWorkflowExecutorListener extends Closeable {
  Optional<Payloads> query(WorkflowQuery query);

  void close();
}
