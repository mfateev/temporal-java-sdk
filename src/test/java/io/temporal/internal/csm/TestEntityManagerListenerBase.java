package io.temporal.internal.csm;

import io.temporal.api.history.v1.HistoryEvent;

abstract class TestEntityManagerListenerBase implements EntityManagerListener {

  boolean invoked;

  @Override
  public void start(HistoryEvent startWorkflowEvent) {}

  @Override
  public void signal(HistoryEvent signalEvent) {}

  @Override
  public void cancel(HistoryEvent cancelEvent) {}

  @Override
  public final void eventLoop() {
    if (invoked) {
      return;
    }
    invoked = true;
    eventLoopImpl();
  }

  protected abstract void eventLoopImpl();
}
