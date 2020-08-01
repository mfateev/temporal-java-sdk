package io.temporal.internal.csm;

import io.temporal.api.history.v1.HistoryEvent;

class TestEntityManagerListenerBase implements EntityManagerListener {
  @Override
  public void start(HistoryEvent startWorkflowEvent) {}

  @Override
  public void signal(HistoryEvent signalEvent) {}

  @Override
  public void cancel(HistoryEvent cancelEvent) {}

  @Override
  public void eventLoop() {}
}
