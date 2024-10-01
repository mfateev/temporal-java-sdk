package io.temporal.internal.replay;

public class ResetWorkflowTaskError extends Error {

  private final String resetReason;
  private final long resetEventId;

  public ResetWorkflowTaskError(String resetReason, long eventId) {
    this.resetReason = resetReason;
    this.resetEventId = eventId;
  }

  public String getResetReason() {
    return resetReason;
  }

  public long getResetEventId() {
    return resetEventId;
  }
}
