package io.temporal.internal.replay;

public class ResetWorkflowTaskError extends Error {

  private final String resetReason;
  private final long workflowTaskFinishEventId;

  public ResetWorkflowTaskError(String resetReason, long eventId) {
    this.resetReason = resetReason;
    this.workflowTaskFinishEventId = eventId;
  }

  public String getResetReason() {
    return resetReason;
  }

  public long getWorkflowTaskFinishEventId() {
    return workflowTaskFinishEventId;
  }
}
