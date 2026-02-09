package io.temporal.internal.worker;

import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponseOrBuilder;
import io.temporal.worker.tuning.SlotPermit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ActivityTask implements ScalingTask {
  private final @Nonnull PollActivityTaskQueueResponseOrBuilder response;
  private final @Nonnull SlotPermit permit;
  private final @Nonnull Runnable completionCallback;

  public ActivityTask(
      @Nonnull PollActivityTaskQueueResponseOrBuilder response,
      @Nonnull SlotPermit permit,
      @Nonnull Runnable completionCallback) {
    this.response = response;
    this.permit = permit;
    this.completionCallback = completionCallback;
  }

  @Nonnull
  public PollActivityTaskQueueResponseOrBuilder getResponse() {
    return response;
  }

  /**
   * Completion handle function that must be called by the handler whenever activity processing is
   * completed.
   */
  @Nonnull
  public Runnable getCompletionCallback() {
    return completionCallback;
  }

  @Nonnull
  public SlotPermit getPermit() {
    return permit;
  }

  @Nullable
  @Override
  public ScalingDecision getScalingDecision() {
    if (!response.hasPollerScalingDecision()) {
      return null;
    }

    return new ScalingTask.ScalingDecision(
        response.getPollerScalingDecision().getPollRequestDeltaSuggestion());
  }
}
