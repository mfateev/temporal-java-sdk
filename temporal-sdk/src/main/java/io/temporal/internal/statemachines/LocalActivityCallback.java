package io.temporal.internal.statemachines;

import io.temporal.api.common.v1.Payloads;
import io.temporal.api.failure.v1.Failure;
import java.time.Duration;
import java.util.Optional;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@FunctionalInterface
public interface LocalActivityCallback
    extends BiConsumer<Optional<Payloads>, LocalActivityCallback.LocalActivityFailedException> {

  @Override
  void accept(Optional<Payloads> successOutput, LocalActivityFailedException exception);

  class LocalActivityFailedException extends RuntimeException {
    private final @Nonnull Failure failure;
    private final int lastAttempt;

    /**
     * If this is not null, code that processes this exception will schedule a workflow timer to
     * continue retrying the execution
     */
    private final @Nullable Duration backoff;

    public LocalActivityFailedException(
        @Nonnull Failure failure,
        long originalScheduledTimestamp,
        int lastAttempt,
        @Nullable Duration backoff) {
      this.failure = failure;
      this.lastAttempt = lastAttempt;
      this.backoff = backoff;
    }

    @Nonnull
    public Failure getFailure() {
      return failure;
    }

    public int getLastAttempt() {
      return lastAttempt;
    }

    @Nullable
    public Duration getBackoff() {
      return backoff;
    }
  }
}
