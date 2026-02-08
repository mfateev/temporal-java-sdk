package io.temporal.internal.worker;

import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import javax.annotation.Nullable;

/**
 * Core single worker options containing infrastructure fields shared between Java and Kotlin SDKs.
 */
public final class CoreSingleWorkerOptions {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(CoreSingleWorkerOptions options) {
    return new Builder(options);
  }

  public static final class Builder {

    private String identity;
    private CorePollerOptions pollerOptions;
    private Scope metricsScope;
    private Duration stickyQueueScheduleToStartTimeout;
    private Duration drainStickyTaskQueueTimeout;
    private Duration maxHeartbeatThrottleInterval;
    private Duration defaultHeartbeatThrottleInterval;
    private long defaultDeadlockDetectionTimeout;
    private boolean usingVirtualThreads;
    private CoreWorkerVersioningOptions versioningOptions;

    private Builder() {}

    private Builder(CoreSingleWorkerOptions options) {
      if (options == null) {
        return;
      }
      this.identity = options.identity;
      this.pollerOptions = options.pollerOptions;
      this.metricsScope = options.metricsScope;
      this.stickyQueueScheduleToStartTimeout = options.stickyQueueScheduleToStartTimeout;
      this.drainStickyTaskQueueTimeout = options.drainStickyTaskQueueTimeout;
      this.maxHeartbeatThrottleInterval = options.maxHeartbeatThrottleInterval;
      this.defaultHeartbeatThrottleInterval = options.defaultHeartbeatThrottleInterval;
      this.defaultDeadlockDetectionTimeout = options.defaultDeadlockDetectionTimeout;
      this.usingVirtualThreads = options.usingVirtualThreads;
      this.versioningOptions = options.versioningOptions;
    }

    public Builder setIdentity(String identity) {
      this.identity = identity;
      return this;
    }

    public Builder setPollerOptions(CorePollerOptions pollerOptions) {
      this.pollerOptions = pollerOptions;
      return this;
    }

    public Builder setMetricsScope(Scope metricsScope) {
      this.metricsScope = metricsScope;
      return this;
    }

    public Builder setStickyQueueScheduleToStartTimeout(
        Duration stickyQueueScheduleToStartTimeout) {
      this.stickyQueueScheduleToStartTimeout = stickyQueueScheduleToStartTimeout;
      return this;
    }

    public Builder setStickyTaskQueueDrainTimeout(Duration drainStickyTaskQueueTimeout) {
      this.drainStickyTaskQueueTimeout = drainStickyTaskQueueTimeout;
      return this;
    }

    public Builder setMaxHeartbeatThrottleInterval(Duration maxHeartbeatThrottleInterval) {
      this.maxHeartbeatThrottleInterval = maxHeartbeatThrottleInterval;
      return this;
    }

    public Builder setDefaultHeartbeatThrottleInterval(Duration defaultHeartbeatThrottleInterval) {
      this.defaultHeartbeatThrottleInterval = defaultHeartbeatThrottleInterval;
      return this;
    }

    public Builder setDefaultDeadlockDetectionTimeout(long defaultDeadlockDetectionTimeout) {
      this.defaultDeadlockDetectionTimeout = defaultDeadlockDetectionTimeout;
      return this;
    }

    public Builder setUsingVirtualThreads(boolean usingVirtualThreads) {
      this.usingVirtualThreads = usingVirtualThreads;
      return this;
    }

    public Builder setVersioningOptions(CoreWorkerVersioningOptions versioningOptions) {
      this.versioningOptions = versioningOptions;
      return this;
    }

    public CoreSingleWorkerOptions build() {
      CorePollerOptions pollerOptions = this.pollerOptions;
      if (pollerOptions == null) {
        pollerOptions = CorePollerOptions.newBuilder().build();
      }

      Scope metricsScope = this.metricsScope;
      if (metricsScope == null) {
        metricsScope = new NoopScope();
      }

      Duration drainStickyTaskQueueTimeout = this.drainStickyTaskQueueTimeout;
      if (drainStickyTaskQueueTimeout == null) {
        drainStickyTaskQueueTimeout = Duration.ofSeconds(0);
      }

      return new CoreSingleWorkerOptions(
          this.identity,
          pollerOptions,
          metricsScope,
          this.stickyQueueScheduleToStartTimeout,
          drainStickyTaskQueueTimeout,
          this.maxHeartbeatThrottleInterval,
          this.defaultHeartbeatThrottleInterval,
          this.defaultDeadlockDetectionTimeout,
          this.usingVirtualThreads,
          this.versioningOptions);
    }
  }

  private final String identity;
  private final CorePollerOptions pollerOptions;
  private final Scope metricsScope;
  private final Duration stickyQueueScheduleToStartTimeout;
  private final Duration drainStickyTaskQueueTimeout;
  private final Duration maxHeartbeatThrottleInterval;
  private final Duration defaultHeartbeatThrottleInterval;
  private final long defaultDeadlockDetectionTimeout;
  private final boolean usingVirtualThreads;
  private final CoreWorkerVersioningOptions versioningOptions;

  private CoreSingleWorkerOptions(
      String identity,
      CorePollerOptions pollerOptions,
      Scope metricsScope,
      Duration stickyQueueScheduleToStartTimeout,
      Duration drainStickyTaskQueueTimeout,
      Duration maxHeartbeatThrottleInterval,
      Duration defaultHeartbeatThrottleInterval,
      long defaultDeadlockDetectionTimeout,
      boolean usingVirtualThreads,
      CoreWorkerVersioningOptions versioningOptions) {
    this.identity = identity;
    this.pollerOptions = pollerOptions;
    this.metricsScope = metricsScope;
    this.stickyQueueScheduleToStartTimeout = stickyQueueScheduleToStartTimeout;
    this.drainStickyTaskQueueTimeout = drainStickyTaskQueueTimeout;
    this.maxHeartbeatThrottleInterval = maxHeartbeatThrottleInterval;
    this.defaultHeartbeatThrottleInterval = defaultHeartbeatThrottleInterval;
    this.defaultDeadlockDetectionTimeout = defaultDeadlockDetectionTimeout;
    this.usingVirtualThreads = usingVirtualThreads;
    this.versioningOptions = versioningOptions;
  }

  public String getIdentity() {
    return identity;
  }

  public CorePollerOptions getPollerOptions() {
    return pollerOptions;
  }

  public Scope getMetricsScope() {
    return metricsScope;
  }

  public Duration getStickyQueueScheduleToStartTimeout() {
    return stickyQueueScheduleToStartTimeout;
  }

  public Duration getDrainStickyTaskQueueTimeout() {
    return drainStickyTaskQueueTimeout;
  }

  public Duration getMaxHeartbeatThrottleInterval() {
    return maxHeartbeatThrottleInterval;
  }

  public Duration getDefaultHeartbeatThrottleInterval() {
    return defaultHeartbeatThrottleInterval;
  }

  public long getDefaultDeadlockDetectionTimeout() {
    return defaultDeadlockDetectionTimeout;
  }

  public boolean isUsingVirtualThreads() {
    return usingVirtualThreads;
  }

  @Nullable
  public CoreWorkerVersioningOptions getVersioningOptions() {
    return versioningOptions;
  }
}
