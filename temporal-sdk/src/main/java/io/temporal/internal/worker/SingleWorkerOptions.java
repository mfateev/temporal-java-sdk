package io.temporal.internal.worker;

import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.temporal.api.common.v1.WorkerVersionStamp;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.GlobalDataConverter;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.worker.WorkerDeploymentOptions;
import java.time.Duration;
import java.util.List;

public final class SingleWorkerOptions {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(SingleWorkerOptions options) {
    return new Builder(options);
  }

  public static final class Builder {

    private String identity;
    private String binaryChecksum;
    private String buildId;
    private boolean useBuildIdForVersioning;
    private DataConverter dataConverter;
    private PollerOptions pollerOptions;
    private Scope metricsScope;
    private boolean enableLoggingInReplay;
    private List<ContextPropagator> contextPropagators;
    private WorkerInterceptor[] workerInterceptors;
    private Duration stickyQueueScheduleToStartTimeout;
    private long defaultDeadlockDetectionTimeout;
    private Duration maxHeartbeatThrottleInterval;
    private Duration defaultHeartbeatThrottleInterval;
    private Duration drainStickyTaskQueueTimeout;
    private boolean usingVirtualThreads;
    private WorkerDeploymentOptions deploymentOptions;

    private Builder() {}

    private Builder(SingleWorkerOptions options) {
      if (options == null) {
        return;
      }
      this.identity = options.getIdentity();
      this.binaryChecksum = options.getBinaryChecksum();
      this.dataConverter = options.getDataConverter();
      this.pollerOptions = options.getPollerOptions();
      this.metricsScope = options.getMetricsScope();
      this.enableLoggingInReplay = options.getEnableLoggingInReplay();
      this.contextPropagators = options.getContextPropagators();
      this.workerInterceptors = options.getWorkerInterceptors();
      this.stickyQueueScheduleToStartTimeout = options.getStickyQueueScheduleToStartTimeout();
      this.defaultDeadlockDetectionTimeout = options.getDefaultDeadlockDetectionTimeout();
      this.maxHeartbeatThrottleInterval = options.getMaxHeartbeatThrottleInterval();
      this.defaultHeartbeatThrottleInterval = options.getDefaultHeartbeatThrottleInterval();
      this.buildId = options.getBuildId();
      this.useBuildIdForVersioning = options.isUsingBuildIdForVersioning();
      this.drainStickyTaskQueueTimeout = options.getDrainStickyTaskQueueTimeout();
      this.usingVirtualThreads = options.isUsingVirtualThreads();
      this.deploymentOptions = options.getDeploymentOptions();
    }

    public Builder setIdentity(String identity) {
      this.identity = identity;
      return this;
    }

    @Deprecated
    public Builder setBinaryChecksum(String binaryChecksum) {
      this.binaryChecksum = binaryChecksum;
      return this;
    }

    public Builder setDataConverter(DataConverter dataConverter) {
      this.dataConverter = dataConverter;
      return this;
    }

    public Builder setPollerOptions(PollerOptions pollerOptions) {
      this.pollerOptions = pollerOptions;
      return this;
    }

    public Builder setMetricsScope(Scope metricsScope) {
      this.metricsScope = metricsScope;
      return this;
    }

    public Builder setEnableLoggingInReplay(boolean enableLoggingInReplay) {
      this.enableLoggingInReplay = enableLoggingInReplay;
      return this;
    }

    /** Specifies the list of context propagators to use during this workflow. */
    public Builder setContextPropagators(List<ContextPropagator> contextPropagators) {
      this.contextPropagators = contextPropagators;
      return this;
    }

    /** Specifies the list of worker interceptors to use during this workflow. */
    public Builder setWorkerInterceptors(WorkerInterceptor[] workerInterceptors) {
      this.workerInterceptors = workerInterceptors;
      return this;
    }

    public Builder setStickyQueueScheduleToStartTimeout(
        Duration stickyQueueScheduleToStartTimeout) {
      this.stickyQueueScheduleToStartTimeout = stickyQueueScheduleToStartTimeout;
      return this;
    }

    public Builder setDefaultDeadlockDetectionTimeout(long defaultDeadlockDetectionTimeout) {
      this.defaultDeadlockDetectionTimeout = defaultDeadlockDetectionTimeout;
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

    public Builder setBuildId(String buildId) {
      this.buildId = buildId;
      return this;
    }

    public Builder setUseBuildIdForVersioning(boolean useBuildIdForVersioning) {
      this.useBuildIdForVersioning = useBuildIdForVersioning;
      return this;
    }

    public Builder setStickyTaskQueueDrainTimeout(Duration drainStickyTaskQueueTimeout) {
      this.drainStickyTaskQueueTimeout = drainStickyTaskQueueTimeout;
      return this;
    }

    public Builder setUsingVirtualThreads(boolean usingVirtualThreads) {
      this.usingVirtualThreads = usingVirtualThreads;
      return this;
    }

    public Builder setDeploymentOptions(WorkerDeploymentOptions deploymentOptions) {
      this.deploymentOptions = deploymentOptions;
      return this;
    }

    public SingleWorkerOptions build() {
      PollerOptions pollerOptions = this.pollerOptions;
      if (pollerOptions == null) {
        pollerOptions = PollerOptions.newBuilder().build();
      }

      DataConverter dataConverter = this.dataConverter;
      if (dataConverter == null) {
        dataConverter = GlobalDataConverter.get();
      }

      Scope metricsScope = this.metricsScope;
      if (metricsScope == null) {
        metricsScope = new NoopScope();
      }

      Duration drainStickyTaskQueueTimeout = this.drainStickyTaskQueueTimeout;
      if (drainStickyTaskQueueTimeout == null) {
        drainStickyTaskQueueTimeout = Duration.ofSeconds(0);
      }

      // Build core deployment options from SDK deployment options.
      CoreWorkerDeploymentOptions coreDeploymentOptions = null;
      if (this.deploymentOptions != null) {
        coreDeploymentOptions =
            CoreWorkerDeploymentOptions.newBuilder()
                .setUseVersioning(this.deploymentOptions.isUsingVersioning())
                .setVersion(this.deploymentOptions.getVersion())
                .setDefaultVersioningBehavior(this.deploymentOptions.getDefaultVersioningBehavior())
                .build();
      }

      // Build core versioning options.
      String effectiveBuildId = this.buildId != null ? this.buildId : this.binaryChecksum;
      CoreWorkerVersioningOptions coreVersioningOptions =
          new CoreWorkerVersioningOptions(
              effectiveBuildId, this.useBuildIdForVersioning, coreDeploymentOptions);

      // Build core single worker options.
      CoreSingleWorkerOptions coreOptions =
          CoreSingleWorkerOptions.newBuilder()
              .setIdentity(this.identity)
              .setBuildId(this.buildId)
              .setPollerOptions(pollerOptions.getCoreOptions())
              .setMetricsScope(metricsScope)
              .setStickyQueueScheduleToStartTimeout(this.stickyQueueScheduleToStartTimeout)
              .setStickyTaskQueueDrainTimeout(drainStickyTaskQueueTimeout)
              .setMaxHeartbeatThrottleInterval(this.maxHeartbeatThrottleInterval)
              .setDefaultHeartbeatThrottleInterval(this.defaultHeartbeatThrottleInterval)
              .setDefaultDeadlockDetectionTimeout(this.defaultDeadlockDetectionTimeout)
              .setEnableLoggingInReplay(this.enableLoggingInReplay)
              .setUsingVirtualThreads(this.usingVirtualThreads)
              .setVersioningOptions(coreVersioningOptions)
              .build();

      return new SingleWorkerOptions(
          coreOptions,
          this.binaryChecksum,
          this.buildId,
          this.useBuildIdForVersioning,
          dataConverter,
          pollerOptions,
          this.contextPropagators,
          this.workerInterceptors,
          this.deploymentOptions);
    }
  }

  private final CoreSingleWorkerOptions coreOptions;
  private final String binaryChecksum;
  private final String buildId;
  private final boolean useBuildIdForVersioning;
  private final DataConverter dataConverter;
  private final PollerOptions pollerOptions;
  private final List<ContextPropagator> contextPropagators;
  private final WorkerInterceptor[] workerInterceptors;
  private final WorkerDeploymentOptions deploymentOptions;

  private SingleWorkerOptions(
      CoreSingleWorkerOptions coreOptions,
      String binaryChecksum,
      String buildId,
      boolean useBuildIdForVersioning,
      DataConverter dataConverter,
      PollerOptions pollerOptions,
      List<ContextPropagator> contextPropagators,
      WorkerInterceptor[] workerInterceptors,
      WorkerDeploymentOptions deploymentOptions) {
    this.coreOptions = coreOptions;
    this.binaryChecksum = binaryChecksum;
    this.buildId = buildId;
    this.useBuildIdForVersioning = useBuildIdForVersioning;
    this.dataConverter = dataConverter;
    this.pollerOptions = pollerOptions;
    this.contextPropagators = contextPropagators;
    this.workerInterceptors = workerInterceptors;
    this.deploymentOptions = deploymentOptions;
  }

  // Infrastructure getters delegate to coreOptions.

  public String getIdentity() {
    return coreOptions.getIdentity();
  }

  public Scope getMetricsScope() {
    return coreOptions.getMetricsScope();
  }

  public Duration getStickyQueueScheduleToStartTimeout() {
    return coreOptions.getStickyQueueScheduleToStartTimeout();
  }

  public Duration getDrainStickyTaskQueueTimeout() {
    return coreOptions.getDrainStickyTaskQueueTimeout();
  }

  public Duration getMaxHeartbeatThrottleInterval() {
    return coreOptions.getMaxHeartbeatThrottleInterval();
  }

  public Duration getDefaultHeartbeatThrottleInterval() {
    return coreOptions.getDefaultHeartbeatThrottleInterval();
  }

  public long getDefaultDeadlockDetectionTimeout() {
    return coreOptions.getDefaultDeadlockDetectionTimeout();
  }

  public boolean isUsingVirtualThreads() {
    return coreOptions.isUsingVirtualThreads();
  }

  // SDK-specific getters stay as direct field access.

  @Deprecated
  public String getBinaryChecksum() {
    return binaryChecksum;
  }

  public String getBuildId() {
    if (buildId == null) {
      return binaryChecksum;
    }
    return buildId;
  }

  public boolean isUsingBuildIdForVersioning() {
    return useBuildIdForVersioning;
  }

  public DataConverter getDataConverter() {
    return dataConverter;
  }

  public PollerOptions getPollerOptions() {
    return pollerOptions;
  }

  public boolean getEnableLoggingInReplay() {
    return coreOptions.getEnableLoggingInReplay();
  }

  public List<ContextPropagator> getContextPropagators() {
    return contextPropagators;
  }

  public WorkerInterceptor[] getWorkerInterceptors() {
    return workerInterceptors;
  }

  public WorkerDeploymentOptions getDeploymentOptions() {
    return deploymentOptions;
  }

  public WorkerVersionStamp workerVersionStamp() {
    return WorkerVersionStamp.newBuilder()
        .setBuildId(this.getBuildId())
        .setUseVersioning(this.isUsingBuildIdForVersioning())
        .build();
  }

  public WorkerVersioningOptions getWorkerVersioningOptions() {
    return new WorkerVersioningOptions(
        this.getBuildId(), this.isUsingBuildIdForVersioning(), this.getDeploymentOptions());
  }

  public CoreSingleWorkerOptions getCoreOptions() {
    return coreOptions;
  }
}
