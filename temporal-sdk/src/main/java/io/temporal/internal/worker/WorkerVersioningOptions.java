package io.temporal.internal.worker;

import io.temporal.worker.WorkerDeploymentOptions;
import javax.annotation.Nullable;

/** Contains old and new worker versioning options together. */
public final class WorkerVersioningOptions {
  private final @Nullable WorkerDeploymentOptions workerDeploymentOptions;
  private final CoreWorkerVersioningOptions coreOptions;

  public WorkerVersioningOptions(
      @Nullable String buildId,
      boolean useBuildIdForVersioning,
      @Nullable WorkerDeploymentOptions workerDeploymentOptions) {
    this.workerDeploymentOptions = workerDeploymentOptions;
    CoreWorkerDeploymentOptions coreDeploymentOptions = null;
    if (workerDeploymentOptions != null) {
      coreDeploymentOptions =
          CoreWorkerDeploymentOptions.newBuilder()
              .setUseVersioning(workerDeploymentOptions.isUsingVersioning())
              .setVersion(workerDeploymentOptions.getVersion())
              .setDefaultVersioningBehavior(workerDeploymentOptions.getDefaultVersioningBehavior())
              .build();
    }
    this.coreOptions =
        new CoreWorkerVersioningOptions(buildId, useBuildIdForVersioning, coreDeploymentOptions);
  }

  public String getBuildId() {
    return coreOptions.getBuildId();
  }

  public boolean isUsingVersioning() {
    return coreOptions.isUsingVersioning();
  }

  @Nullable
  public WorkerDeploymentOptions getWorkerDeploymentOptions() {
    return workerDeploymentOptions;
  }

  public CoreWorkerVersioningOptions getCoreOptions() {
    return coreOptions;
  }
}
