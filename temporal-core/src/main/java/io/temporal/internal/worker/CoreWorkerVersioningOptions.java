package io.temporal.internal.worker;

import javax.annotation.Nullable;

/** Core versioning options containing old and new worker versioning options together. */
public final class CoreWorkerVersioningOptions {
  private final @Nullable String buildId;
  private final boolean useBuildIdForVersioning;
  private final @Nullable CoreWorkerDeploymentOptions workerDeploymentOptions;

  public CoreWorkerVersioningOptions(
      @Nullable String buildId,
      boolean useBuildIdForVersioning,
      @Nullable CoreWorkerDeploymentOptions workerDeploymentOptions) {
    this.buildId = buildId;
    this.useBuildIdForVersioning = useBuildIdForVersioning;
    this.workerDeploymentOptions = workerDeploymentOptions;
  }

  public String getBuildId() {
    if (workerDeploymentOptions != null
        && workerDeploymentOptions.getVersion() != null
        && workerDeploymentOptions.getVersion().getBuildId() != null) {
      return workerDeploymentOptions.getVersion().getBuildId();
    }
    return buildId;
  }

  public boolean isUsingVersioning() {
    return useBuildIdForVersioning
        || (workerDeploymentOptions != null && workerDeploymentOptions.isUsingVersioning());
  }

  @Nullable
  public CoreWorkerDeploymentOptions getWorkerDeploymentOptions() {
    return workerDeploymentOptions;
  }
}
