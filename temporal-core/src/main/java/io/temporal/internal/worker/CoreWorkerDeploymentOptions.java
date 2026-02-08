package io.temporal.internal.worker;

import com.google.common.base.Preconditions;
import io.temporal.common.Experimental;
import io.temporal.common.VersioningBehavior;
import io.temporal.common.WorkerDeploymentVersion;
import java.util.Objects;
import javax.annotation.Nullable;

/** Core options for configuring the Worker Versioning feature. */
@Experimental
public final class CoreWorkerDeploymentOptions {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(CoreWorkerDeploymentOptions options) {
    return new Builder(options);
  }

  public static final class Builder {
    private boolean useVersioning;
    private WorkerDeploymentVersion version;
    private VersioningBehavior defaultVersioningBehavior = VersioningBehavior.UNSPECIFIED;

    private Builder() {}

    private Builder(CoreWorkerDeploymentOptions options) {
      this.useVersioning = options.useVersioning;
      this.version = options.version;
      this.defaultVersioningBehavior = options.defaultVersioningBehavior;
    }

    /**
     * If set, opts this worker into the Worker Deployment Versioning feature. It will only operate
     * on workflows it claims to be compatible with. You must also call {@link
     * Builder#setVersion(WorkerDeploymentVersion)} if this flag is true.
     */
    public Builder setUseVersioning(boolean useVersioning) {
      this.useVersioning = useVersioning;
      return this;
    }

    /** Assign a Deployment Version identifier to this worker. */
    public Builder setVersion(WorkerDeploymentVersion version) {
      this.version = version;
      return this;
    }

    /** Provides a default Versioning Behavior to workflows that do not set one explicitly. */
    public Builder setDefaultVersioningBehavior(VersioningBehavior defaultVersioningBehavior) {
      this.defaultVersioningBehavior = defaultVersioningBehavior;
      return this;
    }

    public CoreWorkerDeploymentOptions build() {
      Preconditions.checkState(
          !(useVersioning && version == null),
          "If useVersioning is set, setVersion must be called");
      return new CoreWorkerDeploymentOptions(useVersioning, version, defaultVersioningBehavior);
    }
  }

  private final boolean useVersioning;
  private final WorkerDeploymentVersion version;
  private final VersioningBehavior defaultVersioningBehavior;

  private CoreWorkerDeploymentOptions(
      boolean useVersioning,
      WorkerDeploymentVersion version,
      VersioningBehavior defaultVersioningBehavior) {
    this.useVersioning = useVersioning;
    this.version = version;
    this.defaultVersioningBehavior = defaultVersioningBehavior;
  }

  public boolean isUsingVersioning() {
    return useVersioning;
  }

  @Nullable
  public WorkerDeploymentVersion getVersion() {
    return version;
  }

  public VersioningBehavior getDefaultVersioningBehavior() {
    return defaultVersioningBehavior;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    CoreWorkerDeploymentOptions that = (CoreWorkerDeploymentOptions) o;
    return useVersioning == that.useVersioning
        && Objects.equals(version, that.version)
        && defaultVersioningBehavior == that.defaultVersioningBehavior;
  }

  @Override
  public int hashCode() {
    return Objects.hash(useVersioning, version, defaultVersioningBehavior);
  }

  @Override
  public String toString() {
    return "CoreWorkerDeploymentOptions{"
        + "useVersioning="
        + useVersioning
        + ", version='"
        + version
        + '\''
        + ", defaultVersioningBehavior="
        + defaultVersioningBehavior
        + '}';
  }
}
