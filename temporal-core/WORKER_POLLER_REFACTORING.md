# Worker and Poller Refactoring to temporal-core

This document describes the plan to move worker and poller infrastructure from `temporal-sdk` to `temporal-core`, enabling code sharing between Java and Kotlin SDKs.

## Goals

1. Share polling infrastructure between Java and Kotlin SDKs
2. Reduce code duplication
3. Maintain SDK-specific flexibility for task handling and user-facing APIs

## Current State

### temporal-core contains:
- State machine infrastructure (`WorkflowStateMachines`, individual state machines)
- Replay abstractions (`ReplayWorkflow`, `ReplayWorkflowContext`)
- Core utilities (`CorePayloadConverter`, marker utilities, SDK flags)
- Factory interfaces (`WorkflowImplementationFactory`, `LocalActivityResult`)
- Generic client (`GenericWorkflowClient`)

### temporal-sdk contains (worker/poller related):
- Pollers: `BasePoller`, `MultiThreadedPoller`, `AsyncPoller`
- Workers: `ActivityWorker`, `WorkflowWorker`, `NexusWorker`, `LocalActivityWorker`
- Poll tasks: `WorkflowPollTask`, `ActivityPollTask`, `NexusPollTask` (sync and async variants)
- Task execution: `PollTaskExecutor`
- Concurrency: `TrackingSlotSupplier`, `AdjustableSemaphore`, `StickyQueueBalancer`
- Lifecycle: `SuspendableWorker`, `Shutdownable`, `ShutdownManager`
- Configuration: `SingleWorkerOptions`, `PollerOptions`, `WorkerVersioningOptions`

## Proposed Core Options Classes

### CorePollerOptions

All fields from current `PollerOptions` - no SDK dependencies:

```java
public final class CorePollerOptions {
    // Rate limiting
    private final int maximumPollRateIntervalMilliseconds;
    private final double maximumPollRatePerSecond;

    // Backoff configuration
    private final double backoffCoefficient;
    private final Duration backoffInitialInterval;
    private final Duration backoffCongestionInitialInterval;
    private final Duration backoffMaximumInterval;
    private final double backoffMaximumJitterCoefficient;

    // Threading
    private final String pollThreadNamePrefix;
    private final boolean usingVirtualThreads;
    private final ExecutorService pollerTaskExecutorOverride;

    // Behavior
    private final PollerBehavior pollerBehavior;
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
}
```

### CoreWorkerDeploymentOptions

```java
public final class CoreWorkerDeploymentOptions {
    private final boolean useVersioning;
    private final WorkerDeploymentVersion version;
    private final VersioningBehavior defaultVersioningBehavior;
}
```

### CoreWorkerVersioningOptions

```java
public final class CoreWorkerVersioningOptions {
    private final String buildId;
    private final boolean useBuildIdForVersioning;
    private final CoreWorkerDeploymentOptions workerDeploymentOptions;
}
```

### CoreSingleWorkerOptions

Infrastructure parts only (no SDK-specific dependencies):

```java
public final class CoreSingleWorkerOptions {
    // Identity
    private final String identity;

    // Polling
    private final CorePollerOptions pollerOptions;

    // Metrics
    private final Scope metricsScope;

    // Timeouts
    private final Duration stickyQueueScheduleToStartTimeout;
    private final Duration drainStickyTaskQueueTimeout;
    private final Duration maxHeartbeatThrottleInterval;
    private final Duration defaultHeartbeatThrottleInterval;
    private final long defaultDeadlockDetectionTimeout;

    // Threading
    private final boolean usingVirtualThreads;

    // Versioning
    private final CoreWorkerVersioningOptions versioningOptions;
}
```

**NOT in core** (SDK-specific, stays in temporal-sdk):
- `DataConverter dataConverter`
- `List<ContextPropagator> contextPropagators`
- `WorkerInterceptor[] workerInterceptors`
- `boolean enableLoggingInReplay`

## SDK Options Structure

### Java SDK

SDK options classes wrap/delegate to core:

```java
public final class SingleWorkerOptions {
    // Delegate to core for shared infrastructure
    private final CoreSingleWorkerOptions coreOptions;

    // SDK-specific fields
    private final DataConverter dataConverter;
    private final List<ContextPropagator> contextPropagators;
    private final WorkerInterceptor[] workerInterceptors;
    private final boolean enableLoggingInReplay;

    // Convenience accessor
    public CoreSingleWorkerOptions getCoreOptions() {
        return coreOptions;
    }
}
```

### Kotlin SDK

Kotlin options follow the same structure in idiomatic Kotlin:

```kotlin
data class KSingleWorkerOptions(
    // Core options (can embed fields or hold reference)
    val coreOptions: CoreSingleWorkerOptions,

    // SDK-specific fields
    val dataConverter: DataConverter,
    val contextPropagators: List<ContextPropagator>,
    val workerInterceptors: List<WorkerInterceptor>,
    val enableLoggingInReplay: Boolean
)
```

Kotlin SDK should also add support for `WorkerDeploymentOptions` to match Java SDK capabilities.

## What Moves to temporal-core

### Polling Infrastructure (CAN MOVE)

| Component | Notes |
|-----------|-------|
| `BasePoller` | Abstract lifecycle management |
| `MultiThreadedPoller` | Sync polling implementation |
| `AsyncPoller` | Async polling implementation |
| `WorkflowPollTask` | Uses `CoreWorkerVersioningOptions` + gRPC stubs |
| `ActivityPollTask` | Uses `CoreWorkerVersioningOptions` + gRPC stubs |
| `NexusPollTask` | Uses `CoreWorkerVersioningOptions` + gRPC stubs |
| `AsyncWorkflowPollTask` | Async variant |
| `AsyncActivityPollTask` | Async variant |
| `AsyncNexusPollTask` | Async variant |
| `PollTaskExecutor` | Uses `CorePollerOptions` |

### Concurrency Control (CAN MOVE)

| Component | Notes |
|-----------|-------|
| `StickyQueueBalancer` | No SDK dependencies |
| `TrackingSlotSupplier` | No SDK dependencies |
| `SlotReservationData` | Simple data class |
| `AdjustableSemaphore` | No SDK dependencies |
| `Throttler` | No SDK dependencies |
| `WorkflowRunLockManager` | No SDK dependencies |
| `WorkflowExecutorCache` | No SDK dependencies |
| `PollScaleReportHandle` | No SDK dependencies |
| `ScalingTask` | No SDK dependencies |

### Lifecycle Management (CAN MOVE)

| Component | Notes |
|-----------|-------|
| `Shutdownable` | Interface |
| `Suspendable` | Interface |
| `SuspendableWorker` | Interface |
| `Startable` | Interface |
| `WorkerLifecycleState` | Enum |
| `ShutdownManager` | No SDK dependencies |

### Task Data Types (CAN MOVE)

| Component | Notes |
|-----------|-------|
| `WorkflowTask` | Wrapper around poll response |
| `ActivityTask` | Wrapper around poll response |
| `NexusTask` | Wrapper around poll response |

### Utilities (CAN MOVE)

| Component | Notes |
|-----------|-------|
| `BlockCallerPolicy` | Thread pool policy |
| `ExecutorThreadFactory` | Thread factory |
| `WorkerThreadsNameHelper` | Naming utility |
| `CircularLongBuffer` | Data structure |
| `WorkerVersioningProtoUtils` | Proto conversion |

## What Stays in temporal-sdk

### Task Handlers (CANNOT MOVE - SDK-specific)

| Component | Reason |
|-----------|--------|
| `WorkflowTaskHandler` | Interface implemented by SDK |
| `ActivityTaskHandler` | Interface implemented by SDK |
| `NexusTaskHandler` | Interface implemented by SDK |

### High-Level Workers (CANNOT MOVE - use handlers)

| Component | Reason |
|-----------|--------|
| `WorkflowWorker` | Uses `WorkflowTaskHandler` |
| `ActivityWorker` | Uses `ActivityTaskHandler` |
| `NexusWorker` | Uses `NexusTaskHandler` |
| `LocalActivityWorker` | Uses `ActivityTaskHandler` |

### SDK Registration API (CANNOT MOVE - user-facing)

| Component | Reason |
|-----------|--------|
| `SyncWorkflowWorker` | SDK-specific registration |
| `SyncActivityWorker` | SDK-specific registration |
| `SyncNexusWorker` | SDK-specific registration |
| `Worker` | User-facing API |
| `WorkerFactory` | User-facing API |
| `WorkerOptions` | User-facing API |

### Other SDK-Specific (CANNOT MOVE)

| Component | Reason |
|-----------|--------|
| `EagerActivityDispatcher` | Depends on SDK activity registration |
| `LocalActivityDispatcher` | Depends on SDK activity execution |
| `QueryReplayHelper` | Depends on SDK query handling |
| `CompositeReplayWorkflowFactory` | Depends on SDK workflow registration |

## Migration Strategy

All phases are complete.

1. **Phase 1: Create Core Options** _(COMPLETE)_
   - Created `CorePollerOptions`, `CoreWorkerVersioningOptions`, `CoreWorkerDeploymentOptions`, `CoreSingleWorkerOptions` in temporal-core
   - Updated Java SDK options to wrap/use core options with `getCoreOptions()`
   - Updated Kotlin SDK `KWorkerOptions` with deployment and poller behavior properties
   - Moved `PollerBehavior` types, `VersioningBehavior`, `WorkerDeploymentVersion` to temporal-core

2. **Phase 2: Move Lifecycle Interfaces** _(COMPLETE)_
   - Moved `Shutdownable`, `Suspendable`, `SuspendableWorker`, `Startable`, `WorkerLifecycleState`, `WorkerWithLifecycle` to temporal-core
   - Moved `ShutdownManager` to temporal-core (refactored `waitForSupplierPermitsReleasedUnlimited` to use `IntSupplier`)
   - Moved `ExecutorThreadFactory`, `WorkerThreadsNameHelper`, `DisableNormalPolling`, `GrpcUtils`

3. **Phase 3: Move Concurrency Infrastructure** _(COMPLETE)_
   - Moved slot suppliers (`TrackingSlotSupplier`, `SlotInfo` hierarchy, `FixedSizeSlotSupplier`), semaphores (`AdjustableSemaphore`), throttlers (`Throttler`, `CircularLongBuffer`)
   - Moved `StickyQueueBalancer`, `WorkflowRunLockManager`, `MetricsType`, `ScalingTask`, `SlotReservationData`
   - Moved `WorkflowExecutorCache` (genericized to `<T extends Closeable>`)
   - Moved `PollScaleReportHandle` (refactored `Functions.Proc1` to `Consumer`)

4. **Phase 4: Move Pollers** _(COMPLETE)_
   - Moved `BasePoller`, `MultiThreadedPoller`, `AsyncPoller`, `PollTaskExecutor`
   - Moved prerequisites: `TaskExecutor`, `ShutdownableTaskExecutor`, `BlockCallerPolicy`, `LoggerTag`, `ThreadConfigurator`, `VirtualThreadDelegate`
   - Changed pollers from `PollerOptions` to `CorePollerOptions`
   - Added multi-release JAR support to temporal-core for Java 21 virtual threads

5. **Phase 5: Move Poll Tasks** _(COMPLETE)_
   - Moved all poll task implementations (sync and async variants for workflow, activity, nexus)
   - Moved task data types (`WorkflowTask`, `ActivityTask`, `NexusTask`) with `Functions.Proc`/`Proc1` replaced by `Runnable`/`Consumer`
   - Moved `PollerTypeMetricsTag`, `WorkflowSlotInfo`, `NexusSlotInfo`, `WorkerVersioningProtoUtils`
   - Changed poll tasks from `WorkerVersioningOptions` to `CoreWorkerVersioningOptions`
   - Added `toCoreOptions()` to `WorkerDeploymentOptions` for SDK-to-core conversion

## Benefits

1. **Code Reuse**: Both Java and Kotlin SDKs share polling infrastructure
2. **Consistency**: Same polling behavior across SDKs
3. **Maintainability**: Bug fixes and improvements apply to both SDKs
4. **Clear Boundaries**: SDK-specific code (handlers, registration) stays in SDK modules
