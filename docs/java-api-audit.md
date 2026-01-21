# Java API Audit

This document tracks direct Java SDK API usages in the Kotlin SDK and samples, identifying areas where Kotlin-native alternatives are needed.

## Summary

The Kotlin SDK proposal aims to eliminate direct Java SDK usage for common operations. This audit identifies remaining gaps.

## Developer-Facing Java APIs (Excluding Annotations)

### 1. Service Connection

| Java API | Current Usage | Proposed Alternative |
|----------|---------------|---------------------|
| `WorkflowServiceStubs.newLocalServiceStubs()` | All samples | `KClient.connect()` |
| `WorkflowServiceStubs.newServiceStubs(options)` | Production apps | `KClient.connect(KClientOptions(...))` |

**Status:** ✅ **Implemented**. `KClient.connect()` supports:
- `connect()` - loads from environment variables (TEMPORAL_ADDRESS, etc.) with localhost:7233 default
- `connect(options: KClientOptions)` - explicit configuration
- `connect(profile: ClientConfigProfile)` - uses Java envconfig profile

### 2. Worker Factory

| Java API | Current Usage | Proposed Alternative |
|----------|---------------|---------------------|
| `KWorkerFactory(client)` | Legacy samples | `KWorker(client, KWorkerOptions(...))` |
| `factory.newWorker(taskQueue)` | Legacy samples | Constructor takes options |
| `factory.start()` | Legacy samples | `worker.run()` or `worker.start()` |

**Status:** ✅ **Implemented**. `KWorker(client, KWorkerOptions)` supports:
- `workflows` - list of workflow implementation classes
- `activities` - list of activity implementation instances
- `worker.run()` - blocks until shutdown
- `worker.start()` - non-blocking start

### 3. Exception Types

| Java API | Usage | Proposed Alternative |
|----------|-------|---------------------|
| `ApplicationFailure.newFailure(...)` | Throwing application errors | Consider `KApplicationFailure` or keep as-is |
| `WorkflowUpdateException` | Catching update failures | Consider `KWorkflowUpdateException` or keep as-is |

**Status:** No Kotlin wrappers exist. These may be acceptable as-is since they're exception types.

### 4. Java Time Types

| Java API | Where Exposed | Proposed Alternative |
|----------|---------------|---------------------|
| `java.time.Instant` | `KWorkflowInfo`, `KActivityInfo` | `kotlinx.datetime.Instant` |
| `java.time.Duration` | Some API signatures | `kotlin.time.Duration` (partially done) |

**Status:** Most Duration APIs accept `kotlin.time.Duration`. Instant conversion is not implemented.

## Required Java Annotations

These annotations are required and have no Kotlin alternatives:

- `@WorkflowInterface`, `@WorkflowMethod`
- `@QueryMethod`, `@SignalMethod`, `@UpdateMethod`, `@UpdateValidatorMethod`
- `@ActivityInterface`, `@ActivityMethod`

**Status:** Expected - annotations are defined in Java SDK.

## Current vs Proposed API

### Current (Samples)

```kotlin
// No Java APIs needed!
val client = KClient.connect()  // Uses env vars or localhost:7233 default

// Simplified worker setup (Python/.NET pattern)
val worker = KWorker(
    client,
    KWorkerOptions(
        taskQueue = TASK_QUEUE,
        workflows = listOf(GreetingWorkflowImpl::class),
        activities = listOf(GreetingActivitiesImpl())
    )
)
worker.run()  // Blocks until shutdown
```

### Proposed (from kotlin-idioms proposal)

```kotlin
// No Java APIs needed
val client = KClient.connect(KClientOptions(target = "localhost:7233"))

val worker = KWorker(
    client,
    KWorkerOptions(
        taskQueue = "task-queue",
        workflows = listOf(GreetingWorkflowImpl::class),
        activities = listOf(GreetingActivitiesImpl())
    )
)

worker.run()
```

## Implementation Gaps

### High Priority (Blocks Java-free experience)

1. ~~**`KClient.connect()`**~~ ✅ **IMPLEMENTED** - Factory method that creates `WorkflowServiceStubs` internally
   - Accepts `KClientOptions` with `target`, `namespace`, etc.
   - Zero-arg version loads from environment variables with localhost:7233 default
   - Hides `WorkflowServiceStubs` from users

2. ~~**`KWorker` direct constructor**~~ ✅ **IMPLEMENTED** - Takes client and options directly
   - `KWorkerOptions` includes `workflows` and `activities` lists
   - Eliminates need for `KWorkerFactory` for simple cases

3. ~~**`worker.run()`**~~ ✅ **IMPLEMENTED** - Blocking run method
   - Blocks until shutdown signal or fatal error
   - Also provides `start()` for non-blocking startup

### Medium Priority (Improves developer experience)

4. **`kotlinx.datetime.Instant`** - Replace `java.time.Instant` in info classes
   - `KWorkflowInfo.workflowRunStartTime` etc.
   - `KActivityInfo` timestamps

### Low Priority (Edge cases)

5. **Exception wrappers** - `KApplicationFailure`, `KWorkflowUpdateException`
   - May not be necessary - exception types are typically caught, not constructed frequently

## Internal Java API Usage (Acceptable)

The following Java API usages in `KWorkflow.kt` and `KActivity.kt` are **internal implementation details** that delegate to Java SDK. These are acceptable:

- `Workflow.getInfo()` → wrapped in `KWorkflowInfo`
- `Workflow.getLogger()` → returns Java logger (acceptable)
- `Workflow.getVersion()` → versioning API
- `Workflow.sideEffect()` → side effect recording
- `Workflow.await()` → fallback for non-suspend workflows
- `Activity.getExecutionContext()` → wrapped in `KActivityContext`

## Recommendations

1. ~~Implement `KClient.connect()` to eliminate `WorkflowServiceStubs` from user code~~ ✅ **DONE**
2. ~~Implement `KWorker(client, options)` constructor to eliminate `KWorkerFactory` for simple cases~~ ✅ **DONE**
3. Keep `KWorkerFactory` available for advanced use cases (multiple workers, dynamic registration)
4. Consider keeping exception types as-is (low-frequency usage)
5. Consider `kotlinx.datetime` for timestamp types in future version
