# Kotlin SDK vs Java SDK API Naming Audit

This document audits the Kotlin SDK for methods/functions that have counterparts in the Java SDK but have different names. Properties instead of setters/getters are acceptable Kotlin idioms and are noted but not flagged as issues.

## Summary

The Kotlin SDK is designed as an idiomatic Kotlin API rather than a thin wrapper around the Java SDK. Most naming differences are intentional design decisions to provide a better Kotlin experience. However, some inconsistencies exist that may warrant review.

---

## 1. Acceptable Kotlin Idioms (Properties vs Getters)

These are NOT issues - they follow Kotlin best practices:

### KWorkflow Object
| Kotlin Property | Java Equivalent |
|----------------|-----------------|
| `typedSearchAttributes` | `Workflow.getTypedSearchAttributes()` |
| `previousRunFailure` | `Workflow.getPreviousRunFailure()` |
| `isReplaying` | `Workflow.isReplaying()` |
| `metricsScope` | `Workflow.getMetricsScope()` |
| `currentUpdateInfo` | `Workflow.getCurrentUpdateInfo()` |
| `isEveryHandlerFinished` | `Workflow.isEveryHandlerFinished()` |
| `currentDetails` (read/write) | `Workflow.getCurrentDetails()` / `Workflow.setCurrentDetails()` |

### KWorkflowInfo Interface
| Kotlin Property | Java Method |
|----------------|-------------|
| `namespace` | `getNamespace()` |
| `workflowId` | `getWorkflowId()` |
| `workflowType` | `getWorkflowType()` |
| `runId` | `getRunId()` |
| `firstExecutionRunId` | `getFirstExecutionRunId()` |
| `continuedExecutionRunId` | `getContinuedExecutionRunId()` |
| `originalExecutionRunId` | `getOriginalExecutionRunId()` |
| `taskQueue` | `getTaskQueue()` |
| `retryOptions` | `getRetryOptions()` |
| `workflowRunTimeout` | `getWorkflowRunTimeout()` |
| `workflowExecutionTimeout` | `getWorkflowExecutionTimeout()` |
| `runStartedTimestamp` | `getRunStartedTimestampMillis()` |
| `parentWorkflowId` | `getParentWorkflowId()` |
| `parentRunId` | `getParentRunId()` |
| `rootWorkflowId` | `getRootWorkflowId()` |
| `rootRunId` | `getRootRunId()` |
| `attempt` | `getAttempt()` |
| `cronSchedule` | `getCronSchedule()` |
| `historyLength` | `getHistoryLength()` |
| `historySize` | `getHistorySize()` |
| `isContinueAsNewSuggested` | `isContinueAsNewSuggested()` |
| `currentBuildId` | `getCurrentBuildId()` |
| `priority` | `getPriority()` |

### KActivityInfo Interface
| Kotlin Property | Java Method |
|----------------|-------------|
| `namespace` | `getNamespace()` |
| `workflowId` | `getWorkflowId()` |
| `runId` | `getRunId()` |
| `activityType` | `getActivityType()` |
| `activityId` | `getActivityId()` |
| `taskQueue` | `getActivityTaskQueue()` |
| `attempt` | `getAttempt()` |
| `scheduledTime` | `getScheduledTimestamp()` |
| `startedTime` | `getCurrentAttemptScheduledTimestamp()` |
| `scheduleToCloseTimeout` | `getScheduleToCloseTimeout()` |
| `startToCloseTimeout` | `getStartToCloseTimeout()` |
| `heartbeatTimeout` | `getHeartbeatTimeout()` |
| `isLocal` | `isLocal()` |
| `taskToken` | `getTaskToken()` |

### KWorkerFactory/KTestWorkflowEnvironment
| Kotlin Property | Java Method |
|----------------|-------------|
| `isStarted` | `isStarted()` |
| `isShutdown` | `isShutdown()` |
| `isTerminated` | `isTerminated()` |

### WorkflowHandle
| Kotlin Property | Java Method |
|----------------|-------------|
| `workflowId` | `getWorkflowId()` |
| `runId` | `getRunId()` |
| `execution` | `getExecution()` |

---

## 2. Naming Differences (Potential Issues)

### 2.1 KWorkflow vs Workflow

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Method** | `getInfo()` | `getInfo()` | **Same** - OK |
| **Method** | `logger()` | `getLogger()` | Different prefix |
| **Method** | `currentTime(): Instant` | `currentTimeMillis(): Long` | Different return type and name |
| **Method** | `currentTimeMillis(): Long` | `currentTimeMillis(): Long` | **Same** - OK |
| **Method** | `randomUUID()` | `randomUUID()` | **Same** - OK |
| **Method** | `newRandom()` | `newRandom()` | **Same** - OK |
| **Method** | `getVersion()` | `getVersion()` | **Same** - OK |
| **Method** | `sideEffect()` | `sideEffect()` | **Same** - OK |
| **Method** | `mutableSideEffect()` | `mutableSideEffect()` | **Same** - OK |
| **Method** | `getMemo()` | `getMemo()` | **Same** - OK |
| **Method** | `upsertMemo()` | `upsertMemo()` | **Same** - OK |
| **Method** | `upsertTypedSearchAttributes()` | `upsertTypedSearchAttributes()` | **Same** - OK |
| **Method** | `getLastCompletionResult()` | `getLastCompletionResult()` | **Same** - OK |
| **Method** | `isCancelRequested()` | n/a | New Kotlin-only API |
| **Method** | `awaitCondition()` | `await()` | **Different name** |
| **Method** | `continueAsNew()` | `continueAsNew()` | **Same** - OK |
| **Method** | `registerSignalHandler()` | `registerListener()` (DynamicWorkflow) | **Different approach** |
| **Method** | `registerQueryHandler()` | `registerListener()` (DynamicWorkflow) | **Different approach** |
| **Method** | `registerUpdateHandler()` | `registerListener()` (DynamicWorkflow) | **Different approach** |

### 2.2 KActivity vs Activity

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Property** | `context` | `getExecutionContext()` | **Different name** |
| **Property** | `info` | N/A (via context.getInfo()) | Convenience property |
| **Method** | `logger()` | N/A | New Kotlin-only API |
| **Method** | `heartbeat()` | `heartbeat()` (via context) | **Same** - OK |
| **Method** | `heartbeatDetails()` | `getHeartbeatDetails()` | Different prefix |
| **Method** | `doNotCompleteOnReturn()` | `doNotCompleteOnReturn()` | **Same** - OK |
| **Property** | `taskToken` | `getTaskToken()` (via context) | Property vs getter - OK |
| **Property** | `executionContext` | `getExecutionContext()` | Property vs getter - OK |
| **Property** | `isCancellationRequested` | N/A | New Kotlin-only API |

### 2.3 KWorkflowClient vs WorkflowClient

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Method** | `startWorkflow()` | `newWorkflowStub()` + `WorkflowClient.start()` | **Different pattern** |
| **Method** | `executeWorkflow()` | Stub method invocation | **Different pattern** |
| **Method** | `getWorkflowHandle()` | `newUntypedWorkflowStub()` | **Different name** |
| **Method** | `getUntypedWorkflowHandle()` | `newUntypedWorkflowStub()` | **Different name** |
| **Method** | `signalWithStart()` | `signalWithStart()` (on stub) | **Same** - OK |
| **Method** | `withStartWorkflowOperation()` | `newWithStartWorkflowOperation()` | **Different prefix** |
| **Method** | `startUpdateWithStart()` | `startUpdateWithStart()` | **Same** - OK |
| **Method** | `executeUpdateWithStart()` | `executeUpdateWithStart()` | **Same** - OK |

### 2.4 KWorker vs Worker

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Method** | `registerWorkflowImplementationTypes()` | `registerWorkflowImplementationTypes()` | **Same** - OK |
| **Method** | `registerActivitiesImplementations()` | `registerActivitiesImplementations()` | **Same** - OK |
| **Method** | `registerNexusServiceImplementations()` | `registerNexusServiceImplementation()` | Plural vs singular |

### 2.5 KWorkerFactory vs WorkerFactory

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Method** | `newWorker()` | `newWorker()` | **Same** - OK |
| **Method** | `start()` | `start()` | **Same** - OK |
| **Method** | `shutdown()` | `shutdown()` | **Same** - OK |
| **Method** | `shutdownNow()` | `shutdownNow()` | **Same** - OK |
| **Method** | `awaitTermination()` | `awaitTermination()` | **Same** - OK |

### 2.6 KTestWorkflowEnvironment vs TestWorkflowEnvironment

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Factory** | `newInstance()` | `newInstance()` | **Same** - OK |
| **Method** | `newWorker()` | `newWorker()` | **Same** - OK |
| **Property** | `workflowClient` | `getWorkflowClient()` | Property - OK |
| **Property** | `currentTimeMillis` | `currentTimeMillis()` | Property vs method - OK |
| **Property** | `currentTime` | N/A | New Kotlin-only convenience |
| **Method** | `sleep()` | `sleep()` | **Same** - OK |
| **Method** | `registerDelayedCallback()` | `registerDelayedCallback()` | **Same** - OK |
| **Method** | `start()` | `start()` | **Same** - OK |
| **Method** | `shutdown()` | `shutdown()` | **Same** - OK |
| **Method** | `shutdownNow()` | `shutdownNow()` | **Same** - OK |
| **Method** | `awaitTermination()` | `awaitTermination()` | **Same** - OK |
| **Method** | `close()` | `close()` | **Same** - OK |
| **Method** | `registerSearchAttribute()` | `registerSearchAttribute()` | **Same** - OK |
| **Method** | `getDiagnostics()` | `getDiagnostics()` | **Same** - OK |
| **Method** | `createNexusEndpoint()` | `createNexusEndpoint()` | **Same** - OK |
| **Method** | `deleteNexusEndpoint()` | `deleteNexusEndpoint()` | **Same** - OK |

### 2.7 WorkflowHandle/KWorkflowHandle

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Method** | `signal()` | `signal()` | **Same** - OK |
| **Method** | `query()` | `query()` | **Same** - OK |
| **Method** | `executeUpdate()` | `update()` | **Different name** |
| **Method** | `cancel()` | `cancel()` | **Same** - OK |
| **Method** | `terminate()` | `terminate()` | **Same** - OK |
| **Method** | `describe()` | `describe()` | **Same** - OK |
| **Method** | `getResult()` | `getResult()` | **Same** - OK |
| **Method** | `result()` (KTypedWorkflowHandle) | `getResult()` | Shorter name |

### 2.8 Schedule APIs (KSchedule* vs Schedule*)

| Category | Kotlin Name | Java Name | Notes |
|----------|-------------|-----------|-------|
| **Client Method** | `KClient.createSchedule()` | `ScheduleClient.createSchedule()` | **Same** - OK |
| **Client Method** | `KClient.scheduleHandle()` | `ScheduleClient.getHandle()` | **Different name** |
| **Client Method** | `KClient.listSchedules()` | `ScheduleClient.listSchedules()` | **Same** - OK (returns Flow) |
| **Handle Method** | `KScheduleHandle.describe()` | `ScheduleHandle.describe()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.update()` | `ScheduleHandle.update()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.pause()` | `ScheduleHandle.pause()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.unpause()` | `ScheduleHandle.unpause()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.trigger()` | `ScheduleHandle.trigger()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.backfill()` | `ScheduleHandle.backfill()` | **Same** - OK |
| **Handle Method** | `KScheduleHandle.delete()` | `ScheduleHandle.delete()` | **Same** - OK |
| **Data Class** | `KSchedule` | `Schedule` | **Same** - OK |
| **Data Class** | `KScheduleSpec` | `ScheduleSpec` | **Same** - OK |
| **Data Class** | `KScheduleState` | `ScheduleState` | **Same** - OK |
| **Data Class** | `KSchedulePolicy` | `SchedulePolicy` | **Same** - OK |
| **Data Class** | `KScheduleOptions` | `ScheduleOptions` | **Same** - OK |
| **Data Class** | `KScheduleActionStartWorkflow` | `ScheduleActionStartWorkflow` | **Same** - OK |
| **Data Class** | `KScheduleCalendarSpec` | `ScheduleCalendarSpec` | **Same** - OK |
| **Data Class** | `KScheduleIntervalSpec` | `ScheduleIntervalSpec` | **Same** - OK |
| **Data Class** | `KScheduleRange` | `ScheduleRange` | **Same** - OK |
| **Data Class** | `KScheduleBackfill` | `ScheduleBackfill` | **Same** - OK |
| **Data Class** | `KScheduleDescription` | `ScheduleDescription` | **Same** - OK |
| **Data Class** | `KScheduleInfo` | `ScheduleInfo` | **Same** - OK |
| **Data Class** | `KScheduleUpdate` | `ScheduleUpdate` | **Same** - OK |
| **Property** | `KScheduleBackfill.overlapPolicy` | `ScheduleBackfill.getOverlapPolicy()` | Property - OK |
| **Property** | `KSchedulePolicy.overlap` | `SchedulePolicy.getOverlap()` | Property - OK |

**Note**: Schedule methods on `KClient` are integrated directly rather than requiring a separate `ScheduleClient` instance. The Kotlin SDK uses `suspend` functions and Kotlin `Flow` for async operations.

---

## 3. Type Differences (Intentional Kotlin Idioms)

These are intentional differences using Kotlin-native types:

| Kotlin Type | Java Type | Notes |
|-------------|-----------|-------|
| `kotlin.time.Duration` | `java.time.Duration` | Kotlin duration in KActivityOptions, KLocalActivityOptions, etc. |
| `Instant` | `long` (milliseconds) | `runStartedTimestamp` uses `Instant` instead of `long` |
| `T?` (nullable) | `Optional<T>` | Kotlin nullable types replace Java Optional |
| Reified generics | Class parameter | `heartbeatDetails<T>()` vs `getHeartbeatDetails(Class<T>)` |

---

## 4. API Style Differences (Design Decisions)

### 4.1 Object vs Static Methods

Java uses static methods on classes like `Workflow`, `Activity`. Kotlin uses object singletons:
- `Workflow.currentTimeMillis()` → `KWorkflow.currentTimeMillis()`
- `Activity.getExecutionContext()` → `KActivity.context`

### 4.2 Builder Pattern vs Data Classes

Java uses builder pattern for options. Kotlin uses data classes with default parameters or DSL builders:
- Java: `ActivityOptions.newBuilder().setStartToCloseTimeout(...).build()`
- Kotlin: `KActivityOptions(startToCloseTimeout = ...)`

### 4.3 Method References Instead of Stub Pattern

Java uses typed stubs for workflow/activity invocation. Kotlin uses method references:
- Java: `stub.greet("World")` (stub is a proxy implementing the interface)
- Kotlin: `executeWorkflow(MyWorkflow::greet, options, "World")`

---

## 5. APIs for Java Compatibility Still Present

**None found.** The Kotlin SDK is purely idiomatic and does not retain Java-style getters/setters for compatibility.

---

## 6. Recommended Actions

### 6.1 Required Changes (IMPLEMENTED)

| Old Name | New Name | Reason | Status |
|----------|----------|--------|--------|
| `KActivity.context` | `KActivity.executionContext` | Align with Java SDK's `getExecutionContext()` | ✅ Done |
| `withStartWorkflowOperation()` | `newWithStartWorkflowOperation()` | Match Java SDK naming | ✅ Done |
| `registerNexusServiceImplementations()` | `registerNexusServiceImplementation()` | Match Java SDK (singular) | ✅ Done |
| `KWorkflow.currentTime()` | `KWorkflow.now()` | Match Kotlin's `Clock.System.now()` | ✅ Done |

**Note**: `KActivity.javaExecutionContext` was added to provide access to the raw Java `ActivityExecutionContext` for advanced use cases.

### 6.5 New APIs Added (IMPLEMENTED)

| API | Description | Status |
|-----|-------------|--------|
| Schedule API | `KClient.createSchedule()`, `scheduleHandle()`, `listSchedules()` with full `KSchedule*` data classes | ✅ Done |
| External Workflow Handles | `KWorkflow.getExternalWorkflowHandle()` for cross-workflow signaling | ✅ Done |
| `KActivityContext.current()` | Companion object method for accessing current activity context | ✅ Done |
| `KClient.connect()` | Suspend function factory for creating connected clients | ✅ Done |
| `KClientOptions` | Data class for client configuration | ✅ Done |
| `KWorkerOptions` | Data class for worker configuration | ✅ Done |

### 6.2 Keep as Is (Confirmed)

| API | Reason |
|-----|--------|
| `awaitCondition()` | Kotlin has built-in `await()` on Deferred; must differentiate |
| `logger()` | Acceptable Kotlin idiom (verb form) |
| `executeUpdate()` | Matches Python SDK's `execute_update` |
| `getWorkflowHandle()` | Acceptable Kotlin naming |

### 6.3 Keep as Is (Intentional Design)

These differences are intentional Kotlin improvements:

1. **Method references for workflow/activity execution** - Better type safety
2. **Data classes for options** - Cleaner than builder pattern
3. **Properties instead of getters** - Standard Kotlin idiom
4. **Nullable types instead of Optional** - Standard Kotlin idiom
5. **`kotlin.time.Duration`** - Native Kotlin duration support

### 6.4 Document Differences

Users migrating from Java SDK should be aware of:

1. Different execution patterns (method references vs stubs)
2. Property access instead of getter methods
3. `suspend` functions for async operations
4. Nullable types instead of Optional

---

## 7. Notable Missing APIs in Kotlin SDK

The following Java SDK APIs don't have direct Kotlin equivalents (may be intentional):

| Java API | Notes |
|----------|-------|
| `Workflow.newTimer()` | Use `delay()` instead |
| `Workflow.newQueue()` | Use Kotlin channels/flows |
| `Workflow.newCompletablePromise()` | Use Kotlin Deferred |
| `WorkflowClient.newWorkflowStub()` (typed) | Use method references pattern |
| `Workflow.newExternalWorkflowStub()` | Not found in Kotlin SDK |
| `Workflow.newActivityStub()` (stub pattern) | Use `executeActivity()` with method refs |
| `Workflow.newLocalActivityStub()` (stub pattern) | Use `executeLocalActivity()` with method refs |
| `Workflow.newChildWorkflowStub()` (stub pattern) | Use `executeChildWorkflow()` with method refs |

---

## 8. Conclusion

The Kotlin SDK provides a well-designed, idiomatic Kotlin API for Temporal. The naming differences from the Java SDK are largely intentional design decisions that make the API more natural for Kotlin developers. The main areas for potential improvement are:

1. **Minor naming alignment** (e.g., `await()` alias for `awaitCondition()`)
2. **Documentation** of migration path from Java SDK
3. **Completion of missing APIs** if needed for specific use cases

The SDK successfully avoids maintaining Java-style compatibility APIs, which keeps the codebase clean and maintainable.
