# Public Kotlin SDK API - Java Import Audit Report

**Date:** 2026-01-23
**Scope:** `io.temporal.kotlin.*` (excluding `internal/`)

## Summary

The public Kotlin SDK contains **67 files**, of which **60 files** import Java classes from the Temporal Java SDK or Java standard library.

---

## 1. Acceptable Imports (Annotations & Exceptions)

These are generally acceptable as they are used for annotations or error handling:

### Annotations (7 imports)

| Import | Purpose |
|--------|---------|
| `io.temporal.activity.ActivityInterface` | Activity interface marker |
| `io.temporal.activity.ActivityMethod` | Activity method marker |
| `io.temporal.workflow.WorkflowInterface` | Workflow interface marker |
| `io.temporal.workflow.WorkflowMethod` | Workflow method marker |
| `io.temporal.workflow.QueryMethod` | Query method marker |
| `io.temporal.workflow.SignalMethod` | Signal method marker |
| `io.temporal.workflow.UpdateMethod` | Update method marker |

### Exceptions/Failures (3 imports)

| Import | Purpose |
|--------|---------|
| `io.temporal.failure.ApplicationFailure` | Application-level failures |
| `io.temporal.failure.CanceledFailure` | Cancellation failures |
| `io.temporal.api.failure.v1.Failure` | Protobuf failure type |

---

## 2. Problematic Imports - Java SDK Classes

These are Java SDK classes imported into the public Kotlin API:

### io.temporal.client.* (35 imports)

| Import | Concern Level |
|--------|--------------|
| `WorkflowClient` | HIGH - Core client class |
| `WorkflowClientOptions` | HIGH - Options class |
| `WorkflowOptions` | HIGH - Options class |
| `WorkflowStub` | HIGH - Core abstraction |
| `WorkflowUpdateHandle` | MEDIUM |
| `WorkflowUpdateStage` | MEDIUM - Enum |
| `WorkflowExecutionDescription` | MEDIUM |
| `UpdateOptions` | HIGH - Options class |
| `ActivityCompletionClient` | HIGH - Core client class |
| `OnConflictOptions` | HIGH - Options class |
| `ScheduleClient` | HIGH - Core client class |
| `ScheduleClientOptions` | HIGH - Options class |
| `Schedule`, `ScheduleAction`, `ScheduleBackfill`, etc. (22 schedule classes) | HIGH - Complete schedule API |

### io.temporal.worker.* (6 imports)

| Import | Concern Level |
|--------|--------------|
| `Worker` | HIGH - Core class |
| `WorkerFactory` | HIGH - Core class |
| `WorkerFactoryOptions` | HIGH - Options class |
| `WorkerOptions` | HIGH - Options class |
| `WorkflowImplementationOptions` | HIGH - Options class |
| `WorkerTuner` | MEDIUM |

### io.temporal.activity.* (9 imports, excluding annotations)

| Import | Concern Level |
|--------|--------------|
| `Activity` | HIGH - Static utilities |
| `ActivityExecutionContext` | HIGH - Core context |
| `ActivityInfo` | HIGH - Core info class |
| `ActivityOptions` | HIGH - Options class |
| `LocalActivityOptions` | HIGH - Options class |
| `ActivityCancellationType` | MEDIUM - Enum |
| `DynamicActivity` | MEDIUM |
| `TypedDynamicActivity` | MEDIUM |
| `ManualActivityCompletionClient` | MEDIUM |

### io.temporal.workflow.* (9 imports, excluding annotations)

| Import | Concern Level |
|--------|--------------|
| `Workflow` | HIGH - Static utilities |
| `WorkflowInfo` | HIGH - Core info class |
| `ChildWorkflowOptions` | HIGH - Options class |
| `ContinueAsNewOptions` | HIGH - Options class |
| `ChildWorkflowCancellationType` | MEDIUM - Enum |
| `ExternalWorkflowStub` | MEDIUM |
| `Promise` | MEDIUM |
| `UpdateInfo` | MEDIUM |

### io.temporal.common.* (14 imports)

| Import | Concern Level |
|--------|--------------|
| `DataConverter` | HIGH - Core interface |
| `EncodedValues` | MEDIUM |
| `RetryOptions` | HIGH - Options class |
| `SearchAttributes` | MEDIUM |
| `SearchAttributeKey` | MEDIUM |
| `SearchAttributeUpdate` | MEDIUM |
| `Priority` | MEDIUM |
| `VersioningIntent` | MEDIUM |
| `VersioningOverride` | MEDIUM |
| `ContextPropagator` | MEDIUM |
| `WorkflowClientInterceptor` | MEDIUM |
| `Header` | MEDIUM |
| `POJOActivityInterfaceMetadata` | LOW - Metadata |
| `POJOWorkflowInterfaceMetadata` | LOW - Metadata |

### io.temporal.serviceclient.* (2 imports)

| Import | Concern Level |
|--------|--------------|
| `WorkflowServiceStubs` | HIGH - Core service client |
| `WorkflowServiceStubsOptions` | HIGH - Options class |

### io.temporal.api.* (Protobuf - 22 imports)

| Import | Concern Level |
|--------|--------------|
| `WorkflowExecution` | HIGH - Core protobuf |
| `Payloads` | HIGH - Core protobuf |
| `WorkflowExecutionStatus` | MEDIUM - Enum |
| `WorkflowIdReusePolicy` | MEDIUM - Enum |
| `WorkflowIdConflictPolicy` | MEDIUM - Enum |
| `ParentClosePolicy` | MEDIUM - Enum |
| `ScheduleOverlapPolicy` | MEDIUM - Enum |
| Various command/query/history attributes | LOW - Internal use |

---

## 3. Java Standard Library Imports

### Commonly Used (Generally Acceptable)

| Type | Files Using |
|------|-------------|
| `java.time.Duration` | 11 files |
| `java.time.Instant` | 8 files |

### Potentially Problematic

| Import | Files Using | Concern |
|--------|-------------|---------|
| `java.util.Random` | 2 files (KWorkflow.kt, KWorkflowOutboundCallsInterceptor.kt) | Should use Kotlin Random? |
| `java.util.UUID` | 2 files (KWorkflow.kt, KWorkflowOutboundCallsInterceptor.kt) | Acceptable |
| `java.util.Optional` | 2 files (KChildWorkflowHandle.kt, KActivityCompletionHandle.kt) | Should avoid - use Kotlin nullable |
| `java.lang.reflect.Type` | 3 files | Implementation detail |
| `java.lang.reflect.Method` | 2 files | Implementation detail |
| `java.util.concurrent.TimeUnit` | 3 files | Could use kotlin.time |
| `java.util.concurrent.ConcurrentHashMap` | 1 file | Implementation detail |
| `java.util.concurrent.atomic.*` | 1 file | Implementation detail |

---

## 4. Files with Most Java Imports

| File | Java SDK Imports | Java Stdlib Imports |
|------|-----------------|---------------------|
| `KWorkflow.kt` | 15 | 3 |
| `KClient.kt` | 14 | 0 |
| `KChildWorkflowHandle.kt` | 8 | 1 |
| `KWorkerOptions.kt` | 5 | 1 |
| `KClientOptions.kt` | 4 | 1 |
| `KWorkflowExecutionDescription.kt` | 5 | 3 |
| `KChildWorkflowOptions.kt` | 8 | 0 |
| `KScheduleActionStartWorkflow.kt` | 4 | 0 |
| `KWorker.kt` | 9 | 2 |

---

## 5. HIGH Priority Issues (Classes Exposed in Public API)

Based on the imports, these Java classes are likely exposed in the public Kotlin API signatures:

### Options Classes
Users may need to work with Java options:
- `WorkflowOptions`, `WorkerOptions`, `ActivityOptions`, etc.
- `RetryOptions`, `DataConverter`

### Core Client Classes
Directly exposed:
- `WorkflowServiceStubs`, `WorkflowServiceStubsOptions`
- `WorkflowClient`, `WorkflowClientOptions`
- `ScheduleClient`, `Schedule*` classes

### Enums from API
Directly used in Kotlin options:
- `WorkflowIdReusePolicy`, `WorkflowIdConflictPolicy`
- `ParentClosePolicy`, `ScheduleOverlapPolicy`
- `WorkflowExecutionStatus`

### Search Attributes
Directly exposed:
- `SearchAttributes`, `SearchAttributeKey`, `SearchAttributeUpdate`

### Context/Info Classes
May be exposed:
- `WorkflowInfo`, `ActivityInfo`
- `ActivityExecutionContext`

---

## 6. Recommendations

1. **Create Kotlin Wrappers** for commonly exposed Java types:
   - `KSearchAttributes`, `KSearchAttributeKey`
   - `KDataConverter`
   - Kotlin enums for policies (or re-export with typealias)

2. **Replace `java.util.Optional`** with Kotlin nullable types

3. **Consider `kotlin.time.Duration`** instead of `java.time.Duration` in public APIs

4. **Hide internal Java SDK usage** by ensuring Java types don't leak into public signatures

---

## Appendix: Complete Import Lists by Category

### All Java SDK Annotations
```
io.temporal.activity.ActivityInterface
io.temporal.activity.ActivityMethod
io.temporal.workflow.QueryMethod
io.temporal.workflow.SignalMethod
io.temporal.workflow.UpdateMethod
io.temporal.workflow.WorkflowInterface
io.temporal.workflow.WorkflowMethod
```

### All Java SDK Exceptions
```
io.temporal.api.failure.v1.Failure
io.temporal.failure.ApplicationFailure
io.temporal.failure.CanceledFailure
```

### All Java SDK Enums
```
io.temporal.api.enums.v1.ParentClosePolicy
io.temporal.api.enums.v1.ScheduleOverlapPolicy
io.temporal.api.enums.v1.WorkflowExecutionStatus
io.temporal.api.enums.v1.WorkflowIdConflictPolicy
io.temporal.api.enums.v1.WorkflowIdReusePolicy
io.temporal.activity.ActivityCancellationType
io.temporal.workflow.ChildWorkflowCancellationType
```

### All Java Standard Library Imports
```
java.lang.reflect.Method
java.lang.reflect.Type
java.time.Duration
java.time.Instant
java.util.Optional
java.util.Random
java.util.UUID
java.util.concurrent.ConcurrentHashMap
java.util.concurrent.TimeUnit
java.util.concurrent.atomic.AtomicBoolean
java.util.concurrent.atomic.AtomicReference
```
