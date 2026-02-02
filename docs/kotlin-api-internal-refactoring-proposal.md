# Kotlin SDK Internal Refactoring Proposal

## Executive Summary

This proposal outlines a comprehensive refactoring of the Kotlin SDK to:
1. Move all implementation classes to internal packages
2. Only expose pure Kotlin classes in the public API
3. Hide implementation dependencies on Java SDK from public API
4. Use Kotlin visibility modifiers (`internal`) to enforce encapsulation

## Current State Analysis

### Java SDK Structure (Target Pattern)

The Java SDK follows a clean separation:

```
io.temporal.
├── activity/          # PUBLIC - Activity annotations and types
├── client/            # PUBLIC - Client API
├── common/            # PUBLIC - Common types (RetryOptions, etc.)
├── failure/           # PUBLIC - Failure types
├── nexus/             # PUBLIC - Nexus API
├── payload/           # PUBLIC - Payload converters
├── worker/            # PUBLIC - Worker API
├── workflow/          # PUBLIC - Workflow API
└── internal/          # INTERNAL - ALL implementation
    ├── sync/          # Workflow execution implementation
    ├── replay/        # Replay logic
    ├── worker/        # Worker implementation
    ├── client/        # Client implementation
    ├── statemachines/ # State machines
    └── ...
```

**Key Principle**: Everything in `internal` is implementation detail. Public packages contain only interfaces, annotations, options classes, and factory methods.

### Kotlin SDK Current Structure

```
io.temporal/                          # Java SDK packages with Kotlin extensions
├── *Ext.kt files                     # OUT OF SCOPE - will be separate module
└── internal/async/                   # 1 file in Java internal package (!)
    └── KotlinMethodReferenceDisassemblyService.kt  # NEEDS TO MOVE

io.temporal.kotlin/                   # Native Kotlin SDK (FOCUS OF REFACTORING)
├── activity/                         # Mixed public/impl
│   ├── KActivityOptions.kt          # Options - PUBLIC
│   ├── KActivityContext.kt          # Interface - PUBLIC
│   ├── KActivityContextImpl.kt      # Implementation - should be INTERNAL
│   ├── KotlinActivityWrapper.kt     # Implementation - should be INTERNAL
│   └── ...
├── client/                          # Mixed public/impl
│   ├── KClient.kt                   # Exposes WorkflowClient publicly
│   ├── KWorkflowOptions.kt          # Options - PUBLIC
│   └── schedules/                   # Schedule API
├── worker/                          # Mixed public/impl
│   ├── KWorker.kt                   # Exposes Worker publicly
│   ├── KWorkerFactory.kt            # Factory - PUBLIC
│   └── KotlinPlugin.kt              # Implementation detail
├── workflow/                        # Mixed public/impl
│   ├── KWorkflow.kt                 # Main API - PUBLIC
│   └── ...
├── common/                          # PUBLIC types
├── interceptor/                     # Interceptor interfaces
├── internal/                        # Internal - but incomplete
│   ├── KOptionsConverters.kt        # Converter - INTERNAL
│   ├── KotlinWorkflowContext.kt     # Implementation - but @PublishedApi
│   ├── KotlinReplayWorkflow.kt      # Implementation
│   └── interceptor/                 # Internal interceptors
└── samples/                         # Should be removed/separate
```

### Problems Identified

1. **Java SDK Types in Public API**
   - `KClient.workflowClient: WorkflowClient` - exposes Java type
   - `KClient.workflowService: WorkflowServiceStubs` - exposes Java type
   - `KWorker.worker: Worker` - exposes Java type
   - Various options classes accept/return Java SDK types

2. **Implementation in Public Packages**
   - `KActivityContextImpl` is in `io.temporal.kotlin.activity` (public)
   - `KotlinActivityWrapper` is in `io.temporal.kotlin.activity` (public)
   - `KotlinPlugin` is in `io.temporal.kotlin.worker` (public)

3. **Leaked Internal APIs**
   - `KotlinWorkflowContext` is `@PublishedApi internal` - effectively public
   - `WorkflowContextElement` in internal but referenced from public API

4. **Misplaced File**
   - `KotlinMethodReferenceDisassemblyService.kt` is in `io.temporal.internal.async`
   - This is actually inside Java SDK's internal package!

---

## Proposed Architecture

### New Package Structure

```
io.temporal.kotlin/
│
├── ─────────────────────── PUBLIC API (NO Java SDK imports) ───────────────────────
│
├── client/                           # Client API - Pure Kotlin facades
│   ├── KClient.kt                   # Facade delegating to KClientInternal
│   ├── KClientOptions.kt            # Pure Kotlin options
│   ├── KWorkflowOptions.kt          # Pure Kotlin options
│   ├── KWorkflowHandle.kt           # Workflow handle (facade)
│   ├── KTypedWorkflowHandle.kt      # Typed workflow handle (facade)
│   ├── KUpdateHandle.kt             # Update handle
│   ├── KUpdateWithStartOptions.kt   # Update options
│   ├── KOnConflictOptions.kt        # Conflict options
│   ├── KWorkflowExecutionDescription.kt
│   ├── KActivityCompletionClient.kt # Activity completion (facade)
│   └── schedules/                   # Schedule API
│       ├── KSchedule.kt
│       ├── KScheduleHandle.kt       # Facade
│       ├── KScheduleOptions.kt
│       └── ... (all schedule types - pure Kotlin)
│
├── worker/                           # Worker API - Pure Kotlin facades
│   ├── KWorker.kt                   # Facade delegating to KWorkerInternal
│   ├── KWorkerFactory.kt            # Facade delegating to KWorkerFactoryInternal
│   ├── KWorkerFactoryOptions.kt     # Pure Kotlin options
│   └── KWorkerOptions.kt            # Pure Kotlin options
│
├── workflow/                         # Workflow API
│   ├── KWorkflow.kt                 # Main workflow API
│   ├── KWorkflowInfo.kt             # Workflow info
│   ├── KChildWorkflowHandle.kt      # Child workflow handle
│   ├── KChildWorkflowOptions.kt     # Child workflow options
│   ├── KContinueAsNewOptions.kt     # Continue-as-new options
│   ├── KExternalWorkflowHandle.kt   # External workflow handle
│   ├── KDynamicWorkflow.kt          # Dynamic workflow interface
│   └── KDynamicHandlers.kt          # Dynamic handler interfaces
│
├── activity/                         # Activity API
│   ├── KActivityContext.kt          # Context interface (pure Kotlin)
│   ├── KActivityInfo.kt             # Activity info
│   ├── KActivityOptions.kt          # Options
│   ├── KLocalActivityOptions.kt     # Local activity options
│   ├── KDynamicActivity.kt          # Dynamic activity interface
│   └── KDynamicActivityHandler.kt   # Dynamic handler interface
│
├── common/                           # Common types
│   ├── KRetryOptions.kt             # Retry options
│   ├── KArgs.kt                     # Type-safe argument wrappers
│   └── KEncodedValues.kt            # Encoded values wrapper
│
├── interceptor/                      # Interceptor interfaces (PUBLIC)
│   ├── KWorkerInterceptor.kt        # Worker interceptor interface
│   ├── KWorkflowInboundCallsInterceptor.kt
│   ├── KWorkflowOutboundCallsInterceptor.kt
│   └── KActivityInboundCallsInterceptor.kt
│
├── JavaInterop.kt                   # Explicit Java SDK interop extensions
├── DurationExt.kt                   # kotlin.time.Duration extensions
├── TemporalDsl.kt                   # DSL marker
│
├── ─────────────────────── INTERNAL (All Java SDK imports here) ───────────────────────
│
└── internal/                         # ALL IMPLEMENTATION
    │
    ├── InternalTemporalApi.kt       # @RequiresOptIn annotation
    │
    ├── client/                      # Client implementation (Java SDK imports)
    │   ├── KClientInternal.kt       # WorkflowClient wrapper
    │   ├── KWorkflowHandleInternal.kt
    │   ├── KActivityCompletionClientInternal.kt
    │   └── KScheduleHandleInternal.kt
    │
    ├── worker/                      # Worker implementation (Java SDK imports)
    │   ├── KWorkerInternal.kt       # Worker wrapper
    │   └── KWorkerFactoryInternal.kt # WorkerFactory wrapper
    │
    ├── workflow/                    # Workflow implementation
    │   ├── KotlinWorkflowContext.kt # Context implementation
    │   ├── KotlinReplayWorkflow.kt  # Replay workflow
    │   ├── KotlinDynamicReplayWorkflow.kt
    │   ├── KotlinWorkflowDefinition.kt
    │   ├── KotlinCoroutineDispatcher.kt
    │   ├── KotlinDelay.kt
    │   └── WorkflowContextElement.kt
    │
    ├── activity/                    # Activity implementation
    │   ├── KActivityContextImpl.kt  # Context implementation
    │   ├── KotlinActivityWrapper.kt # TypedDynamicActivity wrapper
    │   ├── KDynamicActivityWrapper.kt
    │   ├── KActivityRegistry.kt
    │   └── SuspendActivityContextWrapper.kt
    │
    ├── interceptor/                 # Interceptor implementations
    │   ├── RootWorkflowInboundCallsInterceptor.kt
    │   ├── RootWorkflowOutboundCallsInterceptor.kt
    │   ├── RootActivityInboundCallsInterceptor.kt
    │   └── InterceptorChain.kt
    │
    ├── converters/                  # Option converters (Java SDK imports)
    │   ├── KOptionsConverters.kt    # Options → Java SDK
    │   └── KScheduleConverters.kt   # Schedule → Java SDK
    │
    ├── plugin/                      # Worker plugin (Java SDK imports)
    │   ├── KotlinPlugin.kt          # WorkerPlugin implementation
    │   └── KotlinPluginOptions.kt
    │
    └── service/                     # Java SDK method reference support
        └── KotlinMethodReferenceDisassemblyService.kt
```

### Extension Files (Out of Scope)

The 25 extension files in `io.temporal.*` packages predate the native Kotlin SDK. These will be moved to a separate module in the future and are **out of scope** for this refactoring.

Files not touched by this refactoring:
- `io.temporal.activity/*Ext.kt`
- `io.temporal.client/*Ext.kt`
- `io.temporal.common/*Ext.kt`
- `io.temporal.serviceclient/*Ext.kt`
- `io.temporal.worker/*Ext.kt`
- `io.temporal.workflow/*Ext.kt`

---

## Visibility Strategy

### Kotlin Visibility Modifiers

| Modifier | Scope |
|----------|-------|
| `public` | Visible everywhere (default) |
| `internal` | Visible within the same module |
| `private` | Visible within the file/class |

### Visibility Rules for This Refactoring

1. **Public API Classes**: `public` visibility
   - `KClient`, `KWorker`, `KWorkflow`, etc.
   - All options classes (`KWorkflowOptions`, etc.)
   - All handle classes (`KWorkflowHandle`, etc.)
   - Interceptor interfaces

2. **Internal Implementation**: `internal` visibility
   - All classes in `io.temporal.kotlin.internal.*`
   - Implementation classes (`KotlinWorkflowContext`, etc.)
   - Converters, wrappers, plugins

3. **Inline Function Support**: `@PublishedApi internal`
   - Only for classes required by inline functions in public API
   - Minimize usage - prefer non-inline alternatives

### Handling `@PublishedApi internal`

Current problem: `KotlinWorkflowContext` is `@PublishedApi internal` because inline functions in `KWorkflow` need it.

**Solution**: Refactor to minimize `@PublishedApi` exposure:

```kotlin
// BEFORE - KWorkflow.kt
@PublishedApi
internal val context: KotlinWorkflowContext
  get() = WorkflowContextElement.current.context

inline fun <reified R> executeActivity(...): R {
  // Uses context directly
}

// AFTER - KWorkflow.kt
// No @PublishedApi needed
private val context: KotlinWorkflowContext
  get() = WorkflowContextElement.current.context

// Non-inline overload that delegates
suspend fun <R> executeActivity(
  activity: KFunction<*>,
  resultClass: Class<R>,
  options: KActivityOptions,
  vararg args: Any?
): R {
  return context.executeActivity(...)
}

// Inline version calls non-inline version
inline fun <reified R> executeActivity(
  activity: KFunction<*>,
  options: KActivityOptions,
  vararg args: Any?
): R = executeActivity(activity, R::class.java, options, *args)
```

---

## Hiding Java SDK Types

### Current Leakage Points

1. **KClient**
```kotlin
// CURRENT - Java types exposed
public class KClient(
  public val workflowClient: WorkflowClient  // Java type!
) {
  public val workflowService: WorkflowServiceStubs  // Java type!
}
```

2. **KWorker**
```kotlin
// CURRENT - Java type exposed
public class KWorker(
  public val worker: Worker  // Java type!
)
```

### Proposed Solution: Public Facade + Internal Implementation

Split each class into a public facade (no Java imports) and internal implementation (all Java deps):

#### KClient Architecture

```kotlin
// PUBLIC: io.temporal.kotlin.client/KClient.kt
// NO Java SDK imports in this file!
package io.temporal.kotlin.client

public class KClient internal constructor(
  internal val impl: KClientInternal
) {
  public companion object {
    @JvmStatic
    public suspend fun connect(options: KClientOptions): KClient {
      return KClientInternal.connect(options)
    }

    @JvmStatic
    public suspend fun connect(): KClient {
      return KClientInternal.connectFromEnv()
    }
  }

  // All public methods delegate to impl
  public suspend fun <T, R> startWorkflow(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions
  ): KTypedWorkflowHandle<T, R> = impl.startWorkflow(workflow, options)

  // ... other methods delegate similarly
}

// INTERNAL: io.temporal.kotlin.internal.client/KClientInternal.kt
// All Java SDK imports are here
package io.temporal.kotlin.internal.client

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.serviceclient.WorkflowServiceStubs
// ... other Java imports

internal class KClientInternal(
  val workflowClient: WorkflowClient
) {
  val workflowService: WorkflowServiceStubs
    get() = workflowClient.workflowServiceStubs

  companion object {
    suspend fun connect(options: KClientOptions): KClient {
      return withContext(Dispatchers.IO) {
        val serviceStubs = WorkflowServiceStubs.newServiceStubs(options.toServiceStubsOptions())
        val client = WorkflowClient.newInstance(serviceStubs, options.toClientOptions())
        KClient(KClientInternal(client))
      }
    }

    suspend fun connectFromEnv(): KClient {
      return withContext(Dispatchers.IO) {
        val profile = ClientConfigProfile.load()
        val serviceStubs = WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions())
        val client = WorkflowClient.newInstance(serviceStubs, profile.toWorkflowClientOptions())
        KClient(KClientInternal(client))
      }
    }
  }

  // Actual implementation methods
  suspend fun <T, R> startWorkflow(
    workflow: KFunction1<T, R>,
    options: KWorkflowOptions
  ): KTypedWorkflowHandle<T, R> {
    // Implementation using Java SDK
  }
}
```

#### KWorker Architecture

```kotlin
// PUBLIC: io.temporal.kotlin.worker/KWorker.kt
// NO Java SDK imports in this file!
package io.temporal.kotlin.worker

public class KWorker internal constructor(
  internal val impl: KWorkerInternal
) {
  public companion object {
    public operator fun invoke(
      client: KClient,
      options: KWorkerOptions
    ): KWorker = KWorkerInternal.create(client, options)
  }

  public suspend fun run() = impl.run()
  public fun start() = impl.start()
  public fun shutdown() = impl.shutdown()

  // Registration methods delegate
  public fun registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>) {
    impl.registerWorkflowImplementationTypes(*workflowClasses)
  }

  public fun registerActivitiesImplementations(vararg activities: Any) {
    impl.registerActivitiesImplementations(*activities)
  }
}

// INTERNAL: io.temporal.kotlin.internal.worker/KWorkerInternal.kt
package io.temporal.kotlin.internal.worker

import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
// ... other Java imports

internal class KWorkerInternal(
  val worker: Worker,
  val workerFactory: WorkerFactory?,
  // ... other internal state
) {
  companion object {
    fun create(client: KClient, options: KWorkerOptions): KWorker {
      val kotlinPlugin = KotlinPlugin.create(KotlinPluginOptions())
      val factoryOptions = WorkerFactoryOptions.newBuilder()
        .addPlugin(kotlinPlugin)
        .build()
      val workerFactory = WorkerFactory.newInstance(client.impl.workflowClient, factoryOptions)
      val worker = workerFactory.newWorker(options.taskQueue, options.toWorkerOptions())

      return KWorker(KWorkerInternal(worker, workerFactory, ...))
    }
  }

  // Implementation methods
  suspend fun run() { ... }
  fun start() { workerFactory?.start() }
  // etc.
}
```

#### Java Interop Escape Hatch

For users who need Java SDK access (e.g., testing, migration):

```kotlin
// PUBLIC: io.temporal.kotlin/JavaInterop.kt
package io.temporal.kotlin

import io.temporal.client.WorkflowClient
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.Worker

/**
 * Extension to get the underlying Java WorkflowClient.
 *
 * WARNING: This exposes Java SDK types. Use only when necessary
 * for interop with existing Java code.
 */
public fun KClient.toJavaWorkflowClient(): WorkflowClient = impl.workflowClient

/**
 * Extension to get the underlying Java WorkflowServiceStubs.
 */
public fun KClient.toJavaServiceStubs(): WorkflowServiceStubs = impl.workflowService

/**
 * Extension to get the underlying Java Worker.
 */
public fun KWorker.toJavaWorker(): Worker = impl.worker
```

### Benefits

1. **Complete Isolation**: Public class files have zero Java SDK imports
2. **Clean Compilation Units**: IDE auto-import never suggests Java types from Kotlin SDK
3. **Internal Can Change Freely**: Implementation details fully hidden
4. **Explicit Interop**: Java access requires explicit import of `JavaInterop.kt`
5. **Better Testing**: Can mock `KClientInternal` for unit tests

---

## Refactoring Phases

### Phase 1: Restructure Internal Packages

1. Create new internal sub-packages:
   - `internal/workflow/`
   - `internal/activity/`
   - `internal/interceptor/`
   - `internal/converters/`
   - `internal/plugin/`

2. Move implementation classes:
   - `KotlinWorkflowContext` → `internal/workflow/`
   - `KotlinReplayWorkflow` → `internal/workflow/`
   - `KActivityContextImpl` → `internal/activity/`
   - `KotlinActivityWrapper` → `internal/activity/`
   - `KotlinPlugin` → `internal/plugin/`
   - etc.

3. Apply `internal` visibility to all moved classes

### Phase 2: Hide Java SDK Types

1. Make Java SDK properties `internal`:
   - `KClient.workflowClient` → `internal val javaClient`
   - `KWorker.worker` → `internal val javaWorker`

2. Create Java access escape hatch:
   - `KJavaAccess.kt` with explicit accessors
   - Document as advanced/interop use only

3. Update public API to use only Kotlin types

### Phase 3: Reduce @PublishedApi Usage

1. Audit all `@PublishedApi internal` usages
2. Refactor inline functions to minimize exposure
3. Create non-inline overloads where possible

### Phase 4: Fix Misplaced Files

1. Move `KotlinMethodReferenceDisassemblyService.kt` from `io.temporal.internal.async`
   to `io.temporal.kotlin.internal/service/`

---

## Breaking Changes

This refactoring will introduce breaking changes:

1. **Package Moves**: Implementation classes move to internal packages
   - `io.temporal.kotlin.activity.KActivityContextImpl` → `io.temporal.kotlin.internal.activity.KActivityContextImpl`
   - Users should not import implementation classes anyway

2. **Visibility Changes**: Properties become internal
   - `KClient.workflowClient` → `internal` (use `KJavaAccess` instead)
   - `KWorker.worker` → `internal` (use `KJavaAccess` instead)

3. **Extension Files**: Not affected (will be moved to separate module later)

### Migration Guide

```kotlin
// BEFORE: Accessing Java client
val javaClient = kClient.workflowClient
val stub = javaClient.newWorkflowStub(...)

// AFTER: Explicit Java access
import io.temporal.kotlin.internal.java.KJavaAccess
val javaAccess = KJavaAccess.forClient(kClient)
val stub = javaAccess.workflowClient.newWorkflowStub(...)

// OR: Use Kotlin API directly (preferred)
val handle = kClient.startWorkflow(MyWorkflow::execute, args, options)
```

---

## Implementation Checklist

### Phase 1: Create Internal Package Structure
- [ ] Create `internal/client/` package
- [ ] Create `internal/worker/` package
- [ ] Create `internal/workflow/` package
- [ ] Create `internal/activity/` package
- [ ] Create `internal/interceptor/` package
- [ ] Create `internal/converters/` package
- [ ] Create `internal/plugin/` package
- [ ] Create `internal/service/` package

### Phase 2: Split KClient → KClient + KClientInternal
- [ ] Create `KClientInternal` in `internal/client/`
- [ ] Refactor `KClient` as pure Kotlin facade (no Java imports)
- [ ] Move Java SDK operations to `KClientInternal`
- [ ] Create `KWorkflowHandleInternal` for handle implementation
- [ ] Create `KActivityCompletionClientInternal`
- [ ] Create `KScheduleHandleInternal`

### Phase 3: Split KWorker/KWorkerFactory → Internal Implementations
- [ ] Create `KWorkerInternal` in `internal/worker/`
- [ ] Refactor `KWorker` as pure Kotlin facade (no Java imports)
- [ ] Create `KWorkerFactoryInternal` in `internal/worker/`
- [ ] Refactor `KWorkerFactory` as pure Kotlin facade

### Phase 4: Move Workflow Implementation Classes
- [ ] Move `KotlinWorkflowContext` to `internal/workflow/`
- [ ] Move `KotlinReplayWorkflow` to `internal/workflow/`
- [ ] Move `KotlinDynamicReplayWorkflow` to `internal/workflow/`
- [ ] Move `KotlinWorkflowDefinition` to `internal/workflow/`
- [ ] Move `KotlinCoroutineDispatcher` to `internal/workflow/`
- [ ] Move `WorkflowContextElement` to `internal/workflow/`
- [ ] Move `KotlinDelay` to `internal/workflow/`

### Phase 5: Move Activity Implementation Classes
- [ ] Move `KActivityContextImpl` to `internal/activity/`
- [ ] Move `KotlinActivityWrapper` to `internal/activity/`
- [ ] Move `KDynamicActivityWrapper` to `internal/activity/`
- [ ] Move `KActivityRegistry` to `internal/activity/`
- [ ] Move `SuspendActivityContextWrapper` to `internal/activity/`
- [ ] Move `SuspendActivityContext` to `internal/activity/`
- [ ] Move `SuspendActivityThreadContext` to `internal/activity/`

### Phase 6: Move Other Implementation Classes
- [ ] Move `KotlinPlugin` to `internal/plugin/`
- [ ] Move `KotlinPluginOptions` to `internal/plugin/`
- [ ] Move `KOptionsConverters` to `internal/converters/`
- [ ] Move `KScheduleConverters` to `internal/converters/`
- [ ] Move interceptor implementations to `internal/interceptor/`
- [ ] Move `KotlinMethodReferenceDisassemblyService` to `internal/service/`

### Phase 7: Create Java Interop Escape Hatch
- [ ] Create `JavaInterop.kt` with extension functions
- [ ] Add `KClient.toJavaWorkflowClient()` extension
- [ ] Add `KClient.toJavaServiceStubs()` extension
- [ ] Add `KWorker.toJavaWorker()` extension
- [ ] Add `KWorkerFactory.toJavaWorkerFactory()` extension

### Phase 8: Apply Visibility Modifiers
- [ ] Apply `internal` visibility to all classes in `internal/` packages
- [ ] Audit and minimize `@PublishedApi internal` usage
- [ ] Ensure public facades have no Java SDK imports

### Phase 9: Split KWorkflow and Related Classes
- [ ] Create `KWorkflowInternal` in `internal/workflow/`
- [ ] Refactor `KWorkflow` as pure Kotlin facade
- [ ] Create `KChildWorkflowHandleInternal`
- [ ] Refactor `KChildWorkflowHandle` as facade
- [ ] Create `KExternalWorkflowHandleInternal`
- [ ] Refactor `KExternalWorkflowHandle` as facade
- [ ] Evaluate `KWorkflowInfo` - may need internal split
- [ ] Remove `Promise.toDeferred()` and `Promise.await()` extensions (no sync/suspend mixing)
- [ ] Remove `contextPropagators` from `KWorkflowOptions`
- [ ] Remove `contextPropagators` from `KChildWorkflowOptions`
- [ ] Remove `contextPropagators` from `KContinueAsNewOptions`

### Phase 10: Remove Samples
- [ ] Delete `temporal-kotlin/src/main/kotlin/io/temporal/samples/` directory
- [ ] Verify samples exist in `samples-java` repository

### Phase 11: Refactor temporal-kotlin-testing
- [ ] Create `internal/` package structure in testing module
- [ ] Split `KTestWorkflowEnvironment` → facade + internal
- [ ] Split `KTestActivityEnvironment` → facade + internal
- [ ] Move implementation classes to internal packages

### Phase 12: Update Tests and Documentation
- [ ] Update all imports in tests
- [ ] Update integration tests
- [ ] Update documentation
- [ ] Write migration guide for users accessing Java types

---

## Appendix: Full Class Migration Map

### New Internal Classes (Split from Public)

| Public Class | New Internal Class | Location |
|--------------|-------------------|----------|
| `KClient` | `KClientInternal` | `kotlin.internal.client` |
| `KWorkflowHandle` | `KWorkflowHandleInternal` | `kotlin.internal.client` |
| `KActivityCompletionClient` | `KActivityCompletionClientInternal` | `kotlin.internal.client` |
| `KScheduleHandle` | `KScheduleHandleInternal` | `kotlin.internal.client` |
| `KWorker` | `KWorkerInternal` | `kotlin.internal.worker` |
| `KWorkerFactory` | `KWorkerFactoryInternal` | `kotlin.internal.worker` |
| `KWorkflow` | `KWorkflowInternal` | `kotlin.internal.workflow` |
| `KChildWorkflowHandle` | `KChildWorkflowHandleInternal` | `kotlin.internal.workflow` |
| `KExternalWorkflowHandle` | `KExternalWorkflowHandleInternal` | `kotlin.internal.workflow` |

### Testing Module (temporal-kotlin-testing)

| Public Class | New Internal Class | Location |
|--------------|-------------------|----------|
| `KTestWorkflowEnvironment` | `KTestWorkflowEnvironmentInternal` | `kotlin.testing.internal` |
| `KTestActivityEnvironment` | `KTestActivityEnvironmentInternal` | `kotlin.testing.internal` |

### Existing Class Relocations

| Current Location | New Location | Visibility |
|-----------------|--------------|------------|
| `kotlin.internal.KotlinWorkflowContext` | `kotlin.internal.workflow.KotlinWorkflowContext` | `internal` |
| `kotlin.internal.KotlinReplayWorkflow` | `kotlin.internal.workflow.KotlinReplayWorkflow` | `internal` |
| `kotlin.internal.KotlinDynamicReplayWorkflow` | `kotlin.internal.workflow.KotlinDynamicReplayWorkflow` | `internal` |
| `kotlin.internal.KotlinWorkflowDefinition` | `kotlin.internal.workflow.KotlinWorkflowDefinition` | `internal` |
| `kotlin.internal.KotlinCoroutineDispatcher` | `kotlin.internal.workflow.KotlinCoroutineDispatcher` | `internal` |
| `kotlin.internal.WorkflowContextElement` | `kotlin.internal.workflow.WorkflowContextElement` | `internal` |
| `kotlin.internal.KotlinDelay` | `kotlin.internal.workflow.KotlinDelay` | `internal` |
| `kotlin.activity.KActivityContextImpl` | `kotlin.internal.activity.KActivityContextImpl` | `internal` |
| `kotlin.activity.KotlinActivityWrapper` | `kotlin.internal.activity.KotlinActivityWrapper` | `internal` |
| `kotlin.activity.SuspendActivityContextWrapper` | `kotlin.internal.activity.SuspendActivityContextWrapper` | `internal` |
| `kotlin.activity.SuspendActivityContext` | `kotlin.internal.activity.SuspendActivityContext` | `internal` |
| `kotlin.activity.SuspendActivityThreadContext` | `kotlin.internal.activity.SuspendActivityThreadContext` | `internal` |
| `kotlin.activity.KActivityRegistry` | `kotlin.internal.activity.KActivityRegistry` | `internal` |
| `kotlin.internal.KDynamicActivityWrapper` | `kotlin.internal.activity.KDynamicActivityWrapper` | `internal` |
| `kotlin.worker.KotlinPlugin` | `kotlin.internal.plugin.KotlinPlugin` | `internal` |
| `kotlin.worker.KotlinPluginOptions` | `kotlin.internal.plugin.KotlinPluginOptions` | `internal` |
| `kotlin.internal.KOptionsConverters` | `kotlin.internal.converters.KOptionsConverters` | `internal` |
| `kotlin.internal.KScheduleConverters` | `kotlin.internal.converters.KScheduleConverters` | `internal` |
| `kotlin.internal.interceptor.*` | `kotlin.internal.interceptor.*` | `internal` |
| `internal.async.KotlinMethodReferenceDisassemblyService` | `kotlin.internal.service.KotlinMethodReferenceDisassemblyService` | `internal` |

---

## Decisions Made

1. **Testing Module**: Yes, `temporal-kotlin-testing` follows the same facade + internal pattern.

2. **Samples**: Remove from Kotlin SDK - they already exist in `samples-java`.

3. **KWorkflow**: Yes, needs the split - it has Java SDK dependencies.

---

## Java SDK Type Analysis

### Category 1: Proto Classes (`io.temporal.api.*`) - MUST HIDE

These are low-level protobuf generated classes that should never be in public API:

```
io.temporal.api.command.v1.*          # ScheduleActivityTaskCommandAttributes, etc.
io.temporal.api.common.v1.*           # Payloads, WorkflowExecution, Memo, ActivityType, etc.
io.temporal.api.enums.v1.*            # ParentClosePolicy, WorkflowIdReusePolicy, etc.
io.temporal.api.failure.v1.Failure
io.temporal.api.history.v1.HistoryEvent
io.temporal.api.query.v1.WorkflowQuery
io.temporal.api.sdk.v1.UserMetadata
io.temporal.api.taskqueue.v1.TaskQueue
io.temporal.api.workflowservice.v1.*  # DescribeWorkflowExecutionResponse, etc.
```

**Action**: All proto types must be hidden in internal packages.

### Category 2: Internal Java SDK (`io.temporal.internal.*`) - MUST HIDE

These are Java SDK implementation details:

```
io.temporal.internal.common.ProtobufTimeUtils
io.temporal.internal.common.ProtoConverters
io.temporal.internal.common.SearchAttributesUtil
io.temporal.internal.replay.ReplayWorkflow
io.temporal.internal.replay.ReplayWorkflowContext
io.temporal.internal.statemachines.*
io.temporal.internal.worker.WorkflowImplementationFactory
```

**Action**: All internal Java SDK types must be hidden in internal packages.

### Category 3: Annotations - KEEP in Public API

User code needs these annotations:

```
io.temporal.activity.ActivityInterface
io.temporal.activity.ActivityMethod
io.temporal.workflow.WorkflowInterface
io.temporal.workflow.WorkflowMethod
io.temporal.workflow.SignalMethod
io.temporal.workflow.QueryMethod
io.temporal.workflow.UpdateMethod
```

**Action**: Keep - users annotate their code with these. No need to wrap.

### Category 4: Java SDK Enums - KEEP in Public API

These are stable, user-facing enums:

```
io.temporal.activity.ActivityCancellationType
io.temporal.workflow.ChildWorkflowCancellationType
io.temporal.client.WorkflowUpdateStage
```

**Note**: The proto enums (`io.temporal.api.enums.v1.*`) should be wrapped or replaced with Java SDK equivalents where available.

**Action**: Keep Java SDK enums (non-proto). Hide or wrap proto enums.

### Category 5: Complex Types - EVALUATE

| Type | Decision | Reason |
|------|----------|--------|
| `SearchAttributes` | Keep | User-facing, typed API |
| `SearchAttributeKey` | Keep | User-facing, typed API |
| `SearchAttributeUpdate` | Keep | User-facing, typed API |
| `Priority` | Keep | Simple value class |
| `RetryOptions` | Replace | Use `KRetryOptions` (already exists) |
| `ContextPropagator` | Remove | Remove from Kotlin APIs entirely |
| `Promise` | Remove | Kotlin SDK uses suspend/Deferred, no sync interop |
| `DataConverter` | Keep | Advanced users need this |

**Note on Promise**: The Kotlin SDK is fully suspend-based and uses `Deferred<R>`.
`Promise` is for synchronous Java SDK code and should not be mixed with suspend functions.
Remove `Promise.toDeferred()` and `Promise.await()` extensions entirely.

### Category 6: Client/Worker Classes - HIDE via Facade Pattern

These are wrapped with the facade + internal pattern:

```
io.temporal.client.WorkflowClient         -> KClient + KClientInternal
io.temporal.client.WorkflowStub           -> KWorkflowHandle + internal
io.temporal.client.schedules.*            -> KSchedule* + internal
io.temporal.worker.Worker                 -> KWorker + KWorkerInternal
io.temporal.worker.WorkerFactory          -> KWorkerFactory + internal
io.temporal.serviceclient.WorkflowServiceStubs -> hidden in KClientInternal
```

### Summary: What Goes Where

| Category | Location | Example |
|----------|----------|---------|
| Proto classes | `internal/` only | `Payloads`, `WorkflowExecution` |
| Java internal | `internal/` only | `ReplayWorkflowContext` |
| Annotations | Public (re-export or direct use) | `@WorkflowMethod` |
| Java SDK enums | Public | `ActivityCancellationType` |
| Search attributes | Public | `SearchAttributes` |
| Client/Worker | Facade in public, impl in internal | `KClient` |
| Options | Public with converters in internal | `KWorkflowOptions` |
