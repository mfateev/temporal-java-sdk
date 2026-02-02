# Gap Analysis: temporal-kotlin vs temporal-kotlin-sdk-alpha

This document identifies all features in `temporal-kotlin` (Java SDK wrapper) that need to be implemented in `temporal-kotlin-sdk-alpha` (temporal-core based) for 100% feature parity.

## Summary

| Category | Old Module | New Module | Gap |
|----------|------------|------------|-----|
| KWorkflow APIs | 40+ methods | 12 methods | **CRITICAL** |
| KClient APIs | 50+ methods | 8 methods | **CRITICAL** |
| KWorker/Factory | Full runtime | Stub only | **CRITICAL** |
| Handles | 5 classes | 1 class | **HIGH** |
| Testing | Full + JUnit | Basic only | **MEDIUM** |
| Schedules | Full | Data only | **HIGH** |
| Nexus | Supported | None | **MEDIUM** |

---

## 1. KWorkflow - Workflow Context APIs [CRITICAL]

### 1.1 Activity Execution (MISSING - CRITICAL)

The most critical gap. Workflows cannot execute activities.

```kotlin
// OLD - temporal-kotlin has 15+ overloads:
suspend fun <T, R> executeActivity(activity: KFunction1<T, R>, options: KActivityOptions): R
suspend fun <T, A1, R> executeActivity(activity: KFunction2<T, A1, R>, arg1: A1, options: KActivityOptions): R
suspend fun <T, A1, A2, R> executeActivity(activity: KFunction3<T, A1, A2, R>, args: KArgs2<A1, A2>, options: KActivityOptions): R
// ... up to 6 arguments

// Also with KArgs wrapper:
suspend fun <T, R> executeActivity(activity: KSuspendFunction1<T, R>, args: KEncodedValues, options: KActivityOptions): R
```

**NEW - temporal-kotlin-sdk-alpha**: Not implemented

### 1.2 Local Activity Execution (MISSING)

```kotlin
// OLD:
suspend fun <T, R> executeLocalActivity(activity: KFunction1<T, R>, options: KLocalActivityOptions): R
// ... 15+ overloads similar to executeActivity
```

**NEW**: Not implemented

### 1.3 Child Workflow Execution (MISSING)

```kotlin
// OLD:
suspend fun <T, R> executeChildWorkflow(workflow: KSuspendFunction1<T, R>, options: KChildWorkflowOptions): R
suspend fun <T, R> startChildWorkflow(workflow: KSuspendFunction1<T, R>, options: KChildWorkflowOptions): KChildWorkflowHandle<T, R>
// ... multiple overloads
```

**NEW**: Not implemented

### 1.4 Signal/Query/Update Handlers (MISSING)

```kotlin
// OLD - Typed handlers:
fun registerSignalHandler(signal: KFunction1<T, Unit>, handler: () -> Unit)
fun <A> registerSignalHandler(signal: KFunction2<T, A, Unit>, handler: (A) -> Unit)
fun <R> registerQueryHandler(query: KFunction1<T, R>, handler: () -> R)
fun <R> registerUpdateHandler(update: KFunction1<T, R>, handler: () -> R)
fun registerUpdateHandler(update: KFunction1<T, R>, validator: (KEncodedValues) -> Unit, handler: () -> R)

// Dynamic handlers:
suspend fun registerDynamicSignalHandler(handler: (signalName: String, args: KEncodedValues) -> Unit)
fun registerDynamicQueryHandler(handler: (queryType: String, args: KEncodedValues) -> Any?)
fun registerDynamicUpdateHandler(handler: (updateName: String, args: KEncodedValues) -> Any?)
fun registerDynamicUpdateValidator(validator: (updateName: String, args: KEncodedValues) -> Unit)
```

**NEW**: Dynamic handlers declared but throw `UnsupportedOperationException`

### 1.5 External Workflow Interaction (MISSING)

```kotlin
// OLD:
fun <T> getExternalWorkflowHandle(workflowId: String, workflowInterface: Class<T>): KExternalWorkflowHandle<T>
fun getExternalWorkflowHandle(workflowId: String): KUntypedExternalWorkflowHandle
```

**NEW**: Not implemented

### 1.6 Workflow State APIs (PARTIAL)

| API | Old | New |
|-----|-----|-----|
| `info: KWorkflowInfo` | ✅ | ✅ |
| `now(): Instant` | ✅ | ✅ |
| `currentTimeMillis(): Long` | ✅ | ✅ |
| `randomUUID()` | ✅ (UUID) | ✅ (String) |
| `newRandom(): Random` | ✅ | ❌ |
| `getVersion()` | ✅ | ✅ |
| `sideEffect()` | ✅ | ✅ |
| `mutableSideEffect()` | ✅ | ❌ |
| `isCancelRequested()` | ✅ | ❌ |
| `isReplaying` | ✅ | ❌ |
| `logger()` | ✅ | ❌ |

### 1.7 Search Attributes & Memo (MISSING)

```kotlin
// OLD:
val typedSearchAttributes: SearchAttributes
fun <T> getSearchAttribute(key: SearchAttributeKey<T>): T?
fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>)
fun <T> getMemo(key: String, valueClass: Class<T>): T?
fun upsertMemo(memo: Map<String, Any?>)
```

**NEW**: Not implemented

### 1.8 Continue-As-New (PARTIAL)

```kotlin
// OLD:
fun continueAsNew(vararg args: Any?): Nothing
fun continueAsNew(options: KContinueAsNewOptions, vararg args: Any?): Nothing
fun <T> continueAsNew(workflow: KSuspendFunction1<T, *>, options: KContinueAsNewOptions, vararg args: Any?): Nothing
```

**NEW**: Only basic `continueAsNew(vararg args: Any?)` without options

### 1.9 Other Missing Workflow APIs

```kotlin
// OLD:
val metricsScope: Scope
fun <R> getLastCompletionResult(resultClass: Class<R>): R?
val previousRunFailure: Exception?
val currentUpdateInfo: UpdateInfo?
val isEveryHandlerFinished: Boolean
var currentDetails: String?
suspend fun <R> retry(options: KWorkflowRetryOptions, block: suspend () -> R): R
```

---

## 2. KClient - Client APIs [CRITICAL]

### 2.1 Workflow Execution (MISSING - CRITICAL)

```kotlin
// OLD - 14+ overloads each:
suspend fun <T, R> startWorkflow(workflow: KSuspendFunction1<T, R>, options: KWorkflowOptions): KWorkflowHandle<T, R>
suspend fun <T, A1, R> startWorkflow(workflow: KSuspendFunction2<T, A1, R>, arg1: A1, options: KWorkflowOptions): KWorkflowHandle<T, R>
// ... up to 6 arguments

suspend fun <T, R> executeWorkflow(workflow: KSuspendFunction1<T, R>, options: KWorkflowOptions): R
// ... 14+ overloads

// Untyped:
suspend fun startWorkflow(workflowType: String, options: KWorkflowOptions, vararg args: Any?): WorkflowHandle
suspend fun executeWorkflow(workflowType: String, options: KWorkflowOptions, vararg args: Any?): Any?
```

**NEW**: Only `newWorkflowStub()` that creates a stub but no actual execution

### 2.2 Workflow Handles (MISSING)

```kotlin
// OLD:
fun <T> getWorkflowHandle(workflowId: String, workflowClass: Class<T>): KWorkflowHandle<T>
fun <T> getWorkflowHandle(workflowId: String, runId: String, workflowClass: Class<T>): KWorkflowHandle<T>
fun getUntypedWorkflowHandle(workflowId: String): WorkflowHandle
```

**NEW**: Only `getWorkflowStub()` returning `KWorkflowStub` (no interaction methods)

### 2.3 Signal-With-Start (MISSING)

```kotlin
// OLD - Multiple overloads:
suspend fun signalWithStart(workflowType: String, options: KWorkflowOptions, signalName: String, signalArgs: Array<Any?>, workflowArgs: Array<Any?>): WorkflowExecution
suspend fun <T, R> signalWithStart(workflow: KSuspendFunction1<T, R>, signal: KFunction1<T, Unit>, options: KWorkflowOptions): KWorkflowHandle<T, R>
// ... many typed variants
```

### 2.4 Update-With-Start (MISSING)

```kotlin
// OLD:
suspend fun <T, R, UR> startUpdateWithStart(update: KFunction1<T, UR>, options: KUpdateWithStartOptions<T, R>): KUpdateHandle<UR>
suspend fun <T, R, UR> executeUpdateWithStart(update: KFunction1<T, UR>, options: KUpdateWithStartOptions<T, R>): UR
// ... many overloads
```

### 2.5 Schedule Management (PARTIAL)

```kotlin
// OLD:
suspend fun createSchedule(scheduleId: String, schedule: KSchedule, options: KScheduleOptions): KScheduleHandle
fun scheduleHandle(scheduleId: String): KScheduleHandle
fun listSchedules(): Flow<KScheduleListDescription>
fun listSchedules(query: String?, pageSize: Int?): Flow<KScheduleListDescription>
```

**NEW**: Schedule data classes exist but no client methods to create/manage schedules

### 2.6 Activity Completion Client (MISSING)

```kotlin
// OLD:
fun newActivityCompletionClient(): KActivityCompletionClient
```

### 2.7 Connection Options (PARTIAL)

```kotlin
// OLD:
suspend fun connect(options: KClientOptions): KClient
suspend fun connect(): KClient  // Uses ClientConfigProfile
suspend fun connect(profile: ClientConfigProfile): KClient
```

**NEW**: Only `connect(target, options)` and `create(stubs, options)`

---

## 3. Handle Classes [HIGH]

### 3.1 KWorkflowHandle (MISSING)

```kotlin
// OLD:
class KWorkflowHandle<T, R>(
    val workflowId: String,
    val runId: String?,
    val execution: WorkflowExecution,
) {
    suspend fun signal(signalName: String, vararg args: Any?)
    suspend fun <R> query(queryName: String, resultClass: Class<R>, vararg args: Any?): R
    suspend fun <R> executeUpdate(updateName: String, resultClass: Class<R>, vararg args: Any?): R
    suspend fun cancel()
    suspend fun terminate(reason: String? = null)
    suspend fun describe(): KWorkflowExecutionDescription
    suspend fun <R> getResult(resultClass: Class<R>): R
    fun <R> getUpdateHandle(updateId: String, resultClass: Class<R>): KUpdateHandle<R>

    // Typed signal/query/update methods (15+ each)
    suspend fun signal(signal: KFunction1<T, Unit>)
    suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A)
    // ...
}
```

**NEW**: Only `KWorkflowStub` with `workflowId`, `runId`, `workflowType` - no interaction methods

### 3.2 KChildWorkflowHandle (MISSING)

```kotlin
// OLD:
class KChildWorkflowHandle<T, R>(
    val workflowId: String,
    val firstExecutionRunId: String,
) {
    suspend fun result(): R
    suspend fun signal(signal: KFunction1<T, Unit>)
    // ... typed signal methods
    suspend fun signal(signalName: String, vararg args: Any?)
    suspend fun cancel()
}
```

### 3.3 KExternalWorkflowHandle (MISSING)

```kotlin
// OLD:
class KExternalWorkflowHandle<T>(
    val workflowId: String,
    val runId: String?,
) {
    fun signal(method: KFunction1<T, *>)
    // ... typed signal methods
    fun cancel()
    fun cancel(reason: String?)
}

class KUntypedExternalWorkflowHandle(
    val workflowId: String,
    val runId: String?,
) {
    fun signal(signalName: String, vararg args: Any?)
    fun cancel()
}
```

### 3.4 KScheduleHandle (MISSING)

```kotlin
// OLD (inferred from KClient usage):
class KScheduleHandle {
    suspend fun describe(): KScheduleDescription
    suspend fun update(updater: (KScheduleUpdateInput) -> KScheduleUpdate)
    suspend fun delete()
    suspend fun pause(note: String? = null)
    suspend fun unpause(note: String? = null)
    suspend fun trigger()
    suspend fun backfill(backfills: List<KScheduleBackfill>)
}
```

### 3.5 KUpdateHandle (MISSING)

```kotlin
// OLD:
class KUpdateHandle<R>(
    val updateId: String,
    val execution: WorkflowExecution,
) {
    suspend fun result(): R
    suspend fun result(timeout: Duration): R
}
```

### 3.6 KActivityCompletionClient/Handle (MISSING)

```kotlin
// OLD:
class KActivityCompletionClient {
    fun <R> newCompletionHandle(taskToken: ByteArray, resultClass: Class<R>): KActivityCompletionHandle<R>
}

class KActivityCompletionHandle<R> {
    fun complete(result: R)
    fun fail(exception: Throwable)
    fun reportCancellation(details: Any?)
    fun heartbeat(details: Any?)
}
```

---

## 4. KWorker / KWorkerFactory [CRITICAL]

### 4.1 Workflow/Activity Runtime (MISSING - CRITICAL)

The new SDK registers types but has no actual execution runtime.

```kotlin
// OLD - Full runtime infrastructure:
- KotlinPlugin
- KotlinWorkflowDefinition
- KotlinWorkflowImplementationFactory
- KotlinActivityWrapper
- KotlinCoroutineDispatcher
- KotlinReplayWorkflow
- InterceptorChain
- Root interceptors
```

**NEW**: Only stub implementations that track registered types

### 4.2 Worker Lifecycle (PARTIAL)

| Method | Old | New |
|--------|-----|-----|
| `start()` | ✅ (real) | ✅ (stub) |
| `run()` | ✅ | ❌ |
| `shutdown()` | ✅ | ✅ (stub) |
| `shutdownNow()` | ✅ | ✅ (stub) |
| `awaitTermination()` | ✅ | ✅ (stub) |
| `isStarted()` | ✅ | ✅ |
| `isShutdown()` | ✅ | ✅ |
| `isTerminated()` | ✅ | ✅ |

### 4.3 Registration Methods (PARTIAL)

```kotlin
// OLD:
fun registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>)
fun registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>, options: WorkflowImplementationOptions.Builder.() -> Unit)
fun registerActivitiesImplementations(vararg activities: Any)
fun registerNexusServiceImplementation(vararg services: Any)
```

**NEW**: Basic registration without options, no Nexus support

---

## 5. Testing Module [MEDIUM]

### 5.1 KTestWorkflowEnvironment (PARTIAL)

| Feature | Old | New |
|---------|-----|-----|
| `namespace` | ✅ | ✅ |
| `workflowClient` / `workflowServiceStubs` | ✅ | ✅ |
| `operatorServiceStubs` | ✅ | ✅ |
| `currentTimeMillis` / `currentTime` | ✅ | ✅ |
| `sleep()` (Kotlin/Java Duration) | ✅ | ✅ |
| `registerDelayedCallback()` | ✅ | ❌ |
| `newWorker()` | ✅ | ❌ |
| `registerActivitiesImplementations()` | ✅ | ❌ |
| `registerSearchAttribute()` | ✅ | ✅ |
| `createNexusEndpoint()` / `deleteNexusEndpoint()` | ✅ | ✅ |
| `getDiagnostics()` | ✅ | ✅ |
| Lifecycle methods | ✅ | ✅ |

### 5.2 JUnit Extensions (MISSING)

```kotlin
// OLD:
class KTestWorkflowExtension : BeforeEachCallback, AfterEachCallback, ParameterResolver
fun kTestWorkflowExtension(block: KTestWorkflowExtensionBuilder.() -> Unit): KTestWorkflowExtension

class KTestActivityExtension : BeforeEachCallback, AfterEachCallback, ParameterResolver
```

### 5.3 Activity Testing (MISSING)

```kotlin
// OLD:
class KTestActivityEnvironment {
    fun <T> newActivityStub(activityClass: Class<T>): T
    fun registerActivitiesImplementations(vararg activities: Any)
    fun setActivityHeartbeatListener(listener: (Any?) -> Unit)
}
```

### 5.4 Mocking Support (MISSING)

```kotlin
// OLD:
object KActivityMocking {
    fun mock(activityInterface: KClass<*>): Any
}
```

---

## 6. Activity Context [PARTIAL]

### 6.1 KActivityContext

| API | Old | New |
|-----|-----|-----|
| `current` (static) | ✅ | ✅ |
| `info: KActivityInfo` | ✅ | ✅ (partial) |
| `heartbeat(details)` | ✅ | ✅ |
| `heartbeatDetails<T>()` | ✅ | ❌ |
| `taskToken` | ✅ | ✅ |
| `doNotCompleteOnReturn()` | ✅ | ✅ |
| `isDoNotCompleteOnReturn` | ✅ | ✅ |

### 6.2 KActivityInfo (PARTIAL)

The new SDK has `KActivityInfo` but may be missing some fields. Need to verify completeness.

---

## 7. Options & Data Classes [PARTIAL]

### 7.1 Existing in Both (verify completeness)

- `KActivityOptions`
- `KLocalActivityOptions`
- `KChildWorkflowOptions`
- `KWorkflowOptions`
- `KClientOptions`
- `KWorkerOptions`
- `KWorkerFactoryOptions`
- `KRetryOptions`
- `KContinueAsNewOptions`
- `KPriority`
- All `KSchedule*` classes

### 7.2 Missing Options Classes

- `KWorkflowImplementationOptions`
- `KOnConflictOptions`
- `KUpdateWithStartOptions`
- `KActivityCancellationType` (enum)

---

## 8. Nexus Support [MEDIUM]

### 8.1 Workflow-Side (MISSING)

```kotlin
// OLD (via extension):
// NexusServiceOptionsExt.kt
// NexusOperationOptionsExt.kt
// Integration with KWorkflow for Nexus operations
```

### 8.2 Worker-Side (MISSING)

```kotlin
// OLD:
fun registerNexusServiceImplementation(vararg services: Any)
```

---

## 9. Java Interop & Converters [LOW]

### 9.1 Extension Files (NOT NEEDED for alpha)

The old SDK has many `*Ext.kt` files for extending Java SDK classes. These are not needed in the alpha since it doesn't wrap the Java SDK.

### 9.2 Converters (PARTIAL)

- `OptionsConverters` exists in new SDK
- Missing: Schedule converters for client operations

---

## 10. Priority Implementation Order

### Phase 1: Core Workflow Execution (CRITICAL)
1. Workflow execution runtime (`KotlinPlugin`, `KotlinWorkflowDefinition`, etc.)
2. `KWorkflow.executeActivity()` and `executeLocalActivity()`
3. `KClient.startWorkflow()` and `executeWorkflow()`
4. `KWorkflowHandle` with full interaction methods

### Phase 2: Advanced Workflow Features (HIGH)
1. Child workflow execution
2. Signal/Query/Update handlers in workflows
3. External workflow interaction
4. Search attributes and memo
5. Continue-as-new with options

### Phase 3: Client Features (HIGH)
1. Signal-with-start
2. Schedule client methods
3. `KScheduleHandle`
4. Activity completion client

### Phase 4: Testing (MEDIUM)
1. JUnit 5 extensions
2. Activity testing environment
3. Mocking support
4. `registerActivitiesImplementations()` in test env

### Phase 5: Nexus (MEDIUM)
1. Worker registration
2. Workflow-side Nexus operations

### Phase 6: Polish (LOW)
1. Remaining workflow context APIs
2. Full options parity
3. Edge cases and error handling

---

## 11. Estimated Effort

| Phase | Complexity | Files to Create/Modify |
|-------|------------|------------------------|
| Phase 1 | Very High | 15-20 files |
| Phase 2 | High | 10-15 files |
| Phase 3 | Medium | 8-12 files |
| Phase 4 | Medium | 6-10 files |
| Phase 5 | Medium | 4-6 files |
| Phase 6 | Low | Various |

**Total**: Significant effort required for full parity. The core workflow execution runtime (Phase 1) is the foundation that must be completed before most other features can work.
