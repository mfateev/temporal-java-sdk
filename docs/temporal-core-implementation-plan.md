# Temporal Core Refactoring - Implementation Plan

## Overview

This document provides a detailed implementation plan for the temporal-core refactoring described in `temporal-core-protobuf-api-proposal.md`. The goal is to create a minimal `temporal-core` module and an independent `temporal-kotlin-sdk-alpha`.

## Prerequisites

Before starting implementation:

1. **Ensure all tests pass** on the current codebase
2. **Create a feature branch** for this work (e.g., `feature/temporal-core-extraction`)
3. **Set up CI** to build and test all modules
4. **Document current public API surface** for backward compatibility verification

---

## Mandatory Requirements for Every Phase

**CRITICAL**: The following requirements are NON-NEGOTIABLE for each phase:

### 1. Compilation
- All modules MUST compile successfully after each task
- No `@Suppress` annotations to hide compilation errors
- No commented-out code to bypass compilation issues

### 2. Linting
- All code MUST pass lint checks (ktlint for Kotlin, checkstyle for Java)
- No suppression of lint rules to bypass issues
- Code style must be consistent with existing codebase

### 3. Unit Test Policy

**ABSOLUTELY NO TEST DELETION OR DISABLING**

- Every single existing unit test MUST continue to pass
- You are NOT allowed to:
  - Delete any test
  - Disable any test with `@Disabled`, `@Ignore`, or similar annotations
  - Comment out any test
  - Change test assertions to make failing tests pass
  - Skip tests in build configuration
- When moving code to a new module:
  - The corresponding tests MUST be moved with the code
  - Tests must pass in the new location
  - Original tests may be converted to integration tests if they now test across module boundaries

### 4. Phase Gate Checklist

Before marking ANY phase as complete, verify:

```bash
# Must all succeed
./gradlew clean build                    # Compilation
./gradlew check                          # Linting
./gradlew test                           # Unit tests
./gradlew integrationTest                # Integration tests (if applicable)

# Verify test count hasn't decreased
./gradlew test --info | grep "tests found"
```

### 5. Test Migration Tracking

Maintain a spreadsheet/document tracking:
| Original Test Class | Original Module | New Module | Status |
|---------------------|-----------------|------------|--------|
| GenericWorkflowClientTest | temporal-sdk | temporal-core | Migrated ✓ |
| ... | ... | ... | ... |

Every test must be accounted for. The total test count across all modules must be >= the original count.

---

## Phase 1: Extract Core Module

**Goal**: Create `temporal-core` module with protobuf-only interfaces.

**Duration estimate**: This phase has the most foundational work.

### Task 1.1: Create Module Structure

```
temporal-core/
├── build.gradle.kts
├── src/main/java/io/temporal/core/
│   ├── client/
│   ├── worker/
│   ├── activity/
│   ├── converter/
│   ├── common/
│   └── replay/
└── src/test/java/io/temporal/core/
```

**Steps**:
1. Create `temporal-core` directory and `build.gradle.kts`
2. Configure dependencies: only `temporal-serviceclient`, protobuf, gRPC
3. Add module to root `settings.gradle.kts`
4. Verify empty module builds

**Validation**: `./gradlew :temporal-core:build` succeeds

---

### Task 1.2: Extract GenericWorkflowClient Interface

**Current location**: `io.temporal.internal.client.external.GenericWorkflowClient`

**Target location**: `io.temporal.core.client.GenericWorkflowClient`

**Steps**:
1. Copy interface to `temporal-core`
2. Ensure all method signatures use only protobuf types
3. Remove any Java SDK type references
4. Add missing schedule operations if not present:
   - `createSchedule`
   - `describeSchedule`
   - `updateSchedule`
   - `patchSchedule`
   - `deleteSchedule`
   - `listSchedulesAsync`

**Interface**:
```java
package io.temporal.core.client;

public interface GenericWorkflowClient {
    // Workflow lifecycle
    StartWorkflowExecutionResponse start(StartWorkflowExecutionRequest request);
    SignalWorkflowExecutionResponse signal(SignalWorkflowExecutionRequest request);
    SignalWithStartWorkflowExecutionResponse signalWithStart(SignalWithStartWorkflowExecutionRequest request);

    // Queries and updates
    QueryWorkflowResponse query(QueryWorkflowRequest request);
    UpdateWorkflowExecutionResponse update(UpdateWorkflowExecutionRequest request, Deadline deadline);
    CompletableFuture<PollWorkflowExecutionUpdateResponse> pollUpdateAsync(
        PollWorkflowExecutionUpdateRequest request, Deadline deadline);

    // ... all other operations
}
```

**Validation**: Interface compiles with no Java SDK imports

---

### Task 1.3: Extract GenericWorkflowClient Implementation

**Current location**: `io.temporal.internal.client.external.GenericWorkflowClientImpl`

**Target location**: `io.temporal.core.client.GenericWorkflowClientImpl`

**Steps**:
1. Copy implementation to `temporal-core`
2. Move dependent classes:
   - `GrpcRetryer` → `io.temporal.core.client.GrpcRetryer`
   - `WorkflowResultPoller` / long-poll helpers
3. Update imports to use core package
4. Ensure no Java SDK type references

**Validation**: Implementation compiles and unit tests pass

---

### Task 1.4: Extract DataConverter Interface

**Current location**: `io.temporal.common.converter.DataConverter`

**Target location**: `io.temporal.core.converter.DataConverter`

**Steps**:
1. Copy `DataConverter` interface to core
2. Copy `DefaultDataConverter` implementation
3. Copy payload converters:
   - `PayloadConverter`
   - `NullPayloadConverter`
   - `ByteArrayPayloadConverter`
   - `ProtobufJsonPayloadConverter`
   - `ProtobufPayloadConverter`
   - `JacksonJsonPayloadConverter`
4. Copy `FailureConverter` interface and implementation
5. Verify no SDK-specific types in signatures

**Validation**: DataConverter tests pass in core module

---

### Task 1.5: Extract Utility Classes

**Target package**: `io.temporal.core.common`

**Classes to move**:
| Current Location | Target Location |
|-----------------|-----------------|
| `io.temporal.internal.common.ProtobufTimeUtils` | `io.temporal.core.common.ProtobufTimeUtils` |
| `io.temporal.internal.common.SearchAttributesUtil` | `io.temporal.core.common.SearchAttributesUtil` |
| `io.temporal.internal.common.ProtoConverters` | `io.temporal.core.common.ProtoConverters` |
| `io.temporal.internal.common.RetryOptionsUtils` | `io.temporal.core.common.RetryOptionsUtils` |

**Steps**:
1. Copy each utility class
2. Remove any SDK-specific type usage
3. Update internal references
4. Add unit tests if missing

**Validation**: All utility class tests pass

---

### Task 1.6: Extract Worker Internal Interfaces

**Target package**: `io.temporal.core.worker`

**Steps**:

1. **Create WorkerOptionsInternal**:
```java
package io.temporal.core.worker;

public class WorkerOptionsInternal {
    private final int maxConcurrentActivityTaskExecutors;
    private final int maxConcurrentWorkflowTaskExecutors;
    private final int maxConcurrentLocalActivityTaskExecutors;
    private final int maxConcurrentWorkflowTaskPollers;
    private final int maxConcurrentActivityTaskPollers;
    private final double maxActivitiesPerSecond;
    private final double maxTaskQueueActivitiesPerSecond;
    private final boolean localActivityWorkerOnly;
    private final long defaultDeadlockDetectionTimeoutMs;
    private final long maxHeartbeatThrottleIntervalMs;
    private final long defaultHeartbeatThrottleIntervalMs;
    private final double stickyQueueScheduleToStartTimeoutRatio;
    private final boolean disableEagerExecution;
    private final boolean useBuildIdForVersioning;
    private final String buildId;
    private final String identity;

    public static Builder newBuilder() { ... }
}
```

2. **Create WorkerFactoryOptionsInternal**:
```java
package io.temporal.core.worker;

public class WorkerFactoryOptionsInternal {
    private final int workflowCacheSize;
    private final int maxWorkflowThreadCount;
    private final long workflowHostLocalTaskQueueScheduleToStartTimeoutMs;
    private final boolean enableLoggingInReplay;

    public static Builder newBuilder() { ... }
}
```

3. **Create WorkerInternal interface**:
```java
package io.temporal.core.worker;

public interface WorkerInternal {
    void addWorkflowImplementationFactory(WorkflowImplementationFactory factory);
    void registerActivitiesImplementations(Object... activities);
    void registerNexusServiceImplementation(Object nexusService);
    void start();
    void shutdown();
    void shutdownNow();
    boolean awaitTermination(long timeout, TimeUnit unit);
    String getTaskQueue();
    boolean isSuspended();
    void suspendPolling();
    void resumePolling();
}
```

4. **Create WorkerFactoryInternal interface**:
```java
package io.temporal.core.worker;

public interface WorkerFactoryInternal {
    WorkerInternal newWorker(String taskQueue, WorkerOptionsInternal options);
    void start();
    void shutdown();
    void shutdownNow();
    boolean awaitTermination(long timeout, TimeUnit unit);
    boolean isStarted();
    boolean isShutdown();
    boolean isTerminated();
}
```

**Validation**: Interfaces compile with no SDK imports

---

### Task 1.7: Extract Replay/State Machine Internals

**Target packages**:
- `io.temporal.core.replay`
- `io.temporal.core.statemachines`

**Classes to move**:

| Current Location | Target Location |
|-----------------|-----------------|
| `io.temporal.internal.replay.ReplayWorkflow` | `io.temporal.core.replay.ReplayWorkflow` |
| `io.temporal.internal.replay.ReplayWorkflowContext` | `io.temporal.core.replay.ReplayWorkflowContext` |
| `io.temporal.internal.replay.WorkflowContext` | `io.temporal.core.replay.WorkflowContext` |
| `io.temporal.internal.worker.WorkflowImplementationFactory` | `io.temporal.core.worker.WorkflowImplementationFactory` |
| `io.temporal.internal.statemachines.ExecuteActivityParameters` | `io.temporal.core.statemachines.ExecuteActivityParameters` |
| `io.temporal.internal.statemachines.ExecuteLocalActivityParameters` | `io.temporal.core.statemachines.ExecuteLocalActivityParameters` |
| `io.temporal.internal.statemachines.StartChildWorkflowExecutionParameters` | `io.temporal.core.statemachines.StartChildWorkflowExecutionParameters` |
| `io.temporal.internal.statemachines.LocalActivityCallback` | `io.temporal.core.statemachines.LocalActivityCallback` |
| `io.temporal.internal.statemachines.UpdateProtocolCallback` | `io.temporal.core.statemachines.UpdateProtocolCallback` |

**Steps**:
1. Copy each class to core
2. Update internal references
3. Ensure no Java SDK public type references
4. Keep callback-based signatures (no Promise/suspend)

**Validation**: Replay tests pass in core module

---

### Task 1.8: Create CoreActivityContext Interface

**Target location**: `io.temporal.core.activity.CoreActivityContext`

```java
package io.temporal.core.activity;

public interface CoreActivityContext {
    // Identity
    byte[] getTaskToken();
    String getWorkflowId();
    String getRunId();
    String getActivityId();
    String getActivityType();

    // Timing
    long getScheduledTimestamp();
    long getStartedTimestamp();
    long getCurrentAttemptScheduledTimestamp();
    Duration getScheduleToCloseTimeout();
    Duration getStartToCloseTimeout();
    Duration getHeartbeatTimeout();

    // Context
    String getWorkflowType();
    String getWorkflowNamespace();
    String getActivityNamespace();
    String getActivityTaskQueue();
    int getAttempt();
    boolean isLocal();

    // Heartbeat (Payloads-based)
    void heartbeat(Optional<Payloads> details);
    Optional<Payloads> getHeartbeatDetails();

    // Async completion
    void doNotCompleteOnReturn();
    boolean isDoNotCompleteOnReturn();

    // Metrics
    Scope getMetricsScope();
}
```

**Validation**: Interface compiles with only protobuf/primitive types

---

### Task 1.9: Create CoreWorkflowInfo Interface

**Target location**: `io.temporal.core.workflow.CoreWorkflowInfo`

```java
package io.temporal.core.workflow;

public interface CoreWorkflowInfo {
    String getNamespace();
    String getWorkflowId();
    String getWorkflowType();
    String getRunId();
    String getFirstExecutionRunId();
    Optional<String> getContinuedExecutionRunId();
    String getOriginalExecutionRunId();
    String getTaskQueue();
    Duration getWorkflowRunTimeout();
    Duration getWorkflowExecutionTimeout();
    Duration getWorkflowTaskTimeout();
    long getRunStartedTimestampMillis();
    Optional<String> getParentWorkflowId();
    Optional<String> getParentRunId();
    Optional<String> getRootWorkflowId();
    Optional<String> getRootRunId();
    int getAttempt();
    String getCronSchedule();
    long getHistoryLength();
    long getHistorySize();
    boolean isContinueAsNewSuggested();
    Optional<String> getCurrentBuildId();
}
```

**Validation**: Interface compiles with only primitive types

---

### Task 1.10: Implement WorkerFactoryInternal and WorkerInternal

**Steps**:
1. Create `WorkerFactoryInternalImpl` that wraps existing worker infrastructure
2. Create `WorkerInternalImpl` that wraps existing worker
3. Extract common worker logic that doesn't depend on SDK types
4. Keep workflow/activity execution using `WorkflowImplementationFactory`

**Validation**:
- Can create worker factory from `GenericWorkflowClient`
- Can register workflow implementations via factory
- Can start/stop workers

---

### Phase 1 Completion Criteria

- [ ] `temporal-core` module compiles successfully
- [ ] All lint checks pass for `temporal-core`
- [ ] ALL unit tests for moved classes are migrated and pass (zero tests deleted/disabled)
- [ ] All moved classes have no imports from `io.temporal.client.*`, `io.temporal.workflow.*`, `io.temporal.activity.*`
- [ ] No circular dependencies between core and SDK
- [ ] `./gradlew :temporal-core:build :temporal-core:test` succeeds
- [ ] Test count verification: all original tests accounted for

---

## Phase 2: Extract Core Testing Module

**Goal**: Create `temporal-core-testing` with shared test infrastructure.

### Task 2.1: Create Module Structure

```
temporal-core-testing/
├── build.gradle.kts
├── src/main/java/io/temporal/core/testing/
│   ├── TestEnvironmentInternal.java
│   ├── TestEnvironmentOptionsInternal.java
│   └── internal/
└── src/test/java/
```

**Dependencies**:
- `temporal-core`
- `temporal-test-server` (in-memory server)

---

### Task 2.2: Extract TestEnvironmentInternal

```java
package io.temporal.core.testing;

public interface TestEnvironmentInternal extends AutoCloseable {
    WorkflowServiceStubs getWorkflowServiceStubs();
    String getNamespace();
    void start();
    void shutdown();

    // Time control
    void sleep(Duration duration);
    void setCurrentTime(Instant time);
    boolean isTimeSkippingEnabled();
}
```

**Steps**:
1. Create interface for test environment
2. Implement using existing `TestWorkflowEnvironment` internals
3. Extract time-skipping logic
4. Remove SDK-specific type dependencies

**Validation**: Can create test environment and run basic workflow

---

### Phase 2 Completion Criteria

- [ ] `temporal-core-testing` module compiles successfully
- [ ] All lint checks pass
- [ ] ALL unit tests for testing infrastructure migrated and pass (zero tests deleted/disabled)
- [ ] Can create test environment without SDK dependency
- [ ] Time skipping works
- [ ] `./gradlew :temporal-core-testing:build :temporal-core-testing:test` succeeds

---

## Phase 3: Update Java SDK

**Goal**: Refactor Java SDK to use `temporal-core`.

### Task 3.1: Add Core Dependency

Update `temporal-sdk/build.gradle.kts`:
```kotlin
dependencies {
    api(project(":temporal-core"))
    // ... existing dependencies
}
```

---

### Task 3.2: Create Deprecated Aliases

For backward compatibility, create aliases in old packages:

```java
package io.temporal.internal.client.external;

/**
 * @deprecated Use {@link io.temporal.core.client.GenericWorkflowClient}
 */
@Deprecated
public interface GenericWorkflowClient
    extends io.temporal.core.client.GenericWorkflowClient {
}
```

---

### Task 3.3: Refactor WorkflowClient to Use Core

**Steps**:
1. Update `WorkflowClientInternalImpl` to use `GenericWorkflowClient` from core
2. Add conversion methods for SDK options → protobuf requests
3. Keep all public API signatures unchanged

```java
package io.temporal.internal.client;

final class WorkflowOptionsProtoConverter {
    static StartWorkflowExecutionRequest.Builder toStartRequest(
            WorkflowOptions options,
            WorkflowClientOptions clientOptions,
            String workflowId,
            String workflowType,
            Optional<Payloads> input) {
        // Convert SDK options to protobuf request
    }
}
```

**Validation**: All existing `WorkflowClient` tests pass

---

### Task 3.4: Refactor Worker/WorkerFactory to Use Core

**Steps**:
1. Update `Worker` to wrap `WorkerInternal`
2. Update `WorkerFactory` to wrap `WorkerFactoryInternal`
3. Add conversion: `WorkerOptions` → `WorkerOptionsInternal`
4. Keep all public API signatures unchanged

```java
package io.temporal.worker;

public final class Worker {
    private final WorkerInternal internal;
    private final List<WorkerInterceptor> interceptors;

    // Public API unchanged
    public void registerWorkflowImplementationTypes(Class<?>... workflowTypes) {
        // Create WorkflowImplementationFactory and register with internal
    }
}
```

**Validation**: All existing `Worker` tests pass

---

### Task 3.5: Refactor ActivityExecutionContext

**Steps**:
1. Make `ActivityExecutionContext` extend `CoreActivityContext`
2. Add SDK-specific methods (typed heartbeat, WorkflowClient access)
3. Implementation delegates to `CoreActivityContext` for primitives

**Validation**: All activity tests pass

---

### Task 3.6: Update temporal-testing

Update `temporal-testing/build.gradle.kts`:
```kotlin
dependencies {
    api(project(":temporal-sdk"))
    api(project(":temporal-core-testing"))
}
```

Refactor `TestWorkflowEnvironment` to use `TestEnvironmentInternal`.

**Validation**: All testing module tests pass

---

### Phase 3 Completion Criteria

- [ ] Java SDK compiles successfully with `temporal-core` dependency
- [ ] All lint checks pass
- [ ] ALL existing Java SDK tests pass (zero tests deleted/disabled)
- [ ] All existing public APIs unchanged
- [ ] No breaking changes for users
- [ ] `./gradlew :temporal-sdk:build :temporal-sdk:test :temporal-testing:build :temporal-testing:test` succeeds
- [ ] Test count >= original test count

---

## Phase 4: Create temporal-kotlin-sdk-alpha Module

**Goal**: Create independent Kotlin SDK with no Java SDK dependency.

### Task 4.1: Create Module Structure

```
temporal-kotlin-sdk-alpha/
├── build.gradle.kts
├── src/main/kotlin/io/temporal/kotlinsdk/
│   ├── client/
│   │   ├── KClient.kt
│   │   ├── KClientOptions.kt
│   │   ├── KWorkflowHandle.kt
│   │   ├── KWorkflowOptions.kt
│   │   └── schedules/
│   ├── worker/
│   │   ├── KWorker.kt
│   │   ├── KWorkerFactory.kt
│   │   └── KWorkerOptions.kt
│   ├── workflow/
│   │   ├── KWorkflow.kt
│   │   ├── KWorkflowInfo.kt
│   │   ├── annotations.kt
│   │   └── KChildWorkflowOptions.kt
│   ├── activity/
│   │   ├── KActivityContext.kt
│   │   ├── KActivityOptions.kt
│   │   └── annotations.kt
│   ├── common/
│   │   ├── KRetryOptions.kt
│   │   └── KSearchAttributes.kt
│   ├── interceptor/
│   │   └── ... (existing Kotlin interceptors)
│   └── internal/
│       ├── converters/
│       ├── workflow/
│       └── activity/
└── src/test/kotlin/
```

**Dependencies** (build.gradle.kts):
```kotlin
dependencies {
    api(project(":temporal-core"))

    // Kotlin
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // NO dependency on temporal-sdk or temporal-kotlin!
}
```

---

### Task 4.2: Implement Kotlin Annotations

**File**: `src/main/kotlin/io/temporal/kotlinsdk/workflow/annotations.kt`

```kotlin
package io.temporal.kotlinsdk.workflow

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KWorkflowInterface

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KWorkflowMethod(val name: String = "")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KSignalMethod(val name: String = "")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KQueryMethod(val name: String = "")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KUpdateMethod(val name: String = "")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KUpdateValidatorMethod(val updateName: String)
```

**File**: `src/main/kotlin/io/temporal/kotlinsdk/activity/annotations.kt`

```kotlin
package io.temporal.kotlinsdk.activity

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KActivityInterface(val namePrefix: String = "")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KActivityMethod(val name: String = "")
```

**Validation**: Annotations compile

---

### Task 4.3: Implement Annotation Metadata Extraction

**File**: `src/main/kotlin/io/temporal/kotlinsdk/internal/metadata/KWorkflowMetadata.kt`

```kotlin
package io.temporal.kotlinsdk.internal.metadata

internal object KWorkflowMetadata {
    fun getWorkflowType(workflowInterface: KClass<*>): String
    fun getSignalMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>>
    fun getQueryMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>>
    fun getUpdateMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>>
    fun getUpdateValidators(workflowInterface: KClass<*>): Map<String, KFunction<*>>
}

internal object KActivityMetadata {
    fun getActivityType(activityInterface: KClass<*>, method: KFunction<*>): String
    fun getActivityMethods(activityInterface: KClass<*>): List<KFunction<*>>
}
```

**Validation**: Unit tests for metadata extraction pass

---

### Task 4.4: Implement Kotlin Options Classes

Implement all options as Kotlin data classes with DSL builders:

```kotlin
// KWorkflowOptions.kt
data class KWorkflowOptions(
    val taskQueue: String,
    val workflowId: String = UUID.randomUUID().toString(),
    val workflowExecutionTimeout: Duration? = null,
    val workflowRunTimeout: Duration? = null,
    val workflowTaskTimeout: Duration? = null,
    val retryOptions: KRetryOptions? = null,
    val memo: Map<String, Any?>? = null,
    val searchAttributes: Map<String, Any?>? = null,
    // ... all options
)

// KActivityOptions.kt
data class KActivityOptions(
    val taskQueue: String? = null,
    val scheduleToCloseTimeout: Duration? = null,
    val startToCloseTimeout: Duration? = null,
    val scheduleToStartTimeout: Duration? = null,
    val heartbeatTimeout: Duration? = null,
    val retryOptions: KRetryOptions? = null,
    val cancellationType: KActivityCancellationType = KActivityCancellationType.TRY_CANCEL,
    // ... all options
)

// KRetryOptions.kt
data class KRetryOptions(
    val initialInterval: Duration? = null,
    val maximumInterval: Duration? = null,
    val backoffCoefficient: Double? = null,
    val maximumAttempts: Int? = null,
    val nonRetryableErrorTypes: List<String>? = null
)
```

---

### Task 4.5: Implement Proto Converters

**File**: `src/main/kotlin/io/temporal/kotlinsdk/internal/converters/KProtoConverters.kt`

```kotlin
package io.temporal.kotlinsdk.internal.converters

internal object KProtoConverters {
    fun toStartRequest(
        options: KWorkflowOptions,
        clientOptions: KClientOptions,
        workflowType: String,
        input: Payloads?
    ): StartWorkflowExecutionRequest

    fun toSignalRequest(
        execution: KWorkflowExecution,
        clientOptions: KClientOptions,
        signalName: String,
        input: Payloads?
    ): SignalWorkflowExecutionRequest

    fun toRetryPolicy(options: KRetryOptions): RetryPolicy

    // ... all other conversions
}

internal object KScheduleProtoConverters {
    fun toScheduleProto(schedule: KSchedule): Schedule
    fun fromScheduleProto(proto: Schedule): KSchedule
    // ... all schedule conversions
}

internal object KActivityParameterConverters {
    fun toExecuteActivityParameters(
        activityType: String,
        options: KActivityOptions,
        input: Payloads?
    ): ExecuteActivityParameters

    fun toExecuteLocalActivityParameters(
        activityType: String,
        options: KLocalActivityOptions,
        input: Payloads?
    ): ExecuteLocalActivityParameters
}
```

**Validation**: Unit tests for all converters pass

---

### Task 4.6: Implement KClient

```kotlin
package io.temporal.kotlinsdk.client

class KClient private constructor(
    private val genericClient: GenericWorkflowClient,
    private val dataConverter: DataConverter,
    val options: KClientOptions
) {
    companion object {
        fun newInstance(
            serviceStubs: WorkflowServiceStubs,
            options: KClientOptions = KClientOptions()
        ): KClient
    }

    suspend fun <R> startWorkflow(
        workflowType: String,
        options: KWorkflowOptions,
        vararg args: Any?
    ): KWorkflowHandle<R>

    inline fun <reified W, R> startWorkflow(
        options: KWorkflowOptions,
        noinline workflowMethod: W.() -> R
    ): KWorkflowHandle<R>

    suspend fun signalWorkflow(
        execution: KWorkflowExecution,
        signalName: String,
        vararg args: Any?
    )

    suspend fun <R> queryWorkflow(
        execution: KWorkflowExecution,
        queryType: String,
        resultClass: KClass<R>,
        vararg args: Any?
    ): R

    // ... all other operations
}
```

**Validation**: Integration tests with test server pass

---

### Task 4.7: Implement KWorkerFactory and KWorker

```kotlin
package io.temporal.kotlinsdk.worker

class KWorkerFactory private constructor(
    private val internal: WorkerFactoryInternal,
    private val interceptors: List<KWorkerInterceptor>,
    private val dataConverter: DataConverter
) {
    companion object {
        fun newInstance(
            client: KClient,
            options: KWorkerFactoryOptions = KWorkerFactoryOptions()
        ): KWorkerFactory
    }

    fun newWorker(
        taskQueue: String,
        options: KWorkerOptions = KWorkerOptions()
    ): KWorker

    fun start()
    fun shutdown()
    suspend fun awaitTermination(timeout: Duration): Boolean
}

class KWorker internal constructor(
    private val internal: WorkerInternal,
    private val interceptors: List<KWorkerInterceptor>,
    private val dataConverter: DataConverter
) {
    inline fun <reified W : Any> registerWorkflowImplementation()

    fun registerWorkflowImplementation(
        workflowInterface: KClass<*>,
        factory: () -> Any
    )

    fun registerActivitiesImplementations(vararg activities: Any)

    fun registerDynamicWorkflow(factory: () -> KDynamicWorkflow)

    fun registerDynamicActivity(activity: KDynamicActivity)
}
```

---

### Task 4.8: Implement KotlinWorkflowContext (Suspend-based)

Port existing `KotlinWorkflowContext` but use only core types:

```kotlin
package io.temporal.kotlinsdk.internal.workflow

internal class KotlinWorkflowContext(
    internal val replayContext: ReplayWorkflowContext,  // From core
    internal val dataConverter: DataConverter           // From core
) {
    suspend fun <R> executeActivity(
        activityType: String,
        options: KActivityOptions,
        resultClass: KClass<R>,
        vararg args: Any?
    ): R {
        val parameters = KActivityParameterConverters.toExecuteActivityParameters(
            activityType, options, dataConverter.toPayloads(*args).orElse(null)
        )
        return suspendCancellableCoroutine { cont ->
            replayContext.scheduleActivityTask(parameters) { result, failure ->
                if (failure != null) {
                    cont.resumeWithException(dataConverter.failureToException(failure))
                } else {
                    cont.resume(dataConverter.fromPayloads(0, result, resultClass.java, resultClass.java))
                }
            }
        }
    }

    // ... all other operations
}
```

---

### Task 4.9: Implement KActivityContext

```kotlin
package io.temporal.kotlinsdk.activity

interface KActivityContext {
    val info: KActivityInfo
    fun heartbeat(details: Any? = null)
    fun <T : Any> heartbeatDetails(detailsClass: KClass<T>): T?
    val taskToken: ByteArray
    fun doNotCompleteOnReturn()
    val isDoNotCompleteOnReturn: Boolean
}

internal class KActivityContextImpl(
    private val coreContext: CoreActivityContext,
    private val dataConverter: DataConverter
) : KActivityContext {
    override val info: KActivityInfo = KActivityInfoImpl(coreContext)

    override fun heartbeat(details: Any?) {
        coreContext.heartbeat(dataConverter.toPayloads(details))
    }

    override fun <T : Any> heartbeatDetails(detailsClass: KClass<T>): T? {
        return coreContext.heartbeatDetails.map { payloads ->
            dataConverter.fromPayloads(0, Optional.of(payloads), detailsClass.java, detailsClass.java)
        }.orElse(null)
    }
}
```

---

### Task 4.10: Verify No Java SDK Imports

**Validation script**:
```bash
#!/bin/bash
# Check for forbidden imports in temporal-kotlin-sdk-alpha

FORBIDDEN_PATTERNS=(
    "import io.temporal.client\."
    "import io.temporal.workflow\."
    "import io.temporal.activity\."
    "import io.temporal.worker\."
    "import io.temporal.common\."
    "import io.temporal.kotlin\."  # existing temporal-kotlin module
)

for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    if grep -r "$pattern" temporal-kotlin-sdk-alpha/src/main/kotlin/; then
        echo "ERROR: Found forbidden import pattern: $pattern"
        exit 1
    fi
done

echo "OK: No forbidden imports found"
```

---

### Phase 4 Completion Criteria

- [ ] `temporal-kotlin-sdk-alpha` module compiles successfully
- [ ] All lint checks pass (ktlint)
- [ ] NO imports from `io.temporal.client.*`, `io.temporal.workflow.*`, etc.
- [ ] NO dependency on `temporal-sdk` or `temporal-kotlin`
- [ ] All Kotlin annotations defined and working
- [ ] KClient can start/signal/query workflows
- [ ] KWorker can execute workflows and activities
- [ ] ALL tests for equivalent functionality written and pass
- [ ] `./gradlew :temporal-kotlin-sdk-alpha:build :temporal-kotlin-sdk-alpha:test` succeeds
- [ ] Import checker script passes (no forbidden imports)

---

## Phase 5: Create temporal-kotlin-testing-alpha Module

### Task 5.1: Create Module Structure

```
temporal-kotlin-testing-alpha/
├── build.gradle.kts
├── src/main/kotlin/io/temporal/kotlinsdk/testing/
│   ├── KTestWorkflowEnvironment.kt
│   ├── KTestWorkflowEnvironmentOptions.kt
│   └── KTestWorkflowExtension.kt
└── src/test/kotlin/
```

**Dependencies**:
```kotlin
dependencies {
    api(project(":temporal-kotlin-sdk-alpha"))
    api(project(":temporal-core-testing"))

    compileOnly("org.junit.jupiter:junit-jupiter-api:5.10.0")
}
```

---

### Task 5.2: Implement KTestWorkflowEnvironment

```kotlin
package io.temporal.kotlinsdk.testing

class KTestWorkflowEnvironment private constructor(
    private val internal: TestEnvironmentInternal,
    private val options: KTestWorkflowEnvironmentOptions
) : AutoCloseable {

    companion object {
        fun newInstance(
            options: KTestWorkflowEnvironmentOptions = KTestWorkflowEnvironmentOptions()
        ): KTestWorkflowEnvironment
    }

    fun newClient(options: KClientOptions = KClientOptions()): KClient

    fun newWorkerFactory(
        client: KClient,
        options: KWorkerFactoryOptions = KWorkerFactoryOptions()
    ): KWorkerFactory

    fun newWorker(
        taskQueue: String,
        options: KWorkerOptions = KWorkerOptions()
    ): KWorker

    fun sleep(duration: Duration)

    fun start()

    override fun close()
}
```

---

### Task 5.3: Implement KTestWorkflowExtension (JUnit 5)

```kotlin
package io.temporal.kotlinsdk.testing

import org.junit.jupiter.api.extension.*

class KTestWorkflowExtension(
    private val options: KTestWorkflowEnvironmentOptions = KTestWorkflowEnvironmentOptions()
) : BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    lateinit var environment: KTestWorkflowEnvironment
        private set

    override fun beforeAll(context: ExtensionContext) {
        environment = KTestWorkflowEnvironment.newInstance(options)
    }

    override fun afterAll(context: ExtensionContext) {
        environment.close()
    }

    fun newWorker(taskQueue: String, options: KWorkerOptions = KWorkerOptions()): KWorker
    fun newClient(): KClient
}
```

---

### Phase 5 Completion Criteria

- [ ] `temporal-kotlin-testing-alpha` module compiles successfully
- [ ] All lint checks pass (ktlint)
- [ ] ALL unit tests pass (zero tests deleted/disabled)
- [ ] Can run workflow tests with time skipping
- [ ] JUnit 5 extension works
- [ ] `./gradlew :temporal-kotlin-testing-alpha:build :temporal-kotlin-testing-alpha:test` succeeds

---

## Phase 6: Keep Existing temporal-kotlin

**Goal**: Ensure `temporal-kotlin` continues to work unchanged.

### Task 6.1: Verify No Changes Needed

- [ ] `temporal-kotlin` still builds
- [ ] All existing tests pass
- [ ] No breaking changes for users

### Task 6.2: Update Documentation

Add documentation explaining the two Kotlin options:
- `temporal-kotlin` for Java SDK users who want Kotlin conveniences
- `temporal-kotlin-sdk-alpha` for pure Kotlin SDK experience

---

## Phase 7: Final Validation and Documentation

### Task 7.1: Run Full Test Suite

```bash
./gradlew clean build test integrationTest
```

All modules must pass:
- temporal-core
- temporal-core-testing
- temporal-sdk
- temporal-testing
- temporal-kotlin
- temporal-kotlin-sdk-alpha
- temporal-kotlin-testing-alpha

**Test Count Verification**:
```bash
# Record original test count before refactoring
./gradlew test --info 2>&1 | grep -E "tests found|tests executed" > original_test_count.txt

# After refactoring, verify count hasn't decreased
./gradlew test --info 2>&1 | grep -E "tests found|tests executed" > new_test_count.txt

# Compare - new count must be >= original
diff original_test_count.txt new_test_count.txt
```

**REMINDER**: Zero tests may be deleted or disabled. Every original test must either:
1. Pass in its original location, OR
2. Be migrated to a new module and pass there

---

### Task 7.2: Verify Independence

Run the forbidden import check on `temporal-kotlin-sdk-alpha`:
- No `io.temporal.client.*`
- No `io.temporal.workflow.*`
- No `io.temporal.activity.*`
- No `io.temporal.worker.*`
- No `io.temporal.kotlin.*`

---

### Task 7.3: Create Migration Guide

Document:
1. How to migrate from `temporal-kotlin` to `temporal-kotlin-sdk-alpha`
2. Annotation mapping (`@WorkflowInterface` → `@KWorkflowInterface`)
3. Type mapping (`WorkflowOptions` → `KWorkflowOptions`)
4. API differences

---

### Task 7.4: Update README and Documentation

- Update main README with new module structure
- Add Kotlin SDK quickstart guide
- Document when to use `temporal-kotlin` vs `temporal-kotlin-sdk-alpha`

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking Java SDK compatibility | Keep all public APIs unchanged; add deprecation notices only |
| Performance regression | Benchmark critical paths before/after |
| Missing functionality in core | Comprehensive feature matrix checklist |
| Circular dependencies | CI check for dependency cycles |
| Test coverage gaps | Track coverage metrics per module |

---

## Success Metrics

1. **Build time**: Each module builds in < 30 seconds
2. **Test coverage**: > 80% for new code
3. **Zero breaking changes**: All existing tests pass
4. **Independence verified**: Import checker passes
5. **Documentation complete**: All new APIs documented

---

## Appendix: Module Build Order

```
1. temporal-serviceclient (no changes)
2. temporal-core (new)
3. temporal-core-testing (new)
4. temporal-sdk (refactored)
5. temporal-testing (refactored)
6. temporal-kotlin (unchanged)
7. temporal-kotlin-sdk-alpha (new)
8. temporal-kotlin-testing-alpha (new)
```
