# Temporal Core Module with Protobuf API

## Overview

This proposal describes a refactoring to create a minimal `temporal-core` module that communicates exclusively via Protocol Buffers. This enables SDK-specific modules (Java, Kotlin, future SDKs) to define their own idiomatic APIs without sharing option classes.

## Goals

1. **Minimal shared core** - Only gRPC operations, retry logic, and essential utilities
2. **Protobuf-only interface** - Core accepts/returns protobuf types exclusively
3. **SDK independence** - Each SDK defines its own options, handles, and API patterns
4. **No shared Options classes** - Eliminates coupling between SDK APIs
5. **Kotlin SDK independence** - `temporal-kotlin-sdk` has no dependency on Java public API or `temporal-kotlin`

## Module Naming Strategy

| Module | Purpose | Dependencies |
|--------|---------|--------------|
| `temporal-kotlin` | **Existing** Kotlin extensions for Java SDK (DSL builders, coroutine adapters) | `temporal-sdk` |
| `temporal-kotlin-sdk` | **New** independent Kotlin SDK with idiomatic API | `temporal-core` only |
| `temporal-kotlin-testing` | Test utilities for `temporal-kotlin-sdk` | `temporal-kotlin-sdk`, `temporal-core-testing` |

**Key principle**: `temporal-kotlin-sdk` does NOT depend on `temporal-kotlin`. They are independent modules:
- Users who want Java SDK with Kotlin conveniences use `temporal-kotlin`
- Users who want a pure Kotlin SDK use `temporal-kotlin-sdk`
- Users can use both if they need interop during migration

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      temporal-serviceclient                      │
│              (gRPC stubs, protobuf types, connection)           │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        temporal-core                             │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ GenericWorkflowClient (protobuf in/out, retry, metrics)     ││
│  │ GenericScheduleClient (schedule operations)                  ││
│  │ WorkerFactoryInternal, WorkerInternal, WorkerOptionsInternal││
│  │ DataConverter interface                                      ││
│  │ GrpcRetryer, LongPollHelper                                 ││
│  │ ProtobufTimeUtils, SearchAttributesUtil                     ││
│  │ CoreActivityContext, CoreWorkflowInfo                       ││
│  │ WorkflowImplementationFactory, ReplayWorkflow (internals)   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
         │                           │                      │
         ▼                           ▼                      ▼
┌────────────────────┐  ┌──────────────────────┐  ┌────────────────────┐
│   temporal-sdk     │  │ temporal-kotlin-sdk  │  │temporal-core-testing│
│ ┌────────────────┐ │  │ ┌──────────────────┐ │  │ ┌────────────────┐ │
│ │WorkflowClient  │ │  │ │KClient           │ │  │ │TestEnvironment │ │
│ │WorkflowStub    │ │  │ │KWorkflowHandle   │ │  │ │Internal        │ │
│ │Worker/Factory  │ │  │ │KWorker/Factory   │ │  │ │(in-memory srv) │ │
│ │@WorkflowMethod │ │  │ │@KWorkflowMethod  │ │  │ └────────────────┘ │
│ │Interceptors    │ │  │ │KInterceptors     │ │  └────────────────────┘
│ └────────────────┘ │  │ └──────────────────┘ │           │
│(stub-based, Java)  │  │(suspend, pure Kotlin)│           │
└────────────────────┘  └──────────────────────┘           │
         │                       │                         │
         ▼                       ▼                         │
┌────────────────────┐  ┌────────────────────────┐         │
│  temporal-kotlin   │  │temporal-kotlin-testing │◄────────┘
│ ┌────────────────┐ │  │ ┌────────────────────┐ │
│ │DSL Builders    │ │  │ │KTestWorkflow       │ │
│ │Coroutine ext.  │ │  │ │Environment         │ │
│ │Java SDK helpers│ │  │ │JUnit5 Extension    │ │
│ └────────────────┘ │  │ └────────────────────┘ │
│(extensions on Java)│  └────────────────────────┘
└────────────────────┘
         │
         ▼
┌────────────────────┐
│ temporal-testing   │
│ ┌────────────────┐ │
│ │TestWorkflow    │ │
│ │Environment     │ │
│ │JUnit Rule      │ │
│ └────────────────┘ │
└────────────────────┘
```

## What Lives in temporal-core

### 1. Client Operations (GenericWorkflowClient)

Protobuf-in, protobuf-out interface for all workflow operations:

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

    // Workflow management
    void requestCancel(RequestCancelWorkflowExecutionRequest request);
    void terminate(TerminateWorkflowExecutionRequest request);
    DescribeWorkflowExecutionResponse describeWorkflowExecution(DescribeWorkflowExecutionRequest request);

    // History
    GetWorkflowExecutionHistoryResponse getWorkflowExecutionHistory(
        GetWorkflowExecutionHistoryRequest request);
    GetWorkflowExecutionHistoryResponse longPollHistory(
        GetWorkflowExecutionHistoryRequest request, Deadline deadline);
    CompletableFuture<GetWorkflowExecutionHistoryResponse> longPollHistoryAsync(
        GetWorkflowExecutionHistoryRequest request, Deadline deadline);

    // Listing
    ListWorkflowExecutionsResponse listWorkflowExecutions(ListWorkflowExecutionsRequest request);
    CompletableFuture<ListWorkflowExecutionsResponse> listWorkflowExecutionsAsync(
        ListWorkflowExecutionsRequest request);
    CountWorkflowExecutionsResponse countWorkflowExecutions(CountWorkflowExecutionsRequest request);

    // Multi-operation
    ExecuteMultiOperationResponse executeMultiOperation(
        ExecuteMultiOperationRequest request, Deadline deadline);

    // Schedules
    CreateScheduleResponse createSchedule(CreateScheduleRequest request);
    DescribeScheduleResponse describeSchedule(DescribeScheduleRequest request);
    UpdateScheduleResponse updateSchedule(UpdateScheduleRequest request);
    PatchScheduleResponse patchSchedule(PatchScheduleRequest request);
    DeleteScheduleResponse deleteSchedule(DeleteScheduleRequest request);
    CompletableFuture<ListSchedulesResponse> listSchedulesAsync(ListSchedulesRequest request);

    // Worker versioning
    UpdateWorkerBuildIdCompatibilityResponse updateWorkerBuildIdCompatibility(
        UpdateWorkerBuildIdCompatibilityRequest request);
    GetWorkerBuildIdCompatibilityResponse getWorkerBuildIdCompatibility(
        GetWorkerBuildIdCompatibilityRequest request);
    GetWorkerTaskReachabilityResponse getWorkerTaskReachability(
        GetWorkerTaskReachabilityRequest request);
}
```

### 2. Infrastructure Classes

```java
package io.temporal.core.client;

// Retry logic with server capabilities awareness
public class GrpcRetryer { ... }

// Long-poll helpers for result fetching
public class WorkflowResultPoller {
    public Optional<Payloads> pollForResult(
        GenericWorkflowClient client,
        WorkflowExecution execution,
        Duration timeout);

    public CompletableFuture<Optional<Payloads>> pollForResultAsync(
        GenericWorkflowClient client,
        WorkflowExecution execution,
        Duration timeout);
}
```

### 3. Data Conversion

```java
package io.temporal.core.converter;

// Core interface - implementations in each SDK
public interface DataConverter {
    Optional<Payloads> toPayloads(Object... values);
    <T> T fromPayloads(int index, Optional<Payloads> content, Class<T> type, Type genericType);
    Failure exceptionToFailure(Throwable e);
    RuntimeException failureToException(Failure failure);
}

// Default implementation usable by any SDK
public class DefaultDataConverter implements DataConverter { ... }
```

### 4. Utility Classes

```java
package io.temporal.core.common;

public class ProtobufTimeUtils {
    public static Duration toProtoDuration(java.time.Duration duration);
    public static java.time.Duration toJavaDuration(Duration protoDuration);
    public static Timestamp toProtoTimestamp(Instant instant);
    public static Instant toJavaInstant(Timestamp protoTimestamp);
}

public class SearchAttributesUtil {
    public static SearchAttributes encode(Map<String, Object> attributes);
    public static SearchAttributes encodeTyped(SearchAttributes typedAttributes);
}

public class RetryPolicyConverter {
    public static RetryPolicy toProto(/* core retry options */);
}
```

### 5. Worker Internals (for workflow/activity execution)

```java
package io.temporal.core.worker;

// Factory for creating workflow instances during replay
public interface WorkflowImplementationFactory {
    ReplayWorkflow createReplayWorkflow(WorkflowContext context);
}

// Workflow replay context
public interface ReplayWorkflow { ... }
public interface ReplayWorkflowContext { ... }

// State machine parameters (for activity/child workflow scheduling)
public class ExecuteActivityParameters { ... }
public class ExecuteLocalActivityParameters { ... }
public class StartChildWorkflowExecutionParameters { ... }
```

## What Lives in Each SDK

### Java SDK (temporal-sdk)

```java
package io.temporal.client;

// Public API - pure data class, no conversion logic
public class WorkflowOptions {
    private String taskQueue;
    private Duration workflowExecutionTimeout;
    private Duration workflowRunTimeout;
    private RetryOptions retryOptions;
    // ... all options, getters, builder
}

// Stateful stub pattern
public interface WorkflowClient {
    <T> T newWorkflowStub(Class<T> workflowInterface, WorkflowOptions options);
    WorkflowStub newUntypedWorkflowStub(String workflowType, WorkflowOptions options);
    // ...
}

public interface WorkflowStub {
    void signal(String signalName, Object... args);
    <R> R query(String queryType, Class<R> resultClass, Object... args);
    // ...
}
```

```java
package io.temporal.internal.client;

// Internal converter - keeps options classes clean
final class WorkflowOptionsProtoConverter {

    static StartWorkflowExecutionRequest.Builder toStartRequest(
            WorkflowOptions options,
            WorkflowClientOptions clientOptions,
            String workflowId,
            String workflowType,
            Payloads input) {
        StartWorkflowExecutionRequest.Builder request = StartWorkflowExecutionRequest.newBuilder()
            .setNamespace(clientOptions.getNamespace())
            .setIdentity(clientOptions.getIdentity())
            .setRequestId(UUID.randomUUID().toString())
            .setWorkflowId(workflowId)
            .setWorkflowType(WorkflowType.newBuilder().setName(workflowType))
            .setTaskQueue(TaskQueue.newBuilder().setName(options.getTaskQueue()));

        if (options.getWorkflowRunTimeout() != null) {
            request.setWorkflowRunTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getWorkflowRunTimeout()));
        }
        if (options.getWorkflowExecutionTimeout() != null) {
            request.setWorkflowExecutionTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getWorkflowExecutionTimeout()));
        }
        if (options.getRetryOptions() != null) {
            request.setRetryPolicy(RetryOptionsProtoConverter.toProto(options.getRetryOptions()));
        }
        if (input != null) {
            request.setInput(input);
        }
        // ... all other fields
        return request;
    }

    static SignalWorkflowExecutionRequest toSignalRequest(
            WorkflowExecution execution,
            WorkflowClientOptions clientOptions,
            String signalName,
            Payloads input) {
        // ...
    }

    // ... other conversion methods
}
```

### Kotlin SDK (temporal-kotlin-sdk)

```kotlin
package io.temporal.kotlinsdk.client

// Pure data class - no conversion logic
data class KWorkflowOptions(
    val taskQueue: String,
    val workflowId: String = UUID.randomUUID().toString(),
    val workflowExecutionTimeout: Duration? = null,
    val workflowRunTimeout: Duration? = null,
    val workflowTaskTimeout: Duration? = null,
    val retryOptions: KRetryOptions? = null,
    val memo: Map<String, Any?>? = null,
    val searchAttributes: KSearchAttributes? = null,
    // ... all options
)

// Public API
class KClient(
    private val genericClient: GenericWorkflowClient,
    private val options: KClientOptions
) {
    suspend fun <R> startWorkflow(
        workflowType: String,
        options: KWorkflowOptions,
        vararg args: Any?
    ): KWorkflowHandle<R> {
        val request = KProtoConverters.toStartRequest(
            options = options,
            clientOptions = this.options,
            workflowType = workflowType,
            input = dataConverter.toPayloads(*args)
        )
        val response = genericClient.start(request)
        return KWorkflowHandle(
            execution = KWorkflowExecution(request.workflowId, response.runId),
            client = this
        )
    }
}

// Stateless handle pattern
class KWorkflowHandle<R>(
    val execution: KWorkflowExecution,
    private val client: KClient
) {
    suspend fun signal(signalName: String, vararg args: Any?) {
        client.signalWorkflow(execution, signalName, *args)
    }

    suspend fun <T> query(queryType: String, resultClass: KClass<T>, vararg args: Any?): T {
        return client.queryWorkflow(execution, queryType, resultClass, *args)
    }

    suspend fun result(resultClass: KClass<R>): R {
        return client.getWorkflowResult(execution, resultClass)
    }
}
```

```kotlin
package io.temporal.kotlinsdk.internal.converters

// Internal converter - keeps options classes clean
internal object KProtoConverters {

    fun toStartRequest(
        options: KWorkflowOptions,
        clientOptions: KClientOptions,
        workflowType: String,
        input: Payloads?
    ): StartWorkflowExecutionRequest {
        return StartWorkflowExecutionRequest.newBuilder().apply {
            namespace = clientOptions.namespace
            identity = clientOptions.identity
            requestId = UUID.randomUUID().toString()
            workflowId = options.workflowId
            setWorkflowType(WorkflowType.newBuilder().setName(workflowType))
            setTaskQueue(TaskQueue.newBuilder().setName(options.taskQueue))

            options.workflowExecutionTimeout?.let {
                workflowExecutionTimeout = it.toProtoDuration()
            }
            options.workflowRunTimeout?.let {
                workflowRunTimeout = it.toProtoDuration()
            }
            options.workflowTaskTimeout?.let {
                workflowTaskTimeout = it.toProtoDuration()
            }
            options.retryOptions?.let {
                retryPolicy = toRetryPolicy(it)
            }
            input?.let { setInput(it) }
            // ... all other fields
        }.build()
    }

    fun toSignalRequest(
        execution: KWorkflowExecution,
        clientOptions: KClientOptions,
        signalName: String,
        input: Payloads?
    ): SignalWorkflowExecutionRequest {
        return SignalWorkflowExecutionRequest.newBuilder().apply {
            namespace = clientOptions.namespace
            identity = clientOptions.identity
            requestId = UUID.randomUUID().toString()
            setWorkflowExecution(execution.toProto())
            setSignalName(signalName)
            input?.let { setInput(it) }
        }.build()
    }

    fun toRetryPolicy(options: KRetryOptions): RetryPolicy {
        return RetryPolicy.newBuilder().apply {
            options.initialInterval?.let { initialInterval = it.toProtoDuration() }
            options.maximumInterval?.let { maximumInterval = it.toProtoDuration() }
            options.backoffCoefficient?.let { backoffCoefficient = it }
            options.maximumAttempts?.let { maximumAttempts = it }
            options.nonRetryableErrorTypes?.let { addAllNonRetryableErrorTypes(it) }
        }.build()
    }

    // ... other conversion methods
}
```

## Migration Path

### Phase 1: Extract Core Module

1. Create `temporal-core` module
2. Move `GenericWorkflowClient` interface and implementation
3. Move `GenericScheduleClient` interface and implementation
4. Move `GrpcRetryer`, long-poll helpers
5. Move `DataConverter` interface and default implementation
6. Move utility classes (`ProtobufTimeUtils`, `SearchAttributesUtil`)
7. Move worker internals (`ReplayWorkflow`, `WorkflowImplementationFactory`, etc.)
8. Create `WorkerFactoryInternal`, `WorkerInternal`, and `WorkerOptionsInternal`
9. Create `CoreActivityContext` and `CoreWorkflowInfo` interfaces

### Phase 2: Extract Core Testing Module

1. Create `temporal-core-testing` module
2. Move `TestEnvironmentInternal` (in-memory Temporal server)
3. Move shared test infrastructure that doesn't depend on SDK-specific types

### Phase 3: Update Java SDK

1. Add dependency on `temporal-core`
2. Keep all public API classes (`WorkflowClient`, `WorkflowOptions`, etc.)
3. Refactor `WorkflowClientInternalImpl` to use `GenericWorkflowClient` from core
4. Refactor `Worker` and `WorkerFactory` to wrap `WorkerInternal` and `WorkerFactoryInternal`
5. Keep Java annotations (`@WorkflowInterface`, `@WorkflowMethod`, etc.)
6. Remove duplicated internal classes now in core
7. Update `temporal-testing` to depend on `temporal-core-testing`

### Phase 4: Create New Kotlin SDK Module

1. Create new `temporal-kotlin-sdk` module (separate from existing `temporal-kotlin`)
2. Depend ONLY on `temporal-core` (not `temporal-sdk` or `temporal-kotlin`)
3. Add Kotlin annotations (`@KWorkflowInterface`, `@KWorkflowMethod`, etc.)
4. Create `KWorkflowMetadata` and `KActivityMetadata` for annotation processing
5. Implement all Kotlin types (`KClient`, `KWorker`, `KWorkflowOptions`, etc.)
6. Implement direct proto conversion in Kotlin options classes
7. Use `GenericWorkflowClient` and `GenericScheduleClient` directly
8. Wrap `WorkerFactoryInternal`/`WorkerInternal` from core
9. Implement `KScheduleProtoUtil` for schedule conversions

### Phase 5: Create Kotlin Testing Module

1. Create `temporal-kotlin-testing` module
2. Depend on `temporal-kotlin-sdk` (NOT `temporal-kotlin`)
3. Implement `KTestWorkflowEnvironment`
4. Implement `KTestWorkflowExtension` (JUnit 5)
5. Optionally implement `KTestWorkflowRule` (JUnit 4)

### Phase 6: Keep Existing temporal-kotlin

1. Keep `temporal-kotlin` as-is for users who want Java SDK with Kotlin extensions
2. `temporal-kotlin` continues to depend on `temporal-sdk`
3. No changes required - provides DSL builders and coroutine adapters for Java SDK

### Phase 7: Clean Up and Documentation

1. Verify `temporal-kotlin-sdk` has NO imports from `io.temporal.client`, `io.temporal.workflow`, etc.
2. Verify `temporal-kotlin-sdk` does NOT depend on `temporal-kotlin`
3. Update documentation to explain the two Kotlin module options
4. Provide migration guide for users moving from `temporal-kotlin` to `temporal-kotlin-sdk`

## Module Dependencies

```
temporal-serviceclient (gRPC, protobuf)
         │
         ▼
    temporal-core
    (depends on: temporal-serviceclient)
         │
    ┌────┴─────────┬─────────────────────┐
    ▼              ▼                     ▼
temporal-sdk   temporal-kotlin-sdk   temporal-core-testing
(depends on:   (depends on:          (depends on:
 temporal-core) temporal-core ONLY)   temporal-core)
    │              │                     │
    ▼              ▼                     │
temporal-kotlin temporal-kotlin-testing ◄┘
(depends on:    (depends on:
 temporal-sdk)   temporal-kotlin-sdk,
    │            temporal-core-testing)
    ▼
temporal-testing
(depends on:
 temporal-sdk,
 temporal-core-testing)
```

### Full Dependency Tree

```
temporal-serviceclient
├── temporal-core
│   ├── temporal-sdk (Java public API)
│   │   ├── temporal-kotlin (Kotlin extensions for Java SDK)
│   │   └── temporal-testing (Java test utilities)
│   ├── temporal-kotlin-sdk (Independent Kotlin SDK - NO dependency on temporal-kotlin!)
│   │   └── temporal-kotlin-testing (Kotlin SDK test utilities)
│   └── temporal-core-testing (Shared test infrastructure)
│       ├── temporal-testing
│       └── temporal-kotlin-testing
```

### Independence Guarantee

**Critical**: `temporal-kotlin-sdk` MUST NOT depend on:
- `temporal-sdk` (Java SDK)
- `temporal-kotlin` (Java SDK extensions)
- Any `io.temporal.client.*`, `io.temporal.workflow.*`, `io.temporal.activity.*` packages

This ensures users can use the pure Kotlin SDK without pulling in the Java SDK.

## Code Size Estimates

| Module | Approximate Lines | Contents |
|--------|-------------------|----------|
| temporal-core | ~3,000-4,000 | GenericWorkflowClient, GenericScheduleClient, WorkerFactoryInternal, WorkerInternal, retry, long-poll, data converter, worker internals |
| temporal-core-testing | ~500-800 | TestEnvironmentInternal, in-memory server integration |
| temporal-sdk | ~2,000-3,000 | WorkflowClient, WorkerFactory, Worker, stubs, options, annotations, interceptors |
| temporal-testing | ~800-1,200 | TestWorkflowEnvironment, JUnit rules/extensions |
| temporal-kotlin | ~500-800 | DSL builders, coroutine extensions for Java SDK (existing, unchanged) |
| temporal-kotlin-sdk | ~1,500-2,000 | KClient, KWorkerFactory, KWorker, handles, annotations, interceptors, options → protobuf conversion |
| temporal-kotlin-testing | ~400-600 | KTestWorkflowEnvironment, JUnit 5 extension |

## Benefits

1. **Clean separation** - Core has no SDK-specific concepts
2. **Independent evolution** - Each SDK can change its API without affecting others
3. **No options coupling** - SDKs don't share options classes
4. **Simpler core** - Just gRPC operations and utilities
5. **Future SDK support** - New SDKs only depend on core, not existing SDKs

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Duplicated options → protobuf mapping | Mapping is mechanical and type-safe; SDK-specific anyway |
| Divergent SDK behaviors | Core provides consistent gRPC behavior; SDKs differ only in API |
| Migration complexity | Phased approach; Java SDK maintains backward compatibility |
| Testing burden | Core tests gRPC operations; SDK tests their API layer |

## Workflow and Activity API Analysis

### Current Architecture (Actual Code)

The Kotlin SDK already shares a low-level interface with the Java SDK: **`ReplayWorkflowContext`**.

```
┌─────────────────────────────────────────────────────────────────┐
│              ReplayWorkflowContext (callback-based)              │
│                   io.temporal.internal.replay                    │
│  - scheduleActivityTask(params, callback)                       │
│  - scheduleLocalActivityTask(params, callback)                  │
│  - startChildWorkflow(params, startCb, completionCb)            │
│  - newTimer(duration, metadata, callback)                       │
│  - sideEffect(func, metadata, callback)                         │
│  - getVersion(changeId, min, max, callback)                     │
│  - currentTimeMillis(), newRandom(), randomUUID()               │
│  - upsertSearchAttributes(), upsertMemo()                       │
│  - continueAsNewOnCompletion()                                  │
└─────────────────────────────────────────────────────────────────┘
              │                                    │
              ▼                                    ▼
┌───────────────────────────────┐    ┌───────────────────────────────┐
│     WorkflowInternal          │    │   KotlinWorkflowContext       │
│     (Java SDK internal)       │    │   (Kotlin SDK internal)       │
│  - wraps with Promise<T>      │    │  - wraps with suspend funs    │
│  - CancellationScope          │    │  - Kotlin coroutines          │
│  - WorkflowThread             │    │  - awaitCondition()           │
└───────────────────────────────┘    └───────────────────────────────┘
              │                                    │
              ▼                                    ▼
┌───────────────────────────────┐    ┌───────────────────────────────┐
│        Workflow               │    │      (Kotlin public API)      │
│     (Java public API)         │    │  - suspend fun executeActivity│
│  - newActivityStub()          │    │  - suspend fun startChild()   │
│  - newTimer() -> Promise      │    │  - delay(), awaitCondition()  │
│  - sleep(), await()           │    │                               │
└───────────────────────────────┘    └───────────────────────────────┘
```

### ReplayWorkflowContext - The Existing Shared Interface

This is the **highest-level abstraction currently shared** between both SDKs:

```java
// io.temporal.internal.replay.ReplayWorkflowContext
public interface ReplayWorkflowContext {
    // === Workflow Identity ===
    WorkflowExecution getWorkflowExecution();
    WorkflowType getWorkflowType();
    String getWorkflowId();
    String getRunId();
    String getNamespace();
    String getTaskQueue();

    // === Scheduling Operations (callback-based) ===
    ScheduleActivityTaskOutput scheduleActivityTask(
        ExecuteActivityParameters parameters,
        Functions.Proc2<Optional<Payloads>, Failure> callback);

    Functions.Proc1<Exception> scheduleLocalActivityTask(
        ExecuteLocalActivityParameters parameters,
        LocalActivityCallback callback);

    Functions.Proc1<Exception> startChildWorkflow(
        StartChildWorkflowExecutionParameters parameters,
        Functions.Proc2<WorkflowExecution, Exception> startCallback,
        Functions.Proc2<Optional<Payloads>, Exception> completionCallback);

    Functions.Proc1<RuntimeException> newTimer(
        Duration duration,
        UserMetadata userMetadata,
        Functions.Proc1<RuntimeException> callback);

    // === Side Effects and Versioning ===
    void sideEffect(
        Func<Optional<Payloads>> func,
        UserMetadata userMetadata,
        Functions.Proc1<Optional<Payloads>> callback);

    void mutableSideEffect(
        String id,
        UserMetadata userMetadata,
        Func1<Optional<Payloads>, Optional<Payloads>> func,
        Functions.Proc1<Optional<Payloads>> callback);

    void getVersion(
        String changeId,
        int minSupported,
        int maxSupported,
        Functions.Proc2<Integer, RuntimeException> callback);

    // === Deterministic Operations ===
    long currentTimeMillis();
    Random newRandom();
    UUID randomUUID();

    // === State ===
    boolean isReplaying();
    boolean isCancelRequested();
    SearchAttributes getSearchAttributes();  // protobuf
    Payload getMemo(String key);

    // === State Modification ===
    void upsertSearchAttributes(SearchAttributes attributes);
    void upsertMemo(Memo memo);
    void continueAsNewOnCompletion(ContinueAsNewWorkflowExecutionCommandAttributes attributes);

    // === Info ===
    Duration getWorkflowRunTimeout();
    Duration getWorkflowExecutionTimeout();
    Duration getWorkflowTaskTimeout();
    long getRunStartedTimestampMillis();
    int getAttempt();
    String getCronSchedule();
    Optional<Payloads> getLastCompletionResult();
    Optional<Failure> getPreviousRunFailure();
    Scope getMetricsScope();
    // ... more
}
```

### What Kotlin SDK Actually Uses from Java Internals

From `KotlinWorkflowContext.kt`:

```kotlin
// Wraps ReplayWorkflowContext with suspend functions
internal class KotlinWorkflowContext(
    internal val replayContext: ReplayWorkflowContext,
    internal val dataConverter: DataConverter
) {
    suspend fun createTimer(duration: Duration): Unit = suspendCancellableCoroutine { cont ->
        replayContext.newTimer(duration, null) { exception ->
            if (exception != null) cont.resumeWithException(exception)
            else cont.resume(Unit)
        }
    }

    suspend fun executeActivity(parameters: ExecuteActivityParameters): Optional<Payloads> =
        suspendCancellableCoroutine { cont ->
            replayContext.scheduleActivityTask(parameters) { result, failure ->
                if (failure != null) cont.resumeWithException(dataConverter.failureToException(failure))
                else cont.resume(result)
            }
        }
    // ... etc
}
```

### Internal Types Used by Both SDKs

```
io.temporal.internal.statemachines:
  - ExecuteActivityParameters
  - ExecuteLocalActivityParameters
  - StartChildWorkflowExecutionParameters
  - LocalActivityCallback
  - UpdateProtocolCallback

io.temporal.internal.replay:
  - ReplayWorkflowContext
  - ReplayWorkflow
  - WorkflowContext

io.temporal.internal.common:
  - ProtobufTimeUtils
  - SearchAttributesUtil
  - ProtoConverters

io.temporal.internal.worker:
  - WorkflowImplementationFactory
```

### Current Problem: Java Public Types in Kotlin

The Kotlin SDK currently uses these **Java public types**:

```kotlin
// In KotlinWorkflowContext.kt - uses Java options directly
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.ContinueAsNewOptions
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.SearchAttributes
import io.temporal.common.converter.DataConverter
```

### Proposed Change

Move `ReplayWorkflowContext` and related internal types to `temporal-core`. Then:

1. **Core keeps**: `ReplayWorkflowContext`, parameter classes, utility classes
2. **Each SDK defines**: Its own options classes that convert to parameter classes
3. **Remove**: Kotlin SDK dependency on Java public options classes

```
Before:
  Kotlin uses → Java ActivityOptions → builds ExecuteActivityParameters

After:
  Kotlin uses → KActivityOptions → builds ExecuteActivityParameters (directly)

// Core info interface - primitives only
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
    // Note: No getRetryOptions() or getPriority() - those use SDK types
}
```

#### SDK Layer (SDK-Specific Abstractions)

Each SDK builds its own async model and uses SDK-specific types:

```java
package io.temporal.workflow;

// Java SDK - uses Promise-based async model
public final class Workflow {
    // === Async abstractions (Java-specific) ===
    // Kotlin uses suspend functions instead
    public static Promise<Void> newTimer(Duration duration);  // wraps core scheduleTimer
    public static void sleep(Duration duration);              // blocking wrapper
    public static void await(Supplier<Boolean> condition);    // wraps core blockUntil
    public static boolean await(Duration timeout, Supplier<Boolean> condition);

    // Promise creation (Java async primitive)
    public static <E> Promise<E> newPromise();
    public static <E> Promise<E> newPromise(E value);
    public static <E> Promise<E> newFailedPromise(RuntimeException failure);

    // Cancellation scopes (Java pattern - Kotlin uses structured concurrency)
    public static CancellationScope newCancellationScope(Runnable runnable);
    public static CancellationScope newDetachedCancellationScope(Runnable runnable);

    // Workflow concurrency (Java primitives - Kotlin uses coroutine primitives)
    public static <E> WorkflowQueue<E> newQueue(int capacity);
    public static WorkflowLock newWorkflowLock();
    public static WorkflowSemaphore newWorkflowSemaphore(int permits);

    // === Stub creation (uses SDK options classes) ===
    public static <T> T newActivityStub(Class<T> activityInterface, ActivityOptions options);
    public static <T> T newLocalActivityStub(Class<T> activityInterface, LocalActivityOptions options);
    public static <T> T newChildWorkflowStub(Class<T> workflowInterface, ChildWorkflowOptions options);
    public static <T> T newExternalWorkflowStub(Class<T> workflowInterface, String workflowId);
    public static <T> T newContinueAsNewStub(Class<T> workflowInterface, ContinueAsNewOptions options);
    public static <T> T newNexusServiceStub(Class<T> nexusServiceInterface, NexusServiceOptions options);
    // ... untyped variants

    // Continue as new - uses SDK options
    public static void continueAsNew(Object... args);
    public static void continueAsNew(ContinueAsNewOptions options, Object... args);

    // === SDK-specific info (wraps core, adds SDK types) ===
    public static WorkflowInfo getInfo();  // adds RetryOptions, Priority

    // Data access - uses DataConverter
    public static <T> Optional<T> getMemo(String key, Class<T> valueClass);
    public static <T> T getLastCompletionResult(Class<T> resultClass);
    public static Optional<Exception> getPreviousRunFailure();

    // Search attributes - SDK types
    public static SearchAttributes getTypedSearchAttributes();
    public static void upsertTypedSearchAttributes(SearchAttributeUpdate<?>... updates);

    // Handler registration - SDK pattern
    public static void registerListener(Object listener);

    // Activity options management
    public static void setDefaultActivityOptions(ActivityOptions options);
    // ... etc

    // Exception handling
    public static RuntimeException wrap(Exception e);
}
```

```kotlin
package io.temporal.kotlinsdk.workflow

// Kotlin SDK - uses suspend functions and coroutines
object KWorkflow {
    // === Suspend-based operations (Kotlin-specific) ===
    // No Promise needed - uses suspend functions
    suspend fun delay(duration: Duration)           // wraps core scheduleTimer
    suspend fun awaitCondition(condition: () -> Boolean)
    suspend fun awaitCondition(timeout: Duration, condition: () -> Boolean): Boolean

    // Coroutine-based concurrency (replaces Java's Promise/Queue/Lock)
    // Uses standard Kotlin Channel, Mutex, Semaphore with workflow dispatcher

    // === Stub creation (uses Kotlin options classes) ===
    inline fun <reified T> newActivityStub(options: KActivityOptions): T
    inline fun <reified T> newChildWorkflowStub(options: KChildWorkflowOptions): T
    // ... etc

    // === SDK-specific info ===
    fun getInfo(): KWorkflowInfo  // Kotlin data class
}
```

### Activity API - Current Architecture (Actual Code)

#### Current Shared Interface

Unlike workflows (which use internal `ReplayWorkflowContext`), activities use **Java SDK's public `ActivityExecutionContext`** directly:

```
┌─────────────────────────────────────────────────────────────────┐
│          ActivityExecutionContext (Java public type)            │
│                   io.temporal.activity                          │
│  - getInfo(): ActivityInfo                                      │
│  - heartbeat(details)                                           │
│  - getHeartbeatDetails(Class)                                   │
│  - getTaskToken(): byte[]                                       │
│  - doNotCompleteOnReturn()                                      │
│  - useLocalManualCompletion(): ManualActivityCompletionClient   │
│  - getWorkflowClient(): WorkflowClient                          │
│  - getMetricsScope(): Scope                                     │
└─────────────────────────────────────────────────────────────────┘
              │                                    │
              ▼                                    ▼
┌───────────────────────────────────┐    ┌───────────────────────────────────┐
│     Activity.getExecutionContext() │    │      KActivityContext.current     │
│        (Java public API)          │    │        (Kotlin public API)        │
│  - Returns ActivityExecutionContext│    │  - KActivityContextImpl wraps Java │
│  - Thread-local access            │    │  - SuspendActivityContext for     │
│                                   │    │    suspend activities             │
└───────────────────────────────────┘    └───────────────────────────────────┘
```

#### What Kotlin SDK Currently Wraps

From `KActivityContextImpl.kt`:
```kotlin
internal class KActivityContextImpl(
  private val javaContext: ActivityExecutionContext  // Java public type!
) : KActivityContext {
  override val info: KActivityInfo
    get() = KActivityInfoImpl(javaContext.info)  // Wraps Java ActivityInfo

  override fun heartbeat(details: Any?) {
    javaContext.heartbeat(details)  // Delegates to Java
  }

  override val taskToken: ByteArray
    get() = javaContext.taskToken
  // ... etc
}
```

From `KActivityInfoImpl.kt`:
```kotlin
internal class KActivityInfoImpl(private val info: ActivityInfo) : KActivityInfo {
  override val namespace: String get() = info.namespace
  override val workflowId: String get() = info.workflowId
  override val runId: String get() = info.runId
  override val activityType: String get() = info.activityType
  // ... all primitives
}
```

#### Proposed Change for Activities

Move activity execution internals to core, but define a new `CoreActivityContext` interface:

```java
package io.temporal.core.activity;

// Core interface - primitives and protos only
public interface CoreActivityContext {
    // Basic info (primitives)
    byte[] getTaskToken();
    String getWorkflowId();
    String getRunId();
    String getActivityId();
    String getActivityType();
    long getScheduledTimestamp();
    long getStartedTimestamp();
    long getCurrentAttemptScheduledTimestamp();
    Duration getScheduleToCloseTimeout();
    Duration getStartToCloseTimeout();
    Duration getHeartbeatTimeout();
    String getWorkflowType();
    String getWorkflowNamespace();
    String getActivityNamespace();
    String getActivityTaskQueue();
    int getAttempt();
    boolean isLocal();

    // Heartbeat operations
    void heartbeat(Payloads details);
    Optional<Payloads> getHeartbeatDetails();

    // Async completion
    void doNotCompleteOnReturn();
    boolean isDoNotCompleteOnReturn();

    // Metrics
    Scope getMetricsScope();
}
```

#### SDK Layer

Java SDK:
```java
package io.temporal.activity;

public interface ActivityExecutionContext extends CoreActivityContext {
    // SDK-specific info (adds RetryOptions, Priority)
    ActivityInfo getInfo();

    // Typed heartbeat (uses SDK's DataConverter)
    <V> void heartbeat(V details);
    <V> Optional<V> getHeartbeatDetails(Class<V> detailsClass);

    // Manual completion - returns SDK client
    ManualActivityCompletionClient useLocalManualCompletion();

    // WorkflowClient access - SDK type
    WorkflowClient getWorkflowClient();
}
```

Kotlin SDK:
```kotlin
package io.temporal.kotlinsdk.activity

interface KActivityContext {
    val info: KActivityInfo

    // Typed heartbeat
    fun heartbeat(details: Any? = null)
    fun <T> heartbeatDetails(detailsClass: Class<T>): T?

    val taskToken: ByteArray
    fun doNotCompleteOnReturn()
    val isDoNotCompleteOnReturn: Boolean
}

// Implementation wraps CoreActivityContext (not Java's ActivityExecutionContext)
internal class KActivityContextImpl(
  private val coreContext: CoreActivityContext,
  private val dataConverter: DataConverter
) : KActivityContext {
    override fun heartbeat(details: Any?) {
        coreContext.heartbeat(dataConverter.toPayloads(details))
    }
    // ... etc
}
```

### Summary: What Changes

#### Workflows

| Current | Proposed |
|---------|----------|
| `ReplayWorkflowContext` in `io.temporal.internal.replay` | Move to `io.temporal.core.replay` |
| `ExecuteActivityParameters` in `io.temporal.internal.statemachines` | Move to `io.temporal.core.statemachines` |
| `KotlinWorkflowContext` imports Java `ActivityOptions`, `LocalActivityOptions`, etc. | Build parameters directly from `KActivityOptions` |

**Key insight**: `ReplayWorkflowContext` is already callback-based and SDK-agnostic. The problem is that building `ExecuteActivityParameters` currently requires Java options classes.

#### Activities

| Current | Proposed |
|---------|----------|
| `ActivityExecutionContext` is Java SDK public type | Create `CoreActivityContext` in core with primitives/Payloads |
| `KActivityContextImpl` wraps Java `ActivityExecutionContext` | Wrap `CoreActivityContext` instead |
| `ActivityInfo` is Java SDK public type | Create `CoreActivityInfo` in core with primitives |
| `KActivityInfoImpl` wraps Java `ActivityInfo` | Wrap `CoreActivityInfo` instead |

**Key insight**: Activity context is simpler than workflow context - it's primarily about info access and heartbeating.

### Summary Table - Core vs SDK-Specific

| Category | Core (temporal-core) | Java SDK | Kotlin SDK |
|----------|---------------------|----------|------------|
| **Workflow Scheduling** | `ReplayWorkflowContext` (callback-based) | `WorkflowInternal` (Promise-based) | `KotlinWorkflowContext` (suspend functions) |
| **Activity Parameters** | `ExecuteActivityParameters` | Built from `ActivityOptions` | Built from `KActivityOptions` |
| **Workflow Info** | `CoreWorkflowInfo` (primitives) | `WorkflowInfo` (adds `RetryOptions`, `Priority`) | `KWorkflowInfo` (Kotlin data class) |
| **Activity Context** | `CoreActivityContext` (primitives + Payloads) | `ActivityExecutionContext` (typed, DataConverter) | `KActivityContext` (typed, DataConverter) |
| **Activity Info** | `CoreActivityInfo` (primitives) | `ActivityInfo` (adds `RetryOptions`, `Priority`) | `KActivityInfo` (Kotlin data class) |
| **Timer/Delay** | `ReplayWorkflowContext.newTimer()` (callback) | `Workflow.newTimer()` → `Promise<Void>` | `KWorkflow.delay()` → suspend |
| **Side Effects** | `ReplayWorkflowContext.sideEffect()` (callback) | `Workflow.sideEffect()` | `KWorkflow.sideEffect()` |
| **Versioning** | `ReplayWorkflowContext.getVersion()` (callback) | `Workflow.getVersion()` | `KWorkflow.getVersion()` |
| **Random/Time** | `ReplayWorkflowContext` methods | `Workflow.newRandom()`, etc. | `KWorkflow.newRandom()`, etc. |
| **Concurrency** | - | `Promise`, `CancellationScope`, `WorkflowQueue`, `WorkflowLock` | Kotlin coroutines, `Channel`, `Mutex` |
| **Search Attributes** | Protobuf `SearchAttributes` | `TypedSearchAttributes` | `KSearchAttributes` |
| **Stub Creation** | - | `Workflow.newActivityStub(ActivityOptions)` | `KWorkflow.newActivityStub(KActivityOptions)` |
| **WorkflowClient** | - | `WorkflowClient` | `KClient` |
| **Data Conversion** | `DataConverter` interface | `DefaultDataConverter`, typed methods | `DataConverter` (reuse from core) |

### Key Design Decisions

1. **Core is callback-based** - `ReplayWorkflowContext` uses callbacks, not Promise or suspend. Each SDK wraps with its async model.

2. **Core uses Payloads** - Heartbeat, search attributes, memos all use protobuf `Payloads`. SDKs handle serialization.

3. **Info classes split** - Core provides primitives only. SDK-specific types like `RetryOptions` and `Priority` are added in SDK wrappers.

4. **Options classes are SDK-specific** - No shared options. Each SDK converts its options directly to core parameter classes.

5. **WorkflowClient/completion clients are SDK-specific** - Activity context methods that return SDK clients stay in SDK layer.

## Java Public Types Currently Used by Kotlin SDK

Based on actual code analysis, these imports need to be eliminated:

### From `KotlinWorkflowContext.kt`:
```kotlin
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.ContinueAsNewOptions
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.SearchAttributes
import io.temporal.common.converter.DataConverter
```

### From activity internal classes:
```kotlin
import io.temporal.activity.ActivityExecutionContext
import io.temporal.activity.ActivityInfo
import io.temporal.activity.ManualActivityCompletionClient
```

### From converters:
```kotlin
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
```

### Required Changes

| Java Public Type | Kotlin Replacement |
|-----------------|-------------------|
| `ActivityOptions` | `KActivityOptions` → `ExecuteActivityParameters` directly |
| `LocalActivityOptions` | `KLocalActivityOptions` → `ExecuteLocalActivityParameters` directly |
| `ChildWorkflowOptions` | `KChildWorkflowOptions` → `StartChildWorkflowExecutionParameters` directly |
| `ContinueAsNewOptions` | `KContinueAsNewOptions` → `ContinueAsNewWorkflowExecutionCommandAttributes` directly |
| `RetryOptions` | `KRetryOptions` → `RetryPolicy` protobuf directly |
| `ActivityCancellationType` | `KActivityCancellationType` (Kotlin enum) |
| `SearchAttributes` | Use protobuf `SearchAttributes` in core, `KSearchAttributes` in Kotlin |
| `ActivityExecutionContext` | `CoreActivityContext` (new core interface) |
| `ActivityInfo` | `CoreActivityInfo` (new core interface) |

## Schedule API

### Current Implementation

The Java SDK has a `ScheduleProtoUtil` class (~535 lines) that handles bidirectional conversion between Java Schedule classes and protobuf types. The conversion is **mechanical but verbose** - straightforward field mapping with no complex logic.

### Kotlin Schedule Conversion

The Kotlin SDK can replicate this conversion directly. Key observations:

1. **Conversion is mechanical** - Just field-by-field mapping between SDK types and protobufs
2. **No complex logic** - Uses `ProtobufTimeUtils` (in core) for duration/timestamp conversion
3. **Already partially done** - `KScheduleConverters.kt` exists and does similar work

#### Proposed Kotlin Implementation

```kotlin
package io.temporal.kotlinsdk.internal.converters

internal object KScheduleProtoUtil {

    fun scheduleToProto(schedule: KSchedule): io.temporal.api.schedule.v1.Schedule {
        return io.temporal.api.schedule.v1.Schedule.newBuilder().apply {
            action = actionToProto(schedule.action)
            spec = specToProto(schedule.spec)
            schedule.policy?.let { policies = policyToProto(it) }
            schedule.state?.let { state = stateToProto(it) }
        }.build()
    }

    fun actionToProto(action: KScheduleAction): ScheduleAction {
        return when (action) {
            is KScheduleActionStartWorkflow -> {
                val workflowRequest = NewWorkflowExecutionInfo.newBuilder().apply {
                    workflowId = action.options.workflowId
                    setWorkflowType(WorkflowType.newBuilder().setName(action.workflowType))
                    setTaskQueue(TaskQueue.newBuilder().setName(action.options.taskQueue))
                    action.options.workflowRunTimeout?.let {
                        workflowRunTimeout = it.toProtoDuration()
                    }
                    action.options.workflowExecutionTimeout?.let {
                        workflowExecutionTimeout = it.toProtoDuration()
                    }
                    // ... all other fields
                }.build()
                ScheduleAction.newBuilder().setStartWorkflow(workflowRequest).build()
            }
        }
    }

    fun specToProto(spec: KScheduleSpec): ScheduleSpec {
        return ScheduleSpec.newBuilder().apply {
            spec.timeZoneName?.let { timezoneName = it }
            spec.jitter?.let { jitter = it.toProtoDuration() }
            spec.startAt?.let { startTime = it.toProtoTimestamp() }
            spec.endAt?.let { endTime = it.toProtoTimestamp() }
            spec.calendars?.forEach { cal ->
                addStructuredCalendar(calendarToProto(cal))
            }
            spec.intervals?.forEach { interval ->
                addInterval(intervalToProto(interval))
            }
            spec.cronExpressions?.let { addAllCronString(it) }
        }.build()
    }

    // Reverse conversions
    fun protoToSchedule(proto: io.temporal.api.schedule.v1.Schedule): KSchedule {
        return KSchedule(
            action = protoToAction(proto.action),
            spec = protoToSpec(proto.spec),
            policy = if (proto.hasPolicies()) protoToPolicy(proto.policies) else null,
            state = if (proto.hasState()) protoToState(proto.state) else null
        )
    }

    // ... other conversion methods
}
```

### GenericScheduleClient in Core

For consistency, core should provide a `GenericScheduleClient` with protobuf-only interface:

```java
package io.temporal.core.client;

public interface GenericScheduleClient {
    CreateScheduleResponse createSchedule(CreateScheduleRequest request);
    DescribeScheduleResponse describeSchedule(DescribeScheduleRequest request);
    UpdateScheduleResponse updateSchedule(UpdateScheduleRequest request);
    PatchScheduleResponse patchSchedule(PatchScheduleRequest request);
    DeleteScheduleResponse deleteSchedule(DeleteScheduleRequest request);
    CompletableFuture<ListSchedulesResponse> listSchedulesAsync(ListSchedulesRequest request);
}
```

This allows `KClient` to build schedule requests directly from `KSchedule*` types without any Java SDK dependency.

## Worker and WorkerFactory Architecture

### Current Problem

The Kotlin SDK currently depends on Java's `Worker`, `WorkerFactory`, `WorkerOptions`, and `WorkerFactoryOptions` classes directly.

### Proposed Solution: Internal Worker Interfaces in Core

Create internal interfaces in `temporal-core` that use primitives and protobuf types only:

```java
package io.temporal.core.worker;

/**
 * Internal worker factory - primitives and core types only.
 * Each SDK wraps this with SDK-specific options.
 */
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

/**
 * Internal worker - accepts WorkflowImplementationFactory and activity objects.
 */
public interface WorkerInternal {
    void addWorkflowImplementationFactory(WorkflowImplementationFactory factory);
    void registerActivitiesImplementations(Object... activities);
    void registerNexusServiceImplementation(Object nexusService);
    void start();
    void shutdown();
    String getTaskQueue();
}

/**
 * Worker options using primitives only - no SDK-specific types.
 */
public class WorkerOptionsInternal {
    private final int maxConcurrentActivityTaskExecutors;
    private final int maxConcurrentWorkflowTaskExecutors;
    private final int maxConcurrentLocalActivityTaskExecutors;
    private final int maxConcurrentWorkflowTaskPollers;
    private final int maxConcurrentActivityTaskPollers;
    private final double maxActivitiesPerSecond;
    private final double maxTaskQueueActivitiesPerSecond;
    private final boolean localActivityWorkerOnly;
    private final long defaultDeadlockDetectionTimeout;
    private final long maxHeartbeatThrottleInterval;
    private final long defaultHeartbeatThrottleInterval;
    private final double stickyQueueScheduleToStartTimeout;
    private final boolean disableEagerExecution;
    private final boolean useBuildIdForVersioning;
    private final String buildId;
    private final String identity;
    // Note: No WorkerTuner - that's SDK-specific advanced feature

    // Builder pattern
    public static Builder newBuilder() { ... }
}

/**
 * Worker factory options using primitives only.
 */
public class WorkerFactoryOptionsInternal {
    private final int workflowCacheSize;
    private final int maxWorkflowThreadCount;
    private final long workflowHostLocalTaskQueueScheduleToStartTimeout;
    private final boolean enableLoggingInReplay;
    // Note: No WorkerInterceptor list - interceptors are SDK-specific

    public static Builder newBuilder() { ... }
}
```

### SDK Layer

#### Java SDK

```java
package io.temporal.worker;

public final class WorkerFactory {
    private final WorkerFactoryInternal internal;
    private final List<WorkerInterceptor> interceptors;

    public static WorkerFactory newInstance(WorkflowClient client) {
        return newInstance(client, WorkerFactoryOptions.getDefaultInstance());
    }

    public static WorkerFactory newInstance(WorkflowClient client, WorkerFactoryOptions options) {
        WorkerFactoryOptionsInternal internalOptions = toInternal(options);
        WorkerFactoryInternal internal = new WorkerFactoryInternalImpl(
            client.getWorkflowServiceStubs(),
            client.getOptions(),
            internalOptions
        );
        return new WorkerFactory(internal, options.getWorkerInterceptors());
    }

    public Worker newWorker(String taskQueue, WorkerOptions options) {
        WorkerOptionsInternal internalOptions = toInternal(options);
        WorkerInternal internalWorker = internal.newWorker(taskQueue, internalOptions);
        return new Worker(internalWorker, options, interceptors);
    }

    private static WorkerFactoryOptionsInternal toInternal(WorkerFactoryOptions options) {
        return WorkerFactoryOptionsInternal.newBuilder()
            .setWorkflowCacheSize(options.getWorkflowCacheSize())
            .setMaxWorkflowThreadCount(options.getMaxWorkflowThreadCount())
            // ... map all primitive fields
            .build();
    }
}
```

#### Kotlin SDK

```kotlin
package io.temporal.kotlinsdk.worker

class KWorkerFactory private constructor(
    private val internal: WorkerFactoryInternal,
    private val interceptors: List<KWorkerInterceptor>,
    private val dataConverter: DataConverter
) {
    companion object {
        fun newInstance(client: KClient, options: KWorkerFactoryOptions = KWorkerFactoryOptions()): KWorkerFactory {
            val internalOptions = options.toInternal()
            val internal = WorkerFactoryInternalImpl(
                client.workflowServiceStubs,
                client.options.namespace,
                client.options.identity,
                internalOptions
            )
            return KWorkerFactory(internal, options.interceptors, client.dataConverter)
        }
    }

    fun newWorker(taskQueue: String, options: KWorkerOptions = KWorkerOptions()): KWorker {
        val internalOptions = options.toInternal()
        val internalWorker = internal.newWorker(taskQueue, internalOptions)
        return KWorker(internalWorker, options, interceptors, dataConverter)
    }

    fun start() = internal.start()
    fun shutdown() = internal.shutdown()
    // ...
}

// Kotlin options with DSL builder
data class KWorkerOptions(
    val maxConcurrentActivityTaskExecutors: Int = 200,
    val maxConcurrentWorkflowTaskExecutors: Int = 200,
    val maxConcurrentLocalActivityTaskExecutors: Int = 200,
    // ... all options
) {
    internal fun toInternal(): WorkerOptionsInternal {
        return WorkerOptionsInternal.newBuilder()
            .setMaxConcurrentActivityTaskExecutors(maxConcurrentActivityTaskExecutors)
            .setMaxConcurrentWorkflowTaskExecutors(maxConcurrentWorkflowTaskExecutors)
            // ... map all fields
            .build()
    }
}
```

### Benefits

1. **No Java SDK dependency** - Kotlin SDK only depends on `temporal-core`
2. **SDK-specific interceptors** - Each SDK defines its own interceptor chain
3. **SDK-specific advanced features** - `WorkerTuner` stays in Java SDK, Kotlin can add `KWorkerTuner` if needed
4. **Clean separation** - Core handles worker lifecycle, SDKs handle options and interceptors

## Kotlin Annotations

### Rationale

The Kotlin SDK currently imports Java annotations (`@WorkflowInterface`, `@WorkflowMethod`, etc.). To achieve full independence, Kotlin should define its own annotations.

### Kotlin Annotation Definitions

```kotlin
package io.temporal.kotlinsdk.workflow

/**
 * Marks an interface as a Temporal workflow definition.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KWorkflowInterface

/**
 * Marks a method as the main workflow entry point.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KWorkflowMethod(
    /** Optional workflow type name. Defaults to method name. */
    val name: String = ""
)

/**
 * Marks a method as a signal handler.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KSignalMethod(
    /** Optional signal name. Defaults to method name. */
    val name: String = ""
)

/**
 * Marks a method as a query handler.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KQueryMethod(
    /** Optional query type name. Defaults to method name. */
    val name: String = ""
)

/**
 * Marks a method as an update handler.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KUpdateMethod(
    /** Optional update name. Defaults to method name. */
    val name: String = ""
)

/**
 * Marks a method as an update validator.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KUpdateValidatorMethod(
    /** The name of the update this validates. Must match an @KUpdateMethod name. */
    val updateName: String
)
```

```kotlin
package io.temporal.kotlinsdk.activity

/**
 * Marks an interface as a Temporal activity definition.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KActivityInterface(
    /** Optional prefix for activity type names. */
    val namePrefix: String = ""
)

/**
 * Marks a method as an activity implementation.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KActivityMethod(
    /** Optional activity type name. Defaults to method name. */
    val name: String = ""
)
```

### Annotation Processing

The Kotlin SDK needs metadata extraction similar to Java's `POJOWorkflowInterfaceMetadata` and `POJOActivityInterfaceMetadata`:

```kotlin
package io.temporal.kotlinsdk.internal.metadata

internal object KWorkflowMetadata {
    fun getWorkflowType(workflowInterface: KClass<*>): String {
        val annotation = workflowInterface.findAnnotation<KWorkflowInterface>()
            ?: throw IllegalArgumentException("${workflowInterface.simpleName} must be annotated with @KWorkflowInterface")

        val workflowMethod = workflowInterface.memberFunctions
            .find { it.findAnnotation<KWorkflowMethod>() != null }
            ?: throw IllegalArgumentException("${workflowInterface.simpleName} must have a method annotated with @KWorkflowMethod")

        val methodAnnotation = workflowMethod.findAnnotation<KWorkflowMethod>()!!
        return methodAnnotation.name.ifEmpty { workflowMethod.name }
    }

    fun getSignalMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
        return workflowInterface.memberFunctions
            .filter { it.findAnnotation<KSignalMethod>() != null }
            .associateBy { fn ->
                fn.findAnnotation<KSignalMethod>()!!.name.ifEmpty { fn.name }
            }
    }

    fun getQueryMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
        return workflowInterface.memberFunctions
            .filter { it.findAnnotation<KQueryMethod>() != null }
            .associateBy { fn ->
                fn.findAnnotation<KQueryMethod>()!!.name.ifEmpty { fn.name }
            }
    }

    fun getUpdateMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
        return workflowInterface.memberFunctions
            .filter { it.findAnnotation<KUpdateMethod>() != null }
            .associateBy { fn ->
                fn.findAnnotation<KUpdateMethod>()!!.name.ifEmpty { fn.name }
            }
    }
}

internal object KActivityMetadata {
    fun getActivityType(activityInterface: KClass<*>, method: KFunction<*>): String {
        val interfaceAnnotation = activityInterface.findAnnotation<KActivityInterface>()
        val methodAnnotation = method.findAnnotation<KActivityMethod>()

        val prefix = interfaceAnnotation?.namePrefix ?: ""
        val name = methodAnnotation?.name?.ifEmpty { method.name } ?: method.name

        return if (prefix.isNotEmpty()) "$prefix$name" else name
    }
}
```

### Usage Example

```kotlin
@KWorkflowInterface
interface GreetingWorkflow {
    @KWorkflowMethod
    suspend fun greet(name: String): String

    @KSignalMethod
    suspend fun updateGreeting(greeting: String)

    @KQueryMethod
    fun getCurrentGreeting(): String

    @KUpdateMethod(name = "setLanguage")
    suspend fun changeLanguage(language: String): String

    @KUpdateValidatorMethod(updateName = "setLanguage")
    fun validateLanguage(language: String)
}

@KActivityInterface(namePrefix = "Greeting_")
interface GreetingActivities {
    @KActivityMethod
    suspend fun composeGreeting(greeting: String, name: String): String
}
```

## Interceptor Architecture

### Resolved: SDK-Specific Interceptors

Each SDK defines its own interceptor interfaces and chains. The Kotlin SDK already has:

- `KWorkerInterceptor`
- `KWorkflowInboundCallsInterceptor`
- `KWorkflowOutboundCallsInterceptor`
- `KActivityInboundCallsInterceptor`
- `KWorkflowClientInterceptor`
- `KWorkflowClientCallsInterceptor`

**Core does not need interceptor interfaces.** Core provides the extension points (callbacks, factories) that SDKs use to implement their interceptor chains.

### Interceptor Converters

For interop, the Kotlin SDK provides converters between Java and Kotlin interceptors:

```kotlin
// Already exists in KWorkflowClientInterceptorConverters.kt
internal class WorkflowClientInterceptorKotlinWrapper(
    private val kotlinInterceptor: KWorkflowClientInterceptor
) : WorkflowClientInterceptor { ... }

internal class KWorkflowClientInterceptorJavaWrapper(
    private val javaInterceptor: WorkflowClientInterceptor
) : KWorkflowClientInterceptor { ... }
```

These converters allow using Java interceptors in Kotlin and vice versa during migration.

## Testing Module: temporal-kotlin-testing

### Module Structure

```
temporal-kotlin-testing/
├── src/main/kotlin/io/temporal/kotlin/testing/
│   ├── KTestWorkflowEnvironment.kt
│   ├── KTestWorkflowExtension.kt      # JUnit 5 extension
│   ├── KTestWorkflowRule.kt           # JUnit 4 rule (optional)
│   ├── KTestActivityEnvironment.kt
│   └── internal/
│       └── KTestWorkflowEnvironmentImpl.kt
└── build.gradle.kts
```

### Dependencies

```kotlin
// temporal-kotlin-testing/build.gradle.kts
dependencies {
    api(project(":temporal-kotlin"))
    api(project(":temporal-core-testing"))  // New core testing module

    // Optional JUnit support
    compileOnly("org.junit.jupiter:junit-jupiter-api:5.10.0")
    compileOnly("junit:junit:4.13.2")
}
```

### Core Testing Module

Create `temporal-core-testing` with shared test infrastructure:

```java
package io.temporal.core.testing;

/**
 * Core test environment - provides in-memory Temporal server.
 */
public interface TestEnvironmentInternal {
    WorkflowServiceStubs getWorkflowServiceStubs();
    String getNamespace();
    void start();
    void shutdown();
    void close();

    // Time control for testing
    void sleep(Duration duration);
    void setCurrentTime(Instant time);
    boolean isTimeSkippingEnabled();
}
```

### Kotlin Test Environment

```kotlin
package io.temporal.kotlinsdk.testing

/**
 * Test environment for Kotlin workflows and activities.
 */
class KTestWorkflowEnvironment private constructor(
    private val internal: TestEnvironmentInternal,
    private val options: KTestWorkflowEnvironmentOptions
) : AutoCloseable {

    companion object {
        /**
         * Creates a new test environment with time skipping enabled.
         */
        fun newInstance(
            options: KTestWorkflowEnvironmentOptions = KTestWorkflowEnvironmentOptions()
        ): KTestWorkflowEnvironment {
            val internal = TestEnvironmentInternalImpl(options.toInternal())
            return KTestWorkflowEnvironment(internal, options)
        }
    }

    /**
     * Creates a new KClient connected to the test environment.
     */
    fun newClient(options: KClientOptions = KClientOptions()): KClient {
        return KClient.newInstance(
            internal.workflowServiceStubs,
            options.copy(namespace = internal.namespace)
        )
    }

    /**
     * Creates a new KWorkerFactory for the test environment.
     */
    fun newWorkerFactory(
        client: KClient,
        options: KWorkerFactoryOptions = KWorkerFactoryOptions()
    ): KWorkerFactory {
        return KWorkerFactory.newInstance(client, options)
    }

    /**
     * Creates a new worker for the test environment.
     */
    fun newWorker(
        taskQueue: String,
        options: KWorkerOptions = KWorkerOptions()
    ): KWorker {
        val client = newClient()
        val factory = newWorkerFactory(client)
        return factory.newWorker(taskQueue, options)
    }

    /**
     * Advances time in the test environment.
     * Only works when time skipping is enabled.
     */
    fun sleep(duration: Duration) {
        internal.sleep(duration.toJavaDuration())
    }

    fun start() = internal.start()

    override fun close() = internal.close()
}

data class KTestWorkflowEnvironmentOptions(
    val useTimeskipping: Boolean = true,
    val initialTimeMillis: Long? = null,
    val workflowServiceStubsOptions: WorkflowServiceStubsOptions? = null
) {
    internal fun toInternal(): TestEnvironmentOptionsInternal {
        return TestEnvironmentOptionsInternal.newBuilder()
            .setUseTimeskipping(useTimeskipping)
            .apply { initialTimeMillis?.let { setInitialTimeMillis(it) } }
            .build()
    }
}
```

### JUnit 5 Extension

```kotlin
package io.temporal.kotlinsdk.testing

import org.junit.jupiter.api.extension.*

/**
 * JUnit 5 extension for Kotlin workflow tests.
 */
class KTestWorkflowExtension(
    private val options: KTestWorkflowEnvironmentOptions = KTestWorkflowEnvironmentOptions()
) : BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private lateinit var testEnvironment: KTestWorkflowEnvironment
    private lateinit var client: KClient
    private lateinit var workerFactory: KWorkerFactory

    val environment: KTestWorkflowEnvironment get() = testEnvironment

    override fun beforeAll(context: ExtensionContext) {
        testEnvironment = KTestWorkflowEnvironment.newInstance(options)
        client = testEnvironment.newClient()
        workerFactory = testEnvironment.newWorkerFactory(client)
    }

    override fun afterAll(context: ExtensionContext) {
        testEnvironment.close()
    }

    override fun beforeEach(context: ExtensionContext) {
        // Start workers if not already started
    }

    override fun afterEach(context: ExtensionContext) {
        // Clean up between tests
    }

    fun newWorker(
        taskQueue: String,
        options: KWorkerOptions = KWorkerOptions()
    ): KWorker {
        return workerFactory.newWorker(taskQueue, options)
    }

    fun newClient(): KClient = client
}
```

### Usage Example

```kotlin
@ExtendWith(KTestWorkflowExtension::class)
class GreetingWorkflowTest {

    companion object {
        @JvmField
        @RegisterExtension
        val testWorkflow = KTestWorkflowExtension()
    }

    @BeforeEach
    fun setUp() {
        val worker = testWorkflow.newWorker("test-queue")
        worker.registerWorkflowImplementation<GreetingWorkflowImpl>()
        worker.registerActivitiesImplementations(GreetingActivitiesImpl())
        testWorkflow.environment.start()
    }

    @Test
    fun `should greet user`() = runBlocking {
        val client = testWorkflow.newClient()
        val handle = client.startWorkflow<GreetingWorkflow, String>(
            KWorkflowOptions(taskQueue = "test-queue")
        ) { greet("World") }

        val result = handle.result()
        assertEquals("Hello, World!", result)
    }

    @Test
    fun `should handle signal`() = runBlocking {
        val client = testWorkflow.newClient()
        val handle = client.startWorkflow<GreetingWorkflow, String>(
            KWorkflowOptions(taskQueue = "test-queue")
        ) { greet("World") }

        handle.signal { updateGreeting("Hi") }

        // Advance time if needed
        testWorkflow.environment.sleep(Duration.ofSeconds(1))

        val greeting = handle.query { getCurrentGreeting() }
        assertEquals("Hi", greeting)
    }
}
```

## Open Questions

1. **Should `DataConverter` live in core or each SDK?**
   - Recommendation: Interface in core, default implementation in core, SDKs can extend
   - Note: Already used by both SDKs, natural fit for core

2. ~~**Where do interceptors live?**~~
   - **Resolved**: Each SDK defines its own interceptor interfaces
   - Kotlin already has `KWorkflowInboundCallsInterceptor`, `KActivityInboundCallsInterceptor`, etc.
   - Core provides extension points (callbacks, factories), not interceptor interfaces

3. **How to handle worker internals shared between SDKs?**
   - Recommendation: Keep replay/state machine code in core; SDK-specific workflow factories in each SDK
   - `ReplayWorkflowContext`, `WorkflowStateMachines`, state machine classes → core
   - `KotlinWorkflowImplementationFactory`, `WorkflowImplementationFactory` → SDK-specific

4. **Should we keep async variants in core client operations?**
   - Recommendation: Core provides both sync and `CompletableFuture` variants; Kotlin SDK adapts to coroutines
   - `GenericWorkflowClient` already has async variants that Kotlin can wrap with `suspendCoroutine`

5. ~~**Resolved: Promise and CancellationScope**~~
   - **Not in core** - These are Java SDK async primitives
   - Kotlin uses coroutines, channels, structured concurrency instead
   - Core only needs callback-based `ReplayWorkflowContext`
