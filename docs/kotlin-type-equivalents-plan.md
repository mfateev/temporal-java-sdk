# Plan: Implement Kotlin Equivalents for Java SDK Types

## Overview

This plan addresses Java SDK types exposed in the Kotlin SDK public API that should have Kotlin equivalents for a cleaner, more idiomatic API.

## Types to Implement

### 1. KWorkflowClientInterceptor (High Priority)

**Java Type:** `io.temporal.common.interceptors.WorkflowClientInterceptor`

**Rationale:** Consistency with existing K*Interceptors (KWorkerInterceptor, KActivityInboundCallsInterceptor, etc.). Enables suspend functions for client-side interception.

**Implementation:**

```kotlin
// File: temporal-kotlin/src/main/kotlin/io/temporal/kotlin/interceptor/KWorkflowClientInterceptor.kt

public interface KWorkflowClientInterceptor {
  /**
   * Creates a KWorkflowClientCallsInterceptor for intercepting client calls.
   */
  public fun workflowClientCallsInterceptor(
    next: KWorkflowClientCallsInterceptor
  ): KWorkflowClientCallsInterceptor
}

public interface KWorkflowClientCallsInterceptor {
  public suspend fun start(input: StartInput): StartOutput
  public suspend fun signal(input: SignalInput): SignalOutput
  public suspend fun signalWithStart(input: SignalWithStartInput): SignalWithStartOutput
  public suspend fun <R> getResult(input: GetResultInput<R>): GetResultOutput<R>
  public suspend fun <R> query(input: QueryInput<R>): QueryOutput<R>
  public suspend fun <R> startUpdate(input: StartUpdateInput<R>): WorkflowUpdateHandle<R>
  public suspend fun <R> pollWorkflowUpdate(input: PollWorkflowUpdateInput<R>): PollWorkflowUpdateOutput<R>
  public suspend fun cancel(input: CancelInput): CancelOutput
  public suspend fun terminate(input: TerminateInput): TerminateOutput
  public suspend fun describe(input: DescribeInput): DescribeOutput

  // Input/Output data classes as nested types
  public data class StartInput(...)
  public data class StartOutput(...)
  // ... etc
}

// JavaInterop.kt extensions for interceptor conversion
public fun KWorkflowClientInterceptor.toJava(): WorkflowClientInterceptor =
  // Internal wrapper that calls suspend functions using runBlocking or appropriate dispatcher

public fun WorkflowClientInterceptor.toKotlin(): KWorkflowClientInterceptor =
  // Internal wrapper that wraps blocking calls in withContext(Dispatchers.IO)
```

**Tasks:**
- [ ] Create `KWorkflowClientInterceptor` interface
- [ ] Create `KWorkflowClientCallsInterceptor` interface with suspend methods
- [ ] Create Kotlin data classes for all Input/Output types
- [ ] Create `KWorkflowClientCallsInterceptorBase` for easy extension
- [ ] Add `toJava()` extension on `KWorkflowClientInterceptor` in JavaInterop.kt (wraps Kotlin interceptor for Java SDK)
- [ ] Add `toKotlin()` extension on `WorkflowClientInterceptor` in JavaInterop.kt (wraps Java interceptor for Kotlin usage)
- [ ] Update `KClientOptions` to use `KWorkflowClientInterceptor` instead of `WorkflowClientInterceptor`

---

### 2. KRetryOptions (Medium Priority)

**Java Type:** `io.temporal.common.RetryOptions`

**Rationale:** Used in `KWorkflowInfo.retryOptions`. Common configuration class with builder pattern that should be a Kotlin data class.

**Implementation:**

```kotlin
// File: temporal-kotlin/src/main/kotlin/io/temporal/kotlin/common/KRetryOptions.kt

public data class KRetryOptions(
  val initialInterval: Duration = Duration.ofSeconds(1),
  val backoffCoefficient: Double = 2.0,
  val maximumInterval: Duration? = null,
  val maximumAttempts: Int = 0,
  val doNotRetry: List<String> = emptyList()
)
```

**Tasks:**
- [ ] Create `KRetryOptions` data class in `io.temporal.kotlin.common`
- [ ] Add `toJava()` method in KOptionsConverters
- [ ] Add `toKotlin()` extension in JavaInterop.kt
- [ ] Update `KWorkflowInfo` to return `KRetryOptions` instead of `RetryOptions`
- [ ] Update usages in KActivityOptions, KLocalActivityOptions if applicable

---

### 3. KActivityCancellationType (Medium Priority)

**Java Type:** `io.temporal.activity.ActivityCancellationType`

**Rationale:** Simple enum used in `KActivityOptions`. Wrapping provides a cleaner Kotlin API.

**Implementation:**

```kotlin
// File: temporal-kotlin/src/main/kotlin/io/temporal/kotlin/activity/KActivityCancellationType.kt

public enum class KActivityCancellationType {
  WAIT_CANCELLATION_COMPLETED,
  TRY_CANCEL,
  ABANDON;
}
```

**Tasks:**
- [ ] Create `KActivityCancellationType` enum in `io.temporal.kotlin.activity`
- [ ] Add `toJava()` and `toKotlin()` conversions
- [ ] Update `KActivityOptions` to use `KActivityCancellationType`

---

### 4. KWorkflowImplementationOptions (Medium Priority)

**Java Type:** `io.temporal.worker.WorkflowImplementationOptions`

**Rationale:** Used in `KWorkerOptions`. Worker configuration with builder pattern.

**Implementation:**

```kotlin
// File: temporal-kotlin/src/main/kotlin/io/temporal/kotlin/worker/KWorkflowImplementationOptions.kt

public data class KWorkflowImplementationOptions(
  val failWorkflowExceptionTypes: List<KClass<out Throwable>> = emptyList(),
  val activityOptions: Map<String, KActivityOptions> = emptyMap(),
  val defaultActivityOptions: KActivityOptions? = null,
  val localActivityOptions: Map<String, KLocalActivityOptions> = emptyMap(),
  val defaultLocalActivityOptions: KLocalActivityOptions? = null
)
```

**Tasks:**
- [ ] Create `KWorkflowImplementationOptions` data class
- [ ] Add `toJava()` method in KOptionsConverters
- [ ] Add `toKotlin()` extension in JavaInterop.kt
- [ ] Update `KWorkerOptions` to use `KWorkflowImplementationOptions`

---

### 5. KPriority (Low Priority)

**Java Type:** `io.temporal.common.Priority`

**Rationale:** Used in `KWorkflowOptions` and `KWorkflowInfo`. Simple wrapper around an integer value.

**Implementation:**

```kotlin
// File: temporal-kotlin/src/main/kotlin/io/temporal/kotlin/common/KPriority.kt

@JvmInline
public value class KPriority(public val value: Int) {
  public companion object {
    public val DEFAULT: KPriority = KPriority(0)
  }
}
```

**Tasks:**
- [ ] Create `KPriority` inline value class
- [ ] Add `toJava()` and `toKotlin()` conversions
- [ ] Update `KWorkflowOptions` to use `KPriority`
- [ ] Update `KWorkflowInfo` to return `KPriority`

---

### 6. Fix KScheduleActionStartWorkflow (High Priority)

**Issue:** Uses Java `WorkflowOptions` instead of `KWorkflowOptions`

**Current:**
```kotlin
public data class KScheduleActionStartWorkflow(
  val workflowType: String,
  val options: WorkflowOptions,  // Java type!
  val arguments: List<*> = emptyList<Any>(),
  val header: Header = Header.empty()
)
```

**Target:**
```kotlin
public data class KScheduleActionStartWorkflow(
  val workflowType: String,
  val options: KWorkflowOptions,  // Kotlin type
  val arguments: List<*> = emptyList<Any>()
  // Remove header - it's internal interceptor plumbing
)
```

**Tasks:**
- [ ] Update `KScheduleActionStartWorkflow` to use `KWorkflowOptions`
- [ ] Remove `header` property (or make internal) - this is interceptor plumbing
- [ ] Update `fromWorkflowInterface` factory methods
- [ ] Update `KScheduleConverters.toJava()` to convert `KWorkflowOptions` to `WorkflowOptions`
- [ ] Update `JavaInterop.toKotlin()` for `ScheduleActionStartWorkflow`
- [ ] Update tests

---

## Implementation Order

1. **Phase 1: Fix KScheduleActionStartWorkflow** (standalone fix)
   - Update to use KWorkflowOptions
   - Update converters and tests

2. **Phase 2: Simple Types**
   - KActivityCancellationType (enum)
   - KPriority (value class)
   - KRetryOptions (data class)

3. **Phase 3: Complex Types**
   - KWorkflowImplementationOptions (depends on KActivityOptions)

4. **Phase 4: Client Interceptor**
   - KWorkflowClientInterceptor
   - KWorkflowClientCallsInterceptor
   - All input/output types
   - toJava()/toKotlin() interop extensions

---

## Files to Create

```
temporal-kotlin/src/main/kotlin/io/temporal/kotlin/
├── activity/
│   └── KActivityCancellationType.kt
├── common/
│   ├── KPriority.kt
│   └── KRetryOptions.kt
├── interceptor/
│   ├── KWorkflowClientInterceptor.kt
│   └── KWorkflowClientCallsInterceptor.kt
└── worker/
    └── KWorkflowImplementationOptions.kt
```

## Files to Modify

- `KScheduleActionStartWorkflow.kt` - Use KWorkflowOptions
- `KActivityOptions.kt` - Use KActivityCancellationType
- `KWorkerOptions.kt` - Use KWorkflowImplementationOptions
- `KWorkflowOptions.kt` - Use KPriority
- `KWorkflowInfo.kt` - Use KRetryOptions, KPriority
- `KClientOptions.kt` - Use KWorkflowClientInterceptor
- `JavaInterop.kt` - Add toKotlin() extensions
- `KOptionsConverters.kt` - Add toJava() methods
- `KScheduleConverters.kt` - Update for KWorkflowOptions

## Testing

- Unit tests for each new type's conversion (toJava/toKotlin)
- Integration tests for interceptor chain with suspend functions
- Update existing tests that use Java types
- Test interop: Java interceptor wrapped as Kotlin, Kotlin interceptor wrapped as Java
