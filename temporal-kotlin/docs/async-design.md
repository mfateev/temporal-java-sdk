# Kotlin SDK Concurrency Design

## Design Principle

**Use idiomatic Kotlin language patterns wherever possible instead of custom APIs.**

The Kotlin SDK should feel natural to Kotlin developers by leveraging standard `kotlinx.coroutines`
primitives. Custom APIs should only be introduced when Temporal-specific semantics cannot be
achieved through standard patterns.

## Standard Kotlin Patterns to Use

| Pattern | Standard Kotlin | Temporal Integration |
|---------|-----------------|----------------------|
| Parallel execution | `coroutineScope { async { ... } }` | Works via deterministic dispatcher |
| Await multiple | `awaitAll(d1, d2)` | Standard kotlinx.coroutines |
| Sleep/delay | `delay(duration)` | Intercepted via `Delay` interface |
| Deferred results | `Deferred<T>` | `Promise<T>.toDeferred()` |

## Implementation Strategy

### 1. Deterministic Delay via Dispatcher

The `KotlinCoroutineDispatcher` implements the `Delay` interface to intercept standard
`kotlinx.coroutines.delay()` calls and route them through Temporal's deterministic timer:

```kotlin
class KotlinCoroutineDispatcher(...) : CoroutineDispatcher(), Delay {
    override fun scheduleResumeAfterDelay(
        timeMillis: Long,
        continuation: CancellableContinuation<Unit>
    ) {
        // Schedule Temporal timer instead of Thread.sleep
        workflowContext.scheduleTimer(timeMillis) {
            continuation.resume(Unit)
        }
    }
}
```

**Result:** Users write standard `delay(1.seconds)` and it's automatically deterministic.

### 2. Promise to Deferred Conversion

Provide an extension to convert Java SDK `Promise<T>` to standard `Deferred<T>`:

```kotlin
fun <T> Promise<T>.toDeferred(): Deferred<T>
```

This allows Java SDK promises to work with all standard coroutine utilities like `awaitAll()`.

### 3. Standard Async for Parallel Execution

Instead of custom `KWorkflow.async` or `startActivity`, use standard coroutines:

```kotlin
// Parallel execution - standard Kotlin
coroutineScope {
    val d1 = async { KWorkflow.executeActivity<Int>("Add", options, 1, 2) }
    val d2 = async { KWorkflow.executeActivity<Int>("Add", options, 3, 4) }
    val results = awaitAll(d1, d2)  // Standard kotlinx.coroutines.awaitAll
}
```

## API Surface

### Keep (Temporal-specific semantics)

| API | Reason |
|-----|--------|
| `KWorkflow.executeActivity<R>(...)` | Suspend function for activity execution |
| `KWorkflow.executeLocalActivity<R>(...)` | Suspend function for local activity |
| `KWorkflow.executeChildWorkflow<R>(...)` | Suspend function for child workflow |
| `KWorkflow.condition { }` | Temporal-specific blocking pattern |
| `KWorkflow.getInfo()` | Workflow metadata access |
| `KWorkflow.getVersion(...)` | Temporal versioning |
| `KWorkflow.sideEffect { }` | Non-deterministic operations |
| Signal/Query registration | Temporal-specific handlers |

### Remove (redundant with standard Kotlin)

| API | Replacement |
|-----|-------------|
| `KWorkflow.delay(...)` | Standard `delay()` (intercepted) |
| `KWorkflow.async { }` | Standard `coroutineScope { async { } }` |
| `KWorkflow.startActivity(...)` | `async { executeActivity(...) }` |
| `KWorkflow.startLocalActivity(...)` | `async { executeLocalActivity(...) }` |
| `KWorkflow.startChildWorkflow(...)` | `async { executeChildWorkflow(...) }` |
| `KActivityHandle<R>` | Standard `Deferred<T>` |
| `KChildWorkflowHandle<R>` | Standard `Deferred<T>` |
| `Promise<T>.await()` | `Promise<T>.toDeferred().await()` |

### Add

| API | Purpose |
|-----|---------|
| `Promise<T>.toDeferred()` | Bridge Java promises to standard Deferred |
| `Delay` implementation | Intercept standard delay() for determinism |

## Usage Examples

### Sequential Execution

```kotlin
override suspend fun execute(): String {
    val result1 = KWorkflow.executeActivity<Int>("Add", options, 1, 2)
    delay(5.seconds)  // Standard Kotlin, deterministic!
    val result2 = KWorkflow.executeActivity<Int>("Add", options, 3, 4)
    return "Sum: ${result1 + result2}"
}
```

### Parallel Execution

```kotlin
override suspend fun execute(): Int {
    return coroutineScope {
        val d1 = async { KWorkflow.executeActivity<Int>("Add", options, 10, 20) }
        val d2 = async { KWorkflow.executeActivity<Int>("Add", options, 5, 15) }
        val d3 = async { KWorkflow.executeActivity<Int>("Add", options, 100, 200) }

        // Standard kotlinx.coroutines.awaitAll
        val results = awaitAll(d1, d2, d3)
        results.sum()  // 350
    }
}
```

### Mixed Sequential and Parallel

```kotlin
override suspend fun execute(): String {
    // Start long-running task in background
    val backgroundResult = coroutineScope {
        val task = async {
            KWorkflow.executeActivity<String>("SlowOperation", options)
        }

        // Do quick work while slow operation runs
        val quickResult = KWorkflow.executeActivity<String>("QuickOperation", options)

        // Wait for background task
        "$quickResult + ${task.await()}"
    }
    return backgroundResult
}
```

### Working with Java SDK Promises

```kotlin
// When interacting with Java SDK APIs that return Promise
val javaPromise: Promise<String> = someJavaApi()
val deferred = javaPromise.toDeferred()

// Now works with all standard coroutine utilities
coroutineScope {
    val d1 = async { KWorkflow.executeActivity<Int>("Op1", options) }
    val d2 = javaPromise.toDeferred()
    awaitAll(d1, d2)
}
```

## Determinism Guarantees

All standard Kotlin coroutine patterns remain deterministic because:

1. **Dispatcher inheritance**: All coroutines inherit the workflow's deterministic dispatcher
2. **Delay interception**: Standard `delay()` routes through Temporal's timer
3. **Single-threaded execution**: FIFO queue ensures consistent ordering
4. **Replay safety**: Same execution order during replay

## Benefits of This Approach

1. **Familiar patterns**: Kotlin developers use patterns they already know
2. **IDE support**: Full autocomplete and documentation for standard APIs
3. **Ecosystem compatibility**: Works with existing coroutine libraries and utilities
4. **Smaller API surface**: Less custom code to learn and maintain
5. **Future-proof**: Benefits from kotlinx.coroutines improvements automatically
