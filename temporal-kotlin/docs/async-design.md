# KWorkflow.async Design Document

## Overview

This document describes the design for `KWorkflow.async {}` - a mechanism for eager parallel
execution of workflow operations following standard Kotlin coroutines patterns.

## Problem Statement

The current `startActivity()` / `startChildWorkflow()` methods use **deferred/lazy execution**:
- Activity doesn't start until `await()` is called
- `isCompleted` is only true after `await()` completes
- Cannot use `KWorkflow.condition { handle.isCompleted }` patterns
- Not idiomatic Kotlin - standard `async {}` is eager

## Solution

Add `KWorkflow.async` that launches a coroutine in the workflow context and returns a `KDeferred<T>`:

```kotlin
// Usage example
val handle = KWorkflow.async {
  executeActivity<String>("Greet", options, "World")
}
// Activity is now running
KWorkflow.condition { handle.isCompleted }  // Works!
val result = handle.await()
```

## API Design

### KDeferred Interface

```kotlin
interface KDeferred<T> {
  /** Returns true if this deferred has completed (successfully or exceptionally) */
  val isCompleted: Boolean

  /** Returns true if this deferred was cancelled */
  val isCancelled: Boolean

  /** Awaits completion and returns the result or throws the exception */
  suspend fun await(): T

  /** Returns the completion exception, or null if completed successfully or not yet completed */
  fun getCompletionExceptionOrNull(): Throwable?
}
```

### KWorkflow.async Method

```kotlin
object KWorkflow {
  /**
   * Launches a coroutine in the workflow context and returns immediately.
   *
   * The block starts executing immediately (eager execution).
   * Returns a KDeferred that can be used to await the result.
   *
   * Example:
   * ```kotlin
   * val handle = KWorkflow.async {
   *   executeActivity<String>("Greet", options, "World")
   * }
   * // Activity is now running
   * val result = handle.await()
   * ```
   */
  fun <T> async(block: suspend () -> T): KDeferred<T>
}
```

## Usage Patterns

### Parallel Activity Execution

```kotlin
override suspend fun execute(): String {
  val options = ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofSeconds(10))
    .build()

  // Start activities in parallel
  val handle1 = KWorkflow.async {
    KWorkflow.executeActivity<Int>("Add", options, 10, 20)
  }
  val handle2 = KWorkflow.async {
    KWorkflow.executeActivity<Int>("Add", options, 5, 15)
  }
  val handle3 = KWorkflow.async {
    KWorkflow.executeActivity<Int>("Add", options, 100, 200)
  }

  // All three activities are now running in parallel
  // Await results
  val sum = handle1.await() + handle2.await() + handle3.await()
  return "Sum: $sum"  // Sum: 350
}
```

### Condition Waiting on Async Completion

```kotlin
override suspend fun execute(): String {
  var activityResult: String? = null

  // Start activity asynchronously
  val handle = KWorkflow.async {
    activityResult = KWorkflow.executeActivity<String>("Greet", options, "World")
    activityResult
  }

  // Wait for condition - works because isCompleted updates when coroutine finishes
  KWorkflow.condition { handle.isCompleted }

  return "Got: $activityResult"
}
```

### Mixed Async and Sequential

```kotlin
override suspend fun execute(): String {
  // Start long-running activity in background
  val backgroundTask = KWorkflow.async {
    KWorkflow.executeActivity<String>("SlowOperation", options, 5000)
  }

  // Do other work while it runs
  val quickResult = KWorkflow.executeActivity<String>("QuickOperation", options)

  // Now wait for background task
  val slowResult = backgroundTask.await()

  return "$quickResult + $slowResult"
}
```

## Implementation Notes

### Deterministic Execution

The `async` block executes within the workflow's deterministic dispatcher. This ensures:
- Replay safety - same execution order during replay
- No threading issues - all coroutines run on the workflow thread
- Deadlock detection still works

### Condition Notification

When a `KDeferred` completes, it triggers the dispatcher's condition notification mechanism,
allowing any coroutines waiting on `KWorkflow.condition { deferred.isCompleted }` to wake up.

### Comparison with kotlinx.coroutines.async

| Feature | kotlinx.coroutines.async | KWorkflow.async |
|---------|--------------------------|-----------------|
| Eager execution | Yes | Yes |
| Returns | Deferred<T> | KDeferred<T> |
| isCompleted | Yes | Yes |
| await() | Yes | Yes |
| cancel() | Yes | Not yet (future) |
| CoroutineScope receiver | Yes | No (uses workflow context) |

## Future Work

- `awaitAll(vararg deferred)` - wait for multiple deferreds
- `awaitAny(vararg deferred)` - wait for first completion
- Cancellation support via `CancellationScope`
- `supervisorAsync {}` for independent failure handling
