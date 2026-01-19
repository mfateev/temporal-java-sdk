# KArgs Implementation Plan

## Overview

This plan implements the `kargs()` type-safe argument wrapper pattern for the Kotlin SDK, changing the argument order from `(method, options, arg1, arg2, ...)` to `(method, arg/kargs, options)`.

## Argument Pattern Summary

| Arguments | Current Pattern | New Pattern |
|-----------|----------------|-------------|
| 0 args | `(method, options)` | `(method, options)` (unchanged) |
| 1 arg | `(method, options, arg)` | `(method, arg, options)` |
| 2+ args | `(method, options, arg1, arg2, ...)` | `(method, kargs(arg1, arg2, ...), options)` |

## Phase 1: Create KArgs Foundation

### 1.1 Create `io/temporal/kotlin/common/KArgs.kt`

Create the KArgs data classes and factory functions:

```kotlin
package io.temporal.kotlin.common

/**
 * Marker interface for type-safe multi-argument wrappers.
 *
 * Use `kargs()` factory functions to create instances.
 */
public sealed interface KArgs {
    /**
     * Returns the arguments as an array for internal use.
     */
    public fun toArray(): Array<Any?>
}

public data class KArgs2<A1, A2>(val a1: A1, val a2: A2) : KArgs {
    override fun toArray(): Array<Any?> = arrayOf(a1, a2)
}

public data class KArgs3<A1, A2, A3>(val a1: A1, val a2: A2, val a3: A3) : KArgs {
    override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3)
}

public data class KArgs4<A1, A2, A3, A4>(val a1: A1, val a2: A2, val a3: A3, val a4: A4) : KArgs {
    override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4)
}

public data class KArgs5<A1, A2, A3, A4, A5>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5) : KArgs {
    override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4, a5)
}

public data class KArgs6<A1, A2, A3, A4, A5, A6>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6) : KArgs {
    override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4, a5, a6)
}

// Factory functions
public fun <A1, A2> kargs(a1: A1, a2: A2): KArgs2<A1, A2> = KArgs2(a1, a2)
public fun <A1, A2, A3> kargs(a1: A1, a2: A2, a3: A3): KArgs3<A1, A2, A3> = KArgs3(a1, a2, a3)
public fun <A1, A2, A3, A4> kargs(a1: A1, a2: A2, a3: A3, a4: A4): KArgs4<A1, A2, A3, A4> = KArgs4(a1, a2, a3, a4)
public fun <A1, A2, A3, A4, A5> kargs(a1: A1, a2: A2, a3: A3, a4: A4, a5: A5): KArgs5<A1, A2, A3, A4, A5> = KArgs5(a1, a2, a3, a4, a5)
public fun <A1, A2, A3, A4, A5, A6> kargs(a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6): KArgs6<A1, A2, A3, A4, A5, A6> = KArgs6(a1, a2, a3, a4, a5, a6)
```

### 1.2 Create Unit Tests for KArgs

Create `io/temporal/kotlin/common/KArgsTest.kt`:
- Test all KArgs data classes
- Test toArray() conversions
- Test type inference with kargs() factory functions

---

## Phase 2: Modify KWorkflow.kt (Activity Execution)

### 2.1 Activity Execution - Typed Methods

**Current (0 args - unchanged):**
```kotlin
public suspend fun <T, R> executeActivity(
    activity: KFunction1<T, R>,
    options: KActivityOptions
): R
```

**Current (1 arg):**
```kotlin
public suspend fun <T, A1, R> executeActivity(
    activity: KFunction2<T, A1, R>,
    options: KActivityOptions,
    arg1: A1
): R
```

**New (1 arg):**
```kotlin
public suspend fun <T, A1, R> executeActivity(
    activity: KFunction2<T, A1, R>,
    arg: A1,
    options: KActivityOptions
): R
```

**Current (2 args):**
```kotlin
public suspend fun <T, A1, A2, R> executeActivity(
    activity: KFunction3<T, A1, A2, R>,
    options: KActivityOptions,
    arg1: A1,
    arg2: A2
): R
```

**New (2 args via kargs):**
```kotlin
public suspend fun <T, A1, A2, R> executeActivity(
    activity: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KActivityOptions
): R
```

**Similarly for 3-6 args** using KArgs3-KArgs6.

### 2.2 Activity Execution - Suspend Function Methods

Apply the same pattern change to all `KSuspendFunction*` overloads.

### 2.3 Local Activity Execution

Apply the same pattern change to all `executeLocalActivity` methods.

### 2.4 Untyped Activity Execution

**Current:**
```kotlin
public suspend inline fun <reified R> executeActivity(
    activityName: String,
    options: KActivityOptions,
    vararg args: Any?
): R
```

**New (0 args):**
```kotlin
public suspend inline fun <reified R> executeActivity(
    activityName: String,
    options: KActivityOptions
): R
```

**New (1 arg):**
```kotlin
public suspend inline fun <reified R> executeActivity(
    activityName: String,
    arg: Any?,
    options: KActivityOptions
): R
```

**New (2+ args via kargs):**
```kotlin
public suspend inline fun <reified R> executeActivity(
    activityName: String,
    args: KArgs,
    options: KActivityOptions
): R
```

---

## Phase 3: Modify KWorkflow.kt (Child Workflow Execution)

### 3.1 Child Workflow Execution - Typed Methods

Apply the same pattern as activities:
- 0 args: `(workflow, options)` - unchanged
- 1 arg: `(workflow, arg, options)` - reorder
- 2+ args: `(workflow, kargs(...), options)` - use KArgs

### 3.2 Child Workflow Execution - Suspend Function Methods

Apply the same pattern to all `KSuspendFunction*` overloads.

### 3.3 Untyped Child Workflow Execution

Apply the same pattern as untyped activity execution.

---

## Phase 4: Modify KWorkflow.kt (Start Child Workflow)

Apply the same patterns to `startChildWorkflow` methods.

---

## Phase 5: Modify KClient.kt (Client Workflow Operations)

### 5.1 startWorkflow Methods

**Current (1 arg):**
```kotlin
public suspend fun <T, A1, R> startWorkflow(
    workflow: KFunction2<T, A1, R>,
    options: KWorkflowOptions,
    arg1: A1
): KTypedWorkflowHandle<T, R>
```

**New (1 arg):**
```kotlin
public suspend fun <T, A1, R> startWorkflow(
    workflow: KFunction2<T, A1, R>,
    arg: A1,
    options: KWorkflowOptions
): KTypedWorkflowHandle<T, R>
```

**Current (2 args):**
```kotlin
public suspend fun <T, A1, A2, R> startWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    options: KWorkflowOptions,
    arg1: A1,
    arg2: A2
): KTypedWorkflowHandle<T, R>
```

**New (2 args via kargs):**
```kotlin
public suspend fun <T, A1, A2, R> startWorkflow(
    workflow: KFunction3<T, A1, A2, R>,
    args: KArgs2<A1, A2>,
    options: KWorkflowOptions
): KTypedWorkflowHandle<T, R>
```

### 5.2 executeWorkflow Methods

Apply the same pattern to all `executeWorkflow` overloads.

### 5.3 Suspend Workflow Method Overloads

Apply the same pattern to all `KSuspendFunction*` overloads.

### 5.4 signalWithStart Methods

Apply the same pattern for both workflow args and signal args.

### 5.5 newWithStartWorkflowOperation Methods

Apply the same pattern.

### 5.6 startUpdateWithStart / executeUpdateWithStart Methods

Apply the same pattern for update arguments.

---

## Phase 6: Modify KWorkflowHandle.kt

### 6.1 Signal Methods

**Current (1 arg):**
```kotlin
public suspend fun <A> signal(signal: KFunction2<T, A, Unit>, arg: A)
```

**New (1 arg) - unchanged** (signals have no options parameter, so pattern stays the same)

Actually, signals don't have options, so only the pattern `(method, arg)` applies. For 2+ args, we could add:

```kotlin
public suspend fun <A1, A2> signal(signal: KFunction3<T, A1, A2, Unit>, args: KArgs2<A1, A2>)
```

### 6.2 Query Methods

**Current (1 arg):**
```kotlin
public suspend fun <A, R> query(query: KFunction2<T, A, R>, arg: A): R
```

Same consideration as signals - queries have no options.

### 6.3 Update Methods

**Current (1 arg):**
```kotlin
public suspend fun <A, R> executeUpdate(update: KFunction2<T, A, R>, arg: A): R
```

Updates could have options (updateId, etc.), so pattern would be:
- 1 arg: `(update, arg, options?)` - with optional/default options
- 2+ args: `(update, kargs(...), options?)`

---

## Phase 7: Modify KChildWorkflowHandle.kt

### 7.1 Signal Methods

Apply kargs pattern for 2+ arg signals:

```kotlin
public suspend fun <A1, A2> signal(signal: KFunction3<T, A1, A2, Unit>, args: KArgs2<A1, A2>)
```

---

## Phase 8: Modify KExternalWorkflowHandle.kt

### 8.1 Signal Methods

Apply kargs pattern for 2+ arg signals. Currently has methods up to 6 args with positional parameters. Change to kargs for 2+ args.

---

## Test Requirements

### Test Files to Update (per phase)

| Phase | Test Files |
|-------|------------|
| 1 | `KArgsTest.kt` (new) |
| 2-3 | `WorkflowClientExtTest.kt`, `UpdateWithStartTest.kt`, client integration tests |
| 4 | Handle-related tests |
| 5-7 | `KWorkflowTest.kt`, `WorkflowApiIntegrationTest.kt`, `KotlinCoroutineFeaturesIntegrationTest.kt` |
| 8-9 | `KotlinAsyncChildWorkflowTest.kt`, external workflow tests |

### Test Coverage Requirements

- Test 0-arg methods (unchanged behavior)
- Test 1-arg methods with new `(method, arg, options)` signature
- Test 2+ arg methods with `kargs()` wrapper
- Test string-based/untyped APIs with new patterns
- Test type safety (compile-time verification via compilation)
- Test suspend function variants
- Integration tests for end-to-end validation

### Rules

1. **No tests may be disabled** - All existing tests must be updated to use new API
2. **No tests may be removed** - Test coverage must be maintained or improved
3. **Linting required** - `spotlessApply` before each commit
4. **All tests must pass** - Checkpoint validation after each phase

---

## Implementation Order

Each phase includes updating relevant tests. All tests must pass before moving to the next phase. No tests may be disabled or removed.

### Validation After Each Phase
```bash
./gradlew --offline spotlessApply
./gradlew --offline spotlessCheck
./gradlew :temporal-kotlin:test --offline
```

### Phase Sequence

1. **Phase 1**: Create KArgs foundation
   - Create `KArgs.kt` with data classes and factory functions
   - Create `KArgsTest.kt` with unit tests
   - Run tests to verify KArgs classes work correctly
   - **Checkpoint**: `./gradlew :temporal-kotlin:test --offline`

2. **Phase 2**: Modify `KClient.kt` - Client workflow start/execute
   - Update all `startWorkflow` methods (1-6 args)
   - Update all `executeWorkflow` methods (1-6 args)
   - Update suspend workflow variants
   - Update corresponding tests in `WorkflowClientExtTest.kt` and integration tests
   - **Checkpoint**: All client tests pass

3. **Phase 3**: Modify `KClient.kt` - Signal/Update with Start
   - Update `signalWithStart` methods
   - Update `newWithStartWorkflowOperation` methods
   - Update `startUpdateWithStart` / `executeUpdateWithStart` methods
   - Update corresponding tests (e.g., `UpdateWithStartTest.kt`)
   - **Checkpoint**: All client tests pass

4. **Phase 4**: Modify `KWorkflowHandle.kt`
   - Add kargs overloads for signal methods (2+ args)
   - Add kargs overloads for query methods (2+ args)
   - Add kargs overloads for update methods (2+ args)
   - Update handle-related tests
   - **Checkpoint**: All handle tests pass

5. **Phase 5**: Modify `KWorkflow.kt` - Activity Execution
   - Update all `executeActivity` typed methods (1-6 args)
   - Update all `executeActivity` suspend methods (1-6 args)
   - Update untyped `executeActivity` methods
   - Update activity-related tests in `KWorkflowTest.kt` and integration tests
   - **Checkpoint**: All activity tests pass

6. **Phase 6**: Modify `KWorkflow.kt` - Local Activity Execution
   - Update all `executeLocalActivity` typed methods (1-6 args)
   - Update all `executeLocalActivity` suspend methods (1-6 args)
   - Update local activity-related tests
   - **Checkpoint**: All local activity tests pass

7. **Phase 7**: Modify `KWorkflow.kt` - Child Workflow Execution
   - Update all `executeChildWorkflow` typed methods (1-6 args)
   - Update all `executeChildWorkflow` suspend methods (1-6 args)
   - Update `startChildWorkflow` methods
   - Update untyped child workflow methods
   - Update child workflow-related tests
   - **Checkpoint**: All child workflow tests pass

8. **Phase 8**: Modify `KChildWorkflowHandle.kt`
   - Add kargs overloads for signal methods (2+ args)
   - Update child workflow handle tests
   - **Checkpoint**: All child handle tests pass

9. **Phase 9**: Modify `KExternalWorkflowHandle.kt`
   - Replace positional signal methods (2-6 args) with kargs versions
   - Update external workflow handle tests
   - **Checkpoint**: All external handle tests pass

10. **Phase 10**: Final Validation
    - Run full test suite: `./gradlew test`
    - Run linting: `./gradlew spotlessCheck`
    - Verify all integration tests pass
    - **Final Checkpoint**: All tests green

---

## Migration Strategy

Since this is a new SDK (not yet released), we will make a **clean break** without deprecation:
- Remove old method signatures entirely
- Update all tests to use new signatures
- No backward compatibility shims needed

If backward compatibility is desired in the future, consider deprecation:

```kotlin
@Deprecated(
    message = "Use executeActivity(activity, arg, options) instead",
    replaceWith = ReplaceWith("executeActivity(activity, arg, options)")
)
public suspend fun <T, A1, R> executeActivity(
    activity: KFunction2<T, A1, R>,
    options: KActivityOptions,
    arg1: A1
): R = executeActivity(activity, arg1, options)
```

---

## Breaking Changes

This is a **breaking API change**. All code using 1+ argument methods will need to update:

**Before:**
```kotlin
KWorkflow.executeActivity(Activities::greet, options, name)
KWorkflow.executeActivity(Activities::compose, options, greeting, name)
```

**After:**
```kotlin
KWorkflow.executeActivity(Activities::greet, name, options)
KWorkflow.executeActivity(Activities::compose, kargs(greeting, name), options)
```

---

## Files Changed Summary

| File | Changes |
|------|---------|
| `io/temporal/kotlin/common/KArgs.kt` | **NEW** - KArgs classes and kargs() functions |
| `io/temporal/kotlin/workflow/KWorkflow.kt` | Reorder all 1+ arg methods |
| `io/temporal/kotlin/client/KClient.kt` | Reorder all 1+ arg methods |
| `io/temporal/kotlin/client/KWorkflowHandle.kt` | Add kargs overloads for signals/queries/updates |
| `io/temporal/kotlin/workflow/KChildWorkflowHandle.kt` | Add kargs overloads for signals |
| `io/temporal/kotlin/workflow/KExternalWorkflowHandle.kt` | Replace positional args with kargs |
| `io/temporal/kotlin/common/KArgsTest.kt` | **NEW** - Unit tests |
| Various test files | Update to new API patterns |

---

## Estimated Scope

| Category | Count |
|----------|-------|
| Method signatures in `KWorkflow.kt` | ~50+ |
| Method signatures in `KClient.kt` | ~50+ |
| Method signatures in handle classes | ~15+ |
| New files | 2 (`KArgs.kt`, `KArgsTest.kt`) |
| Test files to update | ~10-15 |
| Phases with checkpoints | 10 |

### Per-Phase Validation Commands

```bash
# Format code
./gradlew --offline spotlessApply

# Check formatting
./gradlew --offline spotlessCheck

# Run temporal-kotlin tests only (faster iteration)
./gradlew :temporal-kotlin:test --offline

# Run full test suite (final validation)
./gradlew test
```
