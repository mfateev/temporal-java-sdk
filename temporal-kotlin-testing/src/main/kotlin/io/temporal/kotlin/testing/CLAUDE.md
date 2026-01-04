# Kotlin Testing Module Design Decisions

## Activity Mocking Architecture

### DynamicActivity Pattern for Runtime Mock Registration

The mocking system uses Temporal's `DynamicActivity` interface to intercept all activity calls at runtime. This design allows `testEnv.registerActivitiesImplementations()` to be called at any time during test execution, even after the test environment has started.

**Why not register mocks directly with the worker?**
- Worker activity registration happens before `start()` is called
- Tests often need to configure mocks after obtaining workflow stubs
- The dynamic handler acts as a catch-all that routes to the mock registry

**Implementation:** `KMockDynamicActivityHandler` is automatically registered by `KTestWorkflowExtension` and delegates to `KActivityMockRegistry`.

### Non-Retryable Failures for Unregistered Activities

When an activity type has no registered mock or implementation, we throw `ApplicationFailure.newNonRetryableFailure` instead of a regular exception.

**Why?**
- Regular exceptions (like `IllegalArgumentException`) are retryable by default
- Without this, tests would hang for 30+ seconds waiting for retry exhaustion
- Fast failure provides immediate feedback about missing mocks

### Unified API for Mocks and Real Implementations

`registerActivitiesImplementations()` accepts both Mockito mocks and real activity implementations. The system extracts `@ActivityInterface` metadata from whatever is passed.

**Why a single method?**
- Reduces API surface area
- Same registration flow regardless of whether using mocks or real implementations
- Allows gradual migration from mocks to real implementations in tests

## Suspend Activity Considerations

### Suspend Activities in Workflows

Workflows cannot directly call suspend functions. When a workflow calls an activity defined with `suspend fun`, the call goes through a Java dynamic proxy that doesn't understand Kotlin coroutines.

**Implication:** From the workflow's perspective, all activity calls are blocking. The suspend nature only matters during activity execution on the worker side.

### Suspend Mock Invocation

`KMockDynamicActivityHandler` detects suspend functions by checking for a `Continuation` parameter and uses `runBlocking` with Kotlin reflection (`callSuspend`) to invoke them.

**Why runBlocking?**
- Activity execution happens on worker threads, not coroutine contexts
- The worker expects a blocking result from `DynamicActivity.execute()`
- `runBlocking` bridges the suspend function to the blocking world

## Mock Registry Lifecycle

The `KActivityMockRegistry` is:
1. Created by `KTestWorkflowExtension.beforeEach()`
2. Passed to both `KTestWorkflowEnvironment` (for `registerActivitiesImplementations()`)
3. Passed to `KMockDynamicActivityHandler` (for activity dispatch)

This ensures the same registry instance is shared, so mocks registered via the environment are immediately available to the handler.
