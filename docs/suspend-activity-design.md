# Kotlin Suspend Activity Support - Design Document

## Overview

This document describes the design for supporting Kotlin `suspend` functions as Temporal activities. The solution introduces a new `TypedDynamicActivity` interface in the Java SDK and leverages the existing manual activity completion mechanism to achieve non-blocking suspend function execution.

## Problem Statement

Kotlin suspend functions compile to Java methods with an additional `Continuation` parameter:

```kotlin
// Kotlin interface
@ActivityInterface
interface MyActivities {
    suspend fun processData(input: String): Result
}

// Compiled Java bytecode
interface MyActivities {
    Object processData(String input, Continuation<? super Result> continuation);
}
```

The current Java SDK cannot handle suspend activities because:

1. **Registration fails**: `POJOActivityImplementation.provideArgs()` uses `method.getParameterTypes()` which includes the `Continuation` parameter. When deserializing activity arguments, it expects 2 parameters but only receives 1 from the workflow.

2. **Invocation fails**: `method.invoke(activity, args)` requires a `Continuation` argument that the SDK doesn't provide.

## Solution: TypedDynamicActivity

### Core Concept

Introduce a per-activity-type dynamic activity interface that allows custom activity execution logic. Unlike the existing `DynamicActivity` (which handles ALL unregistered activity types), `TypedDynamicActivity` handles a specific activity type and is stored in the same `activities` map as regular activity executors.

### Java SDK Changes

#### New Interface

```java
package io.temporal.activity;

/**
 * A dynamic activity handler for a specific activity type.
 *
 * Unlike {@link DynamicActivity} which handles all unregistered activity types,
 * TypedDynamicActivity handles a specific activity type and can coexist with
 * regular activity registrations.
 *
 * This is useful for:
 * - Kotlin suspend activity support
 * - Custom activity invocation strategies
 * - Language-specific activity handling
 */
public interface TypedDynamicActivity {
    /**
     * Execute the activity.
     *
     * @param args Encoded activity arguments from the workflow
     * @return Activity result (will be serialized back to the workflow)
     */
    Object execute(EncodedValues args);

    /**
     * @return The activity type name this handler handles
     */
    String getActivityType();
}
```

#### New ActivityTaskExecutor Implementation

```java
package io.temporal.internal.activity;

/**
 * ActivityTaskExecutor that delegates to a TypedDynamicActivity.
 */
class TypedDynamicActivityExecutor implements ActivityTaskExecutors.ActivityTaskExecutor {
    private final TypedDynamicActivity dynamicActivity;
    private final DataConverter dataConverter;
    private final List<ContextPropagator> contextPropagators;
    private final WorkerInterceptor[] interceptors;
    private final ActivityExecutionContextFactory executionContextFactory;

    TypedDynamicActivityExecutor(
        TypedDynamicActivity dynamicActivity,
        DataConverter dataConverter,
        List<ContextPropagator> contextPropagators,
        WorkerInterceptor[] interceptors,
        ActivityExecutionContextFactory executionContextFactory
    ) {
        this.dynamicActivity = dynamicActivity;
        this.dataConverter = dataConverter;
        this.contextPropagators = contextPropagators;
        this.interceptors = interceptors;
        this.executionContextFactory = executionContextFactory;
    }

    @Override
    public ActivityTaskHandler.Result execute(ActivityInfoInternal info, Scope metricsScope) {
        // Create execution context
        InternalActivityExecutionContext context =
            executionContextFactory.createContext(info, dynamicActivity, metricsScope);

        // Set up context propagators
        info.getHeader().ifPresent(header ->
            deserializeAndPopulateContext(header, contextPropagators));

        // Build interceptor chain (for tracing, etc.)
        ActivityInboundCallsInterceptor inboundCallsInterceptor =
            new TypedDynamicActivityInboundCallsInterceptor(dynamicActivity);
        for (WorkerInterceptor interceptor : interceptors) {
            inboundCallsInterceptor = interceptor.interceptActivity(inboundCallsInterceptor);
        }
        inboundCallsInterceptor.init(context);

        try {
            // Create EncodedValues from input payloads
            EncodedValues encodedValues = new EncodedValuesImpl(
                info.getInput(), dataConverter);

            // Execute through interceptor chain
            ActivityOutput result = inboundCallsInterceptor.execute(
                new ActivityInput(info.getHeader().orElse(null), new Object[]{encodedValues}));

            // Handle result
            if (context.isDoNotCompleteOnReturn()) {
                return new ActivityTaskHandler.Result(
                    info.getActivityId(), null, null, null,
                    context.isUseLocalManualCompletion());
            }

            return constructResult(info, result, dataConverter);
        } catch (Throwable e) {
            return mapToActivityFailure(e, info, metricsScope, dataConverter);
        }
    }
}
```

#### Modify ActivityTaskHandlerImpl.registerActivityImplementation()

```java
private void registerActivityImplementation(Object activity) {
    if (activity instanceof Class) {
        throw new IllegalArgumentException("Activity object instance expected, not the class");
    }

    if (activity instanceof DynamicActivity) {
        // Existing: single global dynamic activity handler
        if (dynamicActivity != null) {
            throw new TypeAlreadyRegisteredException(
                "DynamicActivity",
                "An implementation of DynamicActivity is already registered with the worker");
        }
        dynamicActivity = new ActivityTaskExecutors.DynamicActivityImplementation(
            (DynamicActivity) activity,
            dataConverter,
            contextPropagators,
            interceptors,
            executionContextFactory);

    } else if (activity instanceof TypedDynamicActivity) {
        // NEW: per-type dynamic activity handler
        TypedDynamicActivity typedDynamic = (TypedDynamicActivity) activity;
        String typeName = typedDynamic.getActivityType();
        if (activities.containsKey(typeName)) {
            throw new TypeAlreadyRegisteredException(
                typeName,
                "\"" + typeName + "\" activity type is already registered with the worker");
        }
        activities.put(typeName, new TypedDynamicActivityExecutor(
            typedDynamic,
            dataConverter,
            contextPropagators,
            interceptors,
            executionContextFactory));

    } else {
        // Existing: POJO activity registration
        Class<?> cls = activity.getClass();
        POJOActivityImplMetadata activityImplMetadata = POJOActivityImplMetadata.newInstance(cls);
        for (POJOActivityMethodMetadata activityMetadata :
                activityImplMetadata.getActivityMethods()) {
            // ... existing code ...
        }
    }
}
```

### Kotlin SDK Implementation

#### KWorker.registerActivities()

```kotlin
class KWorker(
    val worker: Worker,
    private val activityDispatcher: CoroutineDispatcher = Dispatchers.Default,
) {

    fun registerActivities(vararg activities: Any) {
        for (activity in activities) {
            registerActivity(activity)
        }
    }

    private fun registerActivity(activity: Any) {
        val implClass = activity::class.java

        // Use Java SDK metadata to find activity interfaces and methods
        val activityInterfaces = findActivityInterfaces(implClass)

        for (activityInterface in activityInterfaces) {
            val metadata = POJOActivityInterfaceMetadata.newInstance(activityInterface)

            for (methodMetadata in metadata.methodsMetadata) {
                val method = methodMetadata.method
                val activityType = methodMetadata.activityTypeName

                // Create TypedDynamicActivity wrapper for each method
                val wrapper = KotlinActivityWrapper(
                    activityType = activityType,
                    implementation = activity,
                    method = method,
                    dispatcher = activityDispatcher
                )

                // Register with Java worker
                worker.registerActivitiesImplementations(wrapper)
            }
        }
    }

    private fun findActivityInterfaces(clazz: Class<*>): List<Class<*>> {
        // Recursively find all interfaces annotated with @ActivityInterface
        // (reuse existing logic from KActivityRegistry or similar)
    }
}
```

#### KotlinActivityWrapper

```kotlin
package io.temporal.kotlin.activity

import io.temporal.activity.Activity
import io.temporal.activity.TypedDynamicActivity
import io.temporal.common.converter.EncodedValues
import kotlinx.coroutines.*
import java.lang.reflect.Method
import kotlin.coroutines.Continuation
import kotlin.reflect.jvm.kotlinFunction

/**
 * Wraps a Kotlin activity method (suspend or regular) as a TypedDynamicActivity.
 *
 * For suspend methods: Uses useLocalManualCompletion() to achieve non-blocking execution.
 * For regular methods: Invokes directly and returns the result.
 */
internal class KotlinActivityWrapper(
    private val activityType: String,
    private val implementation: Any,
    private val method: Method,
    private val dispatcher: CoroutineDispatcher
) : TypedDynamicActivity {

    private val isSuspend: Boolean = isSuspendMethod(method)
    private val kFunction = method.kotlinFunction

    // Parameter types excluding Continuation for suspend methods
    private val parameterTypes: Array<Class<*>> = if (isSuspend) {
        method.parameterTypes.dropLast(1).toTypedArray()
    } else {
        method.parameterTypes
    }

    private val genericParameterTypes: Array<java.lang.reflect.Type> = if (isSuspend) {
        method.genericParameterTypes.dropLast(1).toTypedArray()
    } else {
        method.genericParameterTypes
    }

    override fun getActivityType(): String = activityType

    override fun execute(args: EncodedValues): Any? {
        // Decode arguments (excluding Continuation parameter)
        val decodedArgs = decodeArgs(args)

        return if (isSuspend) {
            executeSuspend(decodedArgs)
        } else {
            executeRegular(decodedArgs)
        }
    }

    private fun decodeArgs(encodedValues: EncodedValues): Array<Any?> {
        if (parameterTypes.isEmpty()) {
            return emptyArray()
        }
        return Array(parameterTypes.size) { index ->
            encodedValues.get(index, parameterTypes[index], genericParameterTypes[index])
        }
    }

    private fun executeRegular(args: Array<Any?>): Any? {
        return kFunction?.call(implementation, *args)
            ?: method.invoke(implementation, *args)
    }

    private fun executeSuspend(args: Array<Any?>): Any? {
        val context = Activity.getExecutionContext()
        val completionClient = context.useLocalManualCompletion()

        val job = SupervisorJob()
        val scope = CoroutineScope(dispatcher + job)

        scope.launch {
            // Set up coroutine context for KActivity.heartbeat() support
            val suspendContext = SuspendActivityContext(context, completionClient, job)
            val threadContextElement = SuspendActivityThreadContextElement(
                SuspendActivityExecutionContext(context, completionClient)
            )

            withContext(SuspendActivityContextElement(suspendContext) + threadContextElement) {
                try {
                    val result = kFunction!!.callSuspend(implementation, *args)
                    withContext(Dispatchers.IO) {
                        completionClient.complete(result)
                    }
                } catch (e: CancellationException) {
                    withContext(Dispatchers.IO + NonCancellable) {
                        completionClient.reportCancellation(null)
                    }
                } catch (e: Throwable) {
                    withContext(Dispatchers.IO + NonCancellable) {
                        completionClient.fail(e)
                    }
                }
            }
        }

        // Return null - actual result sent via completion client
        return null
    }

    companion object {
        private fun isSuspendMethod(method: Method): Boolean {
            val params = method.parameterTypes
            return params.isNotEmpty() &&
                   Continuation::class.java.isAssignableFrom(params.last())
        }
    }
}
```

## Execution Flow

### Registration Flow

```
KWorker.registerActivities(MyActivitiesImpl())
  │
  ├─► Use POJOActivityInterfaceMetadata to extract activity methods
  │
  ├─► For each method:
  │     ├─► Get activity type name (handles @ActivityMethod.name() and prefix)
  │     ├─► Detect if suspend (check for Continuation parameter)
  │     └─► Create KotlinActivityWrapper
  │
  └─► worker.registerActivitiesImplementations(wrapper)
        │
        └─► ActivityTaskHandlerImpl.registerActivityImplementation()
              │
              └─► instanceof TypedDynamicActivity
                    │
                    └─► activities.put(typeName, TypedDynamicActivityExecutor)
```

### Execution Flow (Suspend Activity)

```
Activity Task Received
  │
  └─► ActivityTaskHandlerImpl.handle()
        │
        └─► activities.get(activityType)  // Returns TypedDynamicActivityExecutor
              │
              └─► TypedDynamicActivityExecutor.execute()
                    │
                    └─► KotlinActivityWrapper.execute()
                          │
                          ├─► Decode args (exclude Continuation)
                          │
                          ├─► context.useLocalManualCompletion()
                          │
                          ├─► Launch coroutine on dispatcher
                          │     │
                          │     └─► kFunction.callSuspend(impl, *args)
                          │           │
                          │           ├─► On success: completionClient.complete(result)
                          │           ├─► On cancel: completionClient.reportCancellation()
                          │           └─► On error: completionClient.fail(exception)
                          │
                          └─► Return null (Result sent via completion client)
```

### Execution Flow (Non-Suspend Activity)

```
Activity Task Received
  │
  └─► ActivityTaskHandlerImpl.handle()
        │
        └─► activities.get(activityType)  // Returns TypedDynamicActivityExecutor
              │
              └─► TypedDynamicActivityExecutor.execute()
                    │
                    └─► KotlinActivityWrapper.execute()
                          │
                          ├─► Decode args
                          │
                          └─► kFunction.call(impl, *args)
                                │
                                └─► Return result directly
```

## Key Design Decisions

### 1. All Kotlin activities use TypedDynamicActivity

Both suspend and non-suspend Kotlin activities are registered as `TypedDynamicActivity`. This:
- Avoids Java SDK trying to parse suspend method signatures
- Provides uniform handling for all Kotlin activities
- Simplifies the registration logic

### 2. Reuse existing Java metadata classes

`POJOActivityInterfaceMetadata` and `POJOActivityMethodMetadata` work correctly for:
- Extracting activity type names
- Handling `@ActivityMethod` annotations and `namePrefix`
- Traversing interface hierarchy

No changes needed to these classes.

### 3. Manual completion for suspend activities

Using `useLocalManualCompletion()` provides:
- Non-blocking execution (worker thread freed immediately)
- Proper slot/permit management (respects `maxConcurrentActivityExecutionSize`)
- Clean integration with existing SDK infrastructure

### 4. Interceptor chain preserved

`TypedDynamicActivityExecutor` builds the interceptor chain, ensuring:
- Tracing interceptors work
- Custom user interceptors work
- Context propagation works

## Files to Modify/Create

### Java SDK (temporal-sdk)

| File | Change |
|------|--------|
| `io/temporal/activity/TypedDynamicActivity.java` | **NEW** - Interface definition |
| `io/temporal/internal/activity/TypedDynamicActivityExecutor.java` | **NEW** - Executor implementation |
| `io/temporal/internal/activity/ActivityTaskHandlerImpl.java` | **MODIFY** - Add `instanceof TypedDynamicActivity` branch |

### Kotlin SDK (temporal-kotlin)

| File | Change |
|------|--------|
| `io/temporal/kotlin/activity/KotlinActivityWrapper.kt` | **NEW** - TypedDynamicActivity implementation |
| `io/temporal/kotlin/worker/KWorker.kt` | **MODIFY** - Update `registerActivities()` |
| `io/temporal/kotlin/activity/KActivityRegistry.kt` | **REMOVE** - No longer needed |
| `io/temporal/kotlin/activity/KDynamicActivityHandler.kt` | **REMOVE** - No longer needed |
| `io/temporal/kotlin/activity/SuspendActivityInvoker.kt` | **MODIFY/MERGE** - Logic moves to KotlinActivityWrapper |

## Testing Strategy

1. **Unit tests for TypedDynamicActivity registration**
   - Verify activities map contains TypedDynamicActivityExecutor
   - Verify type name collision detection

2. **Integration tests for suspend activities**
   - Simple suspend activity execution
   - Suspend activity with cancellation
   - Suspend activity with heartbeat
   - Suspend activity failure handling

3. **Integration tests for non-suspend Kotlin activities**
   - Verify they work through the new path

4. **Mixed activity tests**
   - Java activities + Kotlin suspend activities on same worker
   - Verify no interference

## Future Considerations

1. **Async cancellation delivery**: Currently cancellation is detected via heartbeat. When the SDK supports async cancellation delivery, the coroutine `Job` can be cancelled directly.

2. **Custom CoroutineDispatcher**: Allow per-activity or per-activity-type dispatcher configuration.

3. **Structured concurrency**: Consider exposing the activity's `CoroutineScope` for child coroutine management.
