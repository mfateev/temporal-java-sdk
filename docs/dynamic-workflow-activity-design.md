# Dynamic Workflow and Activity Support Design

## Overview

This document describes the design for Kotlin-native dynamic workflow and activity support, enabling workflows and activities that can handle any type at runtime.

## Use Cases

- Workflow types determined at runtime
- Single implementation handling multiple workflow/activity types
- Generic workflow routing/dispatching systems
- Building workflow engines or orchestrators on top of Temporal

## Java SDK Reference

The Java SDK provides:
- `DynamicWorkflow` - interface with `Object execute(EncodedValues args)`
- `DynamicActivity` - interface with `Object execute(EncodedValues args)`
- `DynamicSignalHandler` - handles signals by name
- `DynamicQueryHandler` - handles queries by name
- `DynamicUpdateHandler` - handles updates by name

## Kotlin API Design

### 1. KEncodedValues - Type-safe Argument Access

```kotlin
package io.temporal.kotlin.common

import io.temporal.common.converter.EncodedValues
import kotlin.reflect.KClass

/**
 * Kotlin wrapper for EncodedValues providing type-safe argument access.
 */
class KEncodedValues(private val values: EncodedValues) {
    /** Number of arguments */
    val size: Int get() = values.size

    /** Get argument at index with reified type */
    inline fun <reified T> get(index: Int): T = values.get(index, T::class.java)

    /** Get argument at index with explicit Java class */
    fun <T> get(index: Int, type: Class<T>): T = values.get(index, type)

    /** Get argument at index with KClass */
    fun <T : Any> get(index: Int, type: KClass<T>): T = values.get(index, type.java)

    /** Check if empty */
    fun isEmpty(): Boolean = size == 0

    /** Get underlying EncodedValues for Java interop */
    fun toEncodedValues(): EncodedValues = values
}
```

### 2. KDynamicWorkflow Interface

```kotlin
package io.temporal.kotlin.workflow

import io.temporal.kotlin.common.KEncodedValues

/**
 * Interface for implementing dynamic workflows that can handle any workflow type.
 *
 * Dynamic workflows receive all arguments as [KEncodedValues] and can return any result.
 * The workflow type is available via [KWorkflow.info.workflowType].
 *
 * Example:
 * ```kotlin
 * class RouterWorkflow : KDynamicWorkflow {
 *     override suspend fun execute(args: KEncodedValues): Any? {
 *         val workflowType = KWorkflow.info.workflowType
 *         val input = args.get<String>(0)
 *
 *         return when {
 *             workflowType.startsWith("greeting") -> "Hello, $input!"
 *             workflowType.startsWith("farewell") -> "Goodbye, $input!"
 *             else -> "Unknown workflow type: $workflowType"
 *         }
 *     }
 * }
 * ```
 */
interface KDynamicWorkflow {
    /**
     * Execute the workflow with the provided arguments.
     *
     * @param args The workflow arguments
     * @return The workflow result (must be serializable)
     */
    suspend fun execute(args: KEncodedValues): Any?
}
```

### 3. KDynamicActivity Interface

```kotlin
package io.temporal.kotlin.activity

import io.temporal.kotlin.common.KEncodedValues

/**
 * Interface for implementing dynamic activities that can handle any activity type.
 *
 * The activity type is available via [KActivity.info.activityType].
 *
 * Example:
 * ```kotlin
 * class GenericActivity : KDynamicActivity {
 *     override suspend fun execute(args: KEncodedValues): Any? {
 *         val activityType = KActivity.info.activityType
 *         val input = args.get<String>(0)
 *
 *         return "$activityType processed: $input"
 *     }
 * }
 * ```
 */
interface KDynamicActivity {
    /**
     * Execute the activity with the provided arguments.
     *
     * @param args The activity arguments
     * @return The activity result (must be serializable)
     */
    suspend fun execute(args: KEncodedValues): Any?
}
```

### 4. Dynamic Handlers for Signals, Queries, and Updates

```kotlin
package io.temporal.kotlin.workflow

import io.temporal.kotlin.common.KEncodedValues

/**
 * Handler for dynamic signals - receives any signal by name.
 */
fun interface KDynamicSignalHandler {
    /**
     * Handle a signal.
     * @param signalName The name of the signal
     * @param args The signal arguments
     */
    fun handle(signalName: String, args: KEncodedValues)
}

/**
 * Handler for dynamic queries - receives any query by name.
 */
fun interface KDynamicQueryHandler {
    /**
     * Handle a query and return a result.
     * @param queryName The name of the query
     * @param args The query arguments
     * @return The query result
     */
    fun handle(queryName: String, args: KEncodedValues): Any?
}

/**
 * Handler for dynamic updates - receives any update by name.
 */
fun interface KDynamicUpdateHandler {
    /**
     * Handle an update and return a result.
     * @param updateName The name of the update
     * @param args The update arguments
     * @return The update result
     */
    suspend fun handle(updateName: String, args: KEncodedValues): Any?
}

/**
 * Validator for dynamic updates.
 */
fun interface KDynamicUpdateValidator {
    /**
     * Validate an update request. Throw an exception to reject.
     * @param updateName The name of the update
     * @param args The update arguments
     */
    fun validate(updateName: String, args: KEncodedValues)
}
```

### 5. KWorkflow Extensions for Dynamic Handlers

```kotlin
// Add to KWorkflow object

/**
 * Register a handler for all signals not handled by @SignalMethod.
 */
fun registerDynamicSignalHandler(handler: KDynamicSignalHandler)

/**
 * Register a handler for all queries not handled by @QueryMethod.
 */
fun registerDynamicQueryHandler(handler: KDynamicQueryHandler)

/**
 * Register a handler for all updates not handled by @UpdateMethod.
 *
 * @param handler The update handler
 * @param validator Optional validator (called before handler)
 */
fun registerDynamicUpdateHandler(
    handler: KDynamicUpdateHandler,
    validator: KDynamicUpdateValidator? = null
)
```

### 6. KWorkerOptions Extension

```kotlin
data class KWorkerOptions(
    val taskQueue: String,
    val workflows: List<KClass<*>> = emptyList(),
    val activities: List<Any> = emptyList(),

    /**
     * Dynamic workflow implementation class.
     * Handles any workflow type not matched by registered workflows.
     * Only one dynamic workflow can be registered per worker.
     */
    val dynamicWorkflow: KClass<out KDynamicWorkflow>? = null,

    /**
     * Dynamic activity implementation.
     * Handles any activity type not matched by registered activities.
     * Only one dynamic activity can be registered per worker.
     */
    val dynamicActivity: KDynamicActivity? = null,

    // ... existing options
)
```

### 7. KClient Extensions for Untyped Execution

```kotlin
// Add to KClient

/**
 * Execute an untyped workflow by name.
 */
suspend fun <R : Any> executeUntypedWorkflow(
    workflowType: String,
    args: KArgs = kargs(),
    options: KWorkflowOptions,
    resultType: KClass<R>
): R

/**
 * Start an untyped workflow by name.
 */
suspend fun startUntypedWorkflow(
    workflowType: String,
    args: KArgs = kargs(),
    options: KWorkflowOptions
): KUntypedWorkflowHandle

/**
 * Signal with start for untyped workflow.
 */
suspend fun <R : Any> signalWithStartUntypedWorkflow(
    workflowType: String,
    workflowArgs: KArgs = kargs(),
    signalName: String,
    signalArgs: KArgs = kargs(),
    options: KWorkflowOptions,
    resultType: KClass<R>
): KUntypedWorkflowHandle
```

### 8. KUntypedWorkflowHandle

```kotlin
/**
 * Handle for interacting with an untyped workflow.
 */
interface KUntypedWorkflowHandle {
    val workflowId: String
    val runId: String?

    /** Get workflow result */
    suspend fun <R : Any> result(resultType: KClass<R>): R

    /** Send a signal by name */
    suspend fun signal(signalName: String, vararg args: Any?)

    /** Query by name */
    suspend fun <R : Any> query(queryName: String, resultType: KClass<R>, vararg args: Any?): R

    /** Send an update by name */
    suspend fun <R : Any> update(updateName: String, resultType: KClass<R>, vararg args: Any?): R

    /** Cancel the workflow */
    suspend fun cancel()

    /** Terminate the workflow */
    suspend fun terminate(reason: String? = null)
}

// Extension for reified types
suspend inline fun <reified R : Any> KUntypedWorkflowHandle.result(): R = result(R::class)
suspend inline fun <reified R : Any> KUntypedWorkflowHandle.query(queryName: String, vararg args: Any?): R =
    query(queryName, R::class, *args)
suspend inline fun <reified R : Any> KUntypedWorkflowHandle.update(updateName: String, vararg args: Any?): R =
    update(updateName, R::class, *args)
```

### 9. KWorkflow Extension for Untyped Activity Execution

```kotlin
// Add to KWorkflow object

/**
 * Execute an activity by name (for dynamic activity dispatch).
 */
suspend fun <R : Any> executeActivity(
    activityName: String,
    args: KArgs = kargs(),
    options: KActivityOptions,
    resultType: KClass<R>
): R

// Reified extension
suspend inline fun <reified R : Any> executeActivity(
    activityName: String,
    args: KArgs = kargs(),
    options: KActivityOptions
): R = executeActivity(activityName, args, options, R::class)
```

## Complete Example

```kotlin
object HelloDynamic {
    const val TASK_QUEUE = "HelloDynamicTaskQueue"
    const val WORKFLOW_ID = "HelloDynamicWorkflow"

    /**
     * Dynamic workflow that handles any workflow type.
     */
    class DynamicGreetingWorkflow : KDynamicWorkflow {
        private var name: String = ""

        override suspend fun execute(args: KEncodedValues): Any? {
            val greeting = args.get<String>(0)
            val workflowType = KWorkflow.info.workflowType

            // Register dynamic signal handler
            KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
                if (signalName == "greetingSignal") {
                    name = signalArgs.get<String>(0)
                }
            }

            // Wait for name to be set via signal
            KWorkflow.awaitCondition { name.isNotEmpty() }

            // Execute dynamic activity by name
            val result = KWorkflow.executeActivity<String>(
                activityName = "DynamicACT",
                args = kargs(greeting, name, workflowType),
                options = KActivityOptions(startToCloseTimeout = 10.seconds)
            )

            return result
        }
    }

    /**
     * Dynamic activity that handles any activity type.
     */
    class DynamicGreetingActivity : KDynamicActivity {
        override suspend fun execute(args: KEncodedValues): Any? {
            val activityType = KActivity.info.activityType
            val greeting = args.get<String>(0)
            val name = args.get<String>(1)
            val fromType = args.get<String>(2)

            return "$activityType: $greeting $name from: $fromType"
        }
    }

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        val client = KClient.connect()

        val worker = KWorker(
            client,
            KWorkerOptions(
                taskQueue = TASK_QUEUE,
                dynamicWorkflow = DynamicGreetingWorkflow::class,
                dynamicActivity = DynamicGreetingActivity()
            )
        )
        worker.start()

        val options = KWorkflowOptions(
            workflowId = WORKFLOW_ID,
            taskQueue = TASK_QUEUE
        )

        // Start untyped workflow with signal
        val handle = client.signalWithStartUntypedWorkflow(
            workflowType = "DynamicWF",
            workflowArgs = kargs("Hello"),
            signalName = "greetingSignal",
            signalArgs = kargs("John"),
            options = options
        )

        // Get result
        val result = handle.result<String>()
        println(result) // "DynamicACT: Hello John from: DynamicWF"

        System.exit(0)
    }
}
```

## Implementation Notes

### Internal Wrapper for DynamicWorkflow

```kotlin
internal class KDynamicWorkflowWrapper(
    private val implClass: KClass<out KDynamicWorkflow>
) : DynamicWorkflow {

    private lateinit var impl: KDynamicWorkflow

    override fun execute(args: EncodedValues): Any? {
        impl = implClass.createInstance()

        return KotlinWorkflowContext.runWorkflowMethod {
            impl.execute(KEncodedValues(args))
        }
    }
}
```

### Internal Wrapper for DynamicActivity

```kotlin
internal class KDynamicActivityWrapper(
    private val impl: KDynamicActivity
) : DynamicActivity {

    override fun execute(args: EncodedValues): Any? {
        return runBlocking {
            impl.execute(KEncodedValues(args))
        }
    }
}
```

## Summary

| Feature | Java SDK | Kotlin SDK |
|---------|----------|------------|
| Dynamic Workflow | `DynamicWorkflow` | `KDynamicWorkflow` (suspend) |
| Dynamic Activity | `DynamicActivity` | `KDynamicActivity` (suspend) |
| Dynamic Signal Handler | `DynamicSignalHandler` | `KDynamicSignalHandler` |
| Dynamic Query Handler | `DynamicQueryHandler` | `KDynamicQueryHandler` |
| Dynamic Update Handler | `DynamicUpdateHandler` | `KDynamicUpdateHandler` (suspend) |
| Encoded Values | `EncodedValues` | `KEncodedValues` (reified generics) |
| Untyped Workflow Stub | `WorkflowStub` | `KUntypedWorkflowHandle` |
| Untyped Activity Stub | `ActivityStub` | `KWorkflow.executeActivity(name, ...)` |

## Open Questions

1. Should `KDynamicActivity` support both suspend and non-suspend variants?
2. Should we provide a builder pattern for `KWorkerOptions` for Java interop?
3. Do we need `KDynamicWorkflow` to support workflow versioning via `KWorkflow.getVersion()`?
