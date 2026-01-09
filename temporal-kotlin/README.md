# Temporal Kotlin SDK

The Temporal Kotlin SDK provides an idiomatic Kotlin experience for building Temporal workflows using coroutines and suspend functions.

## Key Features

- **Coroutine-based workflows** with `suspend fun`
- **Type-safe method references** for activities and child workflows
- **Kotlin Duration support** (`30.seconds` instead of `Duration.ofSeconds(30)`)
- **Null safety** (`T?` instead of `Optional<T>`)
- **DSL builders** for configuration
- **Full interoperability** with Java SDK

## Requirements

- Kotlin 1.8.x or higher
- kotlinx-coroutines-core 1.7.x or higher

## Installation

Add `temporal-kotlin` as a dependency to your `pom.xml`:
```xml
<dependency>
  <groupId>io.temporal</groupId>
  <artifactId>temporal-kotlin</artifactId>
  <version>N.N.N</version>
</dependency>
```

or to build.gradle.kts:
```kotlin
implementation("io.temporal:temporal-kotlin:N.N.N")
```

## Quick Start

```kotlin
import io.temporal.activity.ActivityInterface
import io.temporal.kotlin.activity.KActivity
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.client.KWorkflowClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.worker.KWorkerFactory
import io.temporal.kotlin.workflow.KWorkflow
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import kotlin.time.Duration.Companion.seconds

// Define activity interface
@ActivityInterface
interface GreetingActivities {
    suspend fun composeGreeting(greeting: String, name: String): String
}

// Implement activity
class GreetingActivitiesImpl : GreetingActivities {
    override suspend fun composeGreeting(greeting: String, name: String): String {
        return "$greeting, $name!"
    }
}

// Define workflow interface
@WorkflowInterface
interface GreetingWorkflow {
    @WorkflowMethod
    suspend fun getGreeting(name: String): String
}

// Implement workflow
class GreetingWorkflowImpl : GreetingWorkflow {
    override suspend fun getGreeting(name: String): String {
        return KWorkflow.executeActivity(
            GreetingActivities::composeGreeting,
            KActivityOptions(startToCloseTimeout = 10.seconds),
            "Hello", name
        )
    }
}

// Start worker and execute workflow
fun main() = runBlocking {
    val service = WorkflowServiceStubs.newLocalServiceStubs()
    val client = KWorkflowClient(service)
    val factory = KWorkerFactory(client)
    val worker = factory.newWorker("greetings")

    worker.registerWorkflowImplementationTypes<GreetingWorkflowImpl>()
    worker.registerSuspendActivities(GreetingActivitiesImpl())

    factory.start()

    val result = client.executeWorkflow(
        GreetingWorkflow::getGreeting,
        KWorkflowOptions(workflowId = "greeting-123", taskQueue = "greetings"),
        "World"
    )
    println(result) // "Hello, World!"
}
```

## Workflow APIs

### KWorkflow Object

The `KWorkflow` object is the primary entry point for workflow operations:

```kotlin
class MyWorkflowImpl : MyWorkflow {
    override suspend fun execute(): String {
        // Get workflow information
        val info = KWorkflow.getInfo()
        println("Workflow ID: ${info.workflowId}")

        // Get deterministic current time
        val now = KWorkflow.currentTime()

        // Generate deterministic UUID
        val id = KWorkflow.randomUUID()

        // Wait for a condition
        KWorkflow.awaitCondition { orderApproved }

        // Wait with timeout (returns false if timeout expires)
        val received = KWorkflow.awaitCondition(30.seconds) { messageReceived }

        return "completed"
    }
}
```

### Executing Activities

Use type-safe method references to execute activities:

```kotlin
// Execute activity with method reference (compile-time type safety)
val result = KWorkflow.executeActivity(
    GreetingActivities::composeGreeting,
    KActivityOptions(startToCloseTimeout = 30.seconds),
    "Hello", "World"
)

// Execute activity by name (for dynamic scenarios)
val result: String = KWorkflow.executeActivity(
    "composeGreeting",
    KActivityOptions(startToCloseTimeout = 30.seconds),
    "Hello", "World"
)
```

### Local Activities

For short-lived activities that run in the same worker process:

```kotlin
val result = KWorkflow.executeLocalActivity(
    ValidationActivities::validate,
    KLocalActivityOptions(startToCloseTimeout = 5.seconds),
    input
)
```

### Child Workflows

Execute child workflows with type-safe method references:

```kotlin
// Execute and wait for result
val result = KWorkflow.executeChildWorkflow(
    ChildWorkflow::process,
    KChildWorkflowOptions(workflowId = "child-123"),
    input
)

// Start async and get handle
val handle = KWorkflow.startChildWorkflow(
    ChildWorkflow::process,
    KChildWorkflowOptions(workflowId = "child-123"),
    input
)
// Later...
val result = handle.result()
```

### Parallel Execution

Use standard Kotlin coroutines for parallel execution:

```kotlin
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

override suspend fun processOrders(orders: List<Order>): List<Result> {
    return coroutineScope {
        orders.map { order ->
            async {
                KWorkflow.executeActivity(
                    OrderActivities::process,
                    options,
                    order
                )
            }
        }.awaitAll()
    }
}
```

### Timers and Delays

Use standard `kotlinx.coroutines.delay()`:

```kotlin
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.minutes

override suspend fun waitAndProcess(): String {
    delay(5.minutes)  // Workflow-safe timer
    return "processed"
}
```

### Signals, Queries, and Updates

Define handlers using annotations:

```kotlin
@WorkflowInterface
interface OrderWorkflow {
    @WorkflowMethod
    suspend fun processOrder(order: Order): OrderResult

    @SignalMethod
    suspend fun cancelOrder(reason: String)

    @QueryMethod
    fun getStatus(): OrderStatus

    @UpdateMethod
    suspend fun addItem(item: OrderItem): Boolean

    @UpdateValidatorMethod(updateMethod = "addItem")
    fun validateAddItem(item: OrderItem)
}
```

### Dynamic Handler Registration

Register handlers at runtime:

```kotlin
override suspend fun execute(): String {
    // Register signal handler
    KWorkflow.registerSignalHandler("notify") { args: KEncodedValues ->
        val message: String = args.get()
        println("Received: $message")
    }

    // Register query handler
    KWorkflow.registerQueryHandler("getState") { args: KEncodedValues ->
        currentState
    }

    // Register update handler with validator
    KWorkflow.registerUpdateHandler(
        "updateConfig",
        validator = { args: KEncodedValues ->
            val config: Config = args.get()
            require(config.isValid()) { "Invalid config" }
        },
        handler = { args: KEncodedValues ->
            val config: Config = args.get()
            this.config = config
            "Updated"
        }
    )

    KWorkflow.awaitCondition { done }
    return "completed"
}
```

### Side Effects and Versioning

```kotlin
// Execute non-deterministic code safely
val randomValue = KWorkflow.sideEffect {
    SecureRandom().nextInt(100)
}

// Version workflow changes
val version = KWorkflow.getVersion("feature-flag", KWorkflow.DEFAULT_VERSION, 1)
if (version == 1) {
    // New implementation
} else {
    // Old implementation
}
```

### Search Attributes and Memo

```kotlin
// Get search attributes
val status = KWorkflow.getSearchAttribute(SearchAttributeKey.forKeyword("Status"))

// Update search attributes
KWorkflow.upsertTypedSearchAttributes(
    SearchAttributeKey.forKeyword("Status").valueSet("Processing"),
    SearchAttributeKey.forLong("Count").valueSet(42L)
)

// Get memo value
val note: String? = KWorkflow.getMemo("note")

// Update memo
KWorkflow.upsertMemo(mapOf("note" to "updated value"))
```

### Continue-As-New

```kotlin
override suspend fun processItems(items: List<Item>): String {
    // Process first batch
    val remaining = processFirstBatch(items)

    if (remaining.isNotEmpty()) {
        // Wait for all handlers to complete before continuing
        KWorkflow.awaitCondition { KWorkflow.isEveryHandlerFinished() }

        // Continue as new with remaining items
        KWorkflow.continueAsNew(
            KContinueAsNewOptions(taskQueue = "my-queue"),
            remaining
        )
    }

    return "completed"
}
```

## Activity APIs

### KActivity Object

Access activity context and APIs:

```kotlin
class MyActivityImpl : MyActivity {
    override suspend fun process(input: String): String {
        // Get activity info
        val info = KActivity.context.info
        println("Activity: ${info.activityType}, attempt: ${info.attempt}")

        // Log using activity logger
        KActivity.logger().info("Processing input")

        // Heartbeat during long operations
        for (i in 1..100) {
            doWork(i)
            KActivity.heartbeat(i)  // Report progress
        }

        // Get heartbeat details from previous attempt (for retries)
        val lastProgress: Int? = KActivity.heartbeatDetails()

        return "done"
    }
}
```

## Client APIs

### KWorkflowClient

Create a Kotlin-idiomatic workflow client:

```kotlin
val service = WorkflowServiceStubs.newLocalServiceStubs()
val client = KWorkflowClient(service) {
    setNamespace("my-namespace")
}

// Start workflow and wait for result
val result = client.executeWorkflow(
    GreetingWorkflow::getGreeting,
    KWorkflowOptions(workflowId = "greeting-123", taskQueue = "greetings"),
    "World"
)

// Start workflow async
val handle = client.startWorkflow(
    GreetingWorkflow::getGreeting,
    KWorkflowOptions(workflowId = "greeting-123", taskQueue = "greetings"),
    "World"
)
// Later...
val result = handle.result()
```

### Workflow Handles

Interact with running workflows:

```kotlin
// Get typed handle
val handle = client.getWorkflowHandle<OrderWorkflow>("order-123")

// Send signal
handle.signal(OrderWorkflow::cancelOrder, "Customer request")

// Query
val status = handle.query(OrderWorkflow::getStatus)

// Execute update
val success = handle.executeUpdate(OrderWorkflow::addItem, newItem)

// Get result
val result = handle.result()
```

### Signal-With-Start

Atomically start a workflow and send a signal:

```kotlin
val handle = client.signalWithStart(
    OrderWorkflow::processOrder,
    KWorkflowOptions(workflowId = "order-123", taskQueue = "orders"),
    order,
    OrderWorkflow::addItem,  // Signal to send
    additionalItem           // Signal argument
)
```

### Update-With-Start

Atomically start a workflow and send an update:

```kotlin
val startOp = client.withStartWorkflowOperation(
    OrderWorkflow::processOrder,
    KWorkflowOptions(
        workflowId = "order-123",
        taskQueue = "orders",
        workflowIdConflictPolicy = WorkflowIdConflictPolicy.USE_EXISTING
    ),
    order
)

val result = client.executeUpdateWithStart(
    OrderWorkflow::addItem,
    KUpdateWithStartOptions(startWorkflowOperation = startOp),
    newItem
)
```

## Worker APIs

### KWorkerFactory and KWorker

Create workers with Kotlin-idiomatic APIs:

```kotlin
val client = KWorkflowClient(service)
val factory = KWorkerFactory(client) {
    setMaxWorkflowThreadCount(100)
}

val worker = factory.newWorker("my-task-queue") {
    setMaxConcurrentActivityExecutionSize(50)
}

// Register workflow implementations (reified generics)
worker.registerWorkflowImplementationTypes<OrderWorkflowImpl>()
worker.registerWorkflowImplementationTypes<ShippingWorkflowImpl>()

// Register activities with suspend method support
worker.registerSuspendActivities(OrderActivitiesImpl())

// Register regular activities
worker.registerActivitiesImplementations(LegacyActivitiesImpl())

factory.start()
```

## Testing

### Test Workflow Environment

```kotlin
@ExtendWith(KTestWorkflowExtension::class)
class MyWorkflowTest {

    @Test
    fun testWorkflow(
        testEnv: KTestWorkflowEnvironment,
        worker: KWorker,
        client: KWorkflowClient
    ) = runBlocking {
        worker.registerWorkflowImplementationTypes<GreetingWorkflowImpl>()
        worker.registerSuspendActivities(GreetingActivitiesImpl())
        testEnv.start()

        val result = client.executeWorkflow(
            GreetingWorkflow::getGreeting,
            KWorkflowOptions(workflowId = "test-1", taskQueue = worker.taskQueue),
            "World"
        )

        assertEquals("Hello, World!", result)
    }
}
```

### Test Activity Environment

```kotlin
@ExtendWith(KTestActivityExtension::class)
class MyActivityTest {

    @Test
    fun testActivity(testEnv: KTestActivityEnvironment) = runBlocking {
        testEnv.registerActivitiesImplementations(GreetingActivitiesImpl())

        val result = testEnv.executeActivity(
            GreetingActivities::composeGreeting,
            "Hello", "World"
        )

        assertEquals("Hello, World!", result)
    }
}
```

## Interceptors

Implement cross-cutting concerns with Kotlin interceptors:

```kotlin
class LoggingInterceptor : KWorkerInterceptorBase() {
    override fun interceptWorkflow(next: KWorkflowInboundCallsInterceptor) =
        object : KWorkflowInboundCallsInterceptorBase(next) {
            override suspend fun execute(input: KWorkflowInput): Any? {
                println("Starting workflow: ${input.workflowType}")
                return next.execute(input)
            }
        }

    override fun interceptActivity(next: KActivityInboundCallsInterceptor) =
        object : KActivityInboundCallsInterceptorBase(next) {
            override suspend fun execute(input: KActivityInput): Any? {
                println("Starting activity: ${input.activityInfo.activityType}")
                return next.execute(input)
            }
        }
}

// Register with worker factory
val factory = KWorkerFactory(client) {
    setWorkerInterceptors(LoggingInterceptor())
}
```

## Kotlin Options Classes

The SDK provides Kotlin data classes for all options:

```kotlin
// Activity options with Kotlin Duration
val activityOptions = KActivityOptions(
    startToCloseTimeout = 30.seconds,
    heartbeatTimeout = 5.seconds,
    retryOptions = KRetryOptions(
        initialInterval = 1.seconds,
        maximumInterval = 30.seconds,
        backoffCoefficient = 2.0,
        maximumAttempts = 5
    )
)

// Workflow options
val workflowOptions = KWorkflowOptions(
    workflowId = "my-workflow-123",
    taskQueue = "my-queue",
    workflowExecutionTimeout = 1.hours,
    workflowRunTimeout = 30.minutes
)

// Child workflow options
val childOptions = KChildWorkflowOptions(
    workflowId = "child-workflow-123",
    parentClosePolicy = ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE
)
```

## Java SDK Extensions

This module also provides Kotlin extensions for the Java SDK classes for a more idiomatic experience when working directly with Java SDK types.

### Options DSL

```kotlin
val retryOptions = RetryOptions {
    setInitialInterval(Duration.ofMillis(100))
    setMaximumInterval(Duration.ofSeconds(1))
    setBackoffCoefficient(1.5)
    setMaximumAttempts(5)
}

val activityOptions = ActivityOptions {
    setTaskQueue("TestQueue")
    setStartToCloseTimeout(Duration.ofMinutes(1))
    setRetryOptions {
        setInitialInterval(Duration.ofMillis(100))
    }
}
```

### Reified Type Extensions

```kotlin
val workflowResult = workflowStub.getResult<List<Long>>()
```

### Metadata Extensions

```kotlin
val activityName = activityName(ActivityInterface::activityMethod)
val workflowName = workflowName<WorkflowInterface>()
val workflowSignalName = workflowSignalName(WorkflowInterface::signalMethod)
```

## Migration from Java SDK

| Java SDK | Kotlin SDK |
|----------|------------|
| `Workflow.sleep(Duration)` | `delay(duration)` |
| `Workflow.await(() -> cond)` | `KWorkflow.awaitCondition { cond }` |
| `Workflow.newActivityStub(...)` | `KWorkflow.executeActivity(...)` |
| `Workflow.newChildWorkflowStub(...)` | `KWorkflow.executeChildWorkflow(...)` |
| `Promise<T>` | `Deferred<T>` via `coroutineScope { async { } }` |
| `Optional<T>` | `T?` |
| `Duration.ofSeconds(30)` | `30.seconds` |
| `ActivityOptions.newBuilder()...build()` | `KActivityOptions(...)` |

## Related

- [Temporal Documentation](https://docs.temporal.io/)
- [Kotlin Coroutines Guide](https://kotlinlang.org/docs/coroutines-guide.html)
- [temporal-kotlin-testing](../temporal-kotlin-testing) - Testing utilities
