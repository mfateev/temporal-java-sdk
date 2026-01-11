# Proposals vs Implementation Gap Analysis

This document identifies inconsistencies between the API specification in the proposals repo (`/Users/maxim/temporal/proposals-root/kotlin-sdk/kotlin/`) and the actual implementation in the Kotlin SDK.

## Summary

| Category | Status |
|----------|--------|
| KWorkflow.info | FIXED - Added `info` property |
| Query Property Syntax | FIXED - Added support for `@get:QueryMethod` on `val` properties |
| Continue-As-New | Consistent |
| Client API | Consistent |
| Activity API | Consistent |
| Worker API | Consistent |
| Dynamic Handlers | Consistent |

---

## 1. KWorkflow.info Property - FIXED

**Proposal (continue-as-new.md line 93):**
```kotlin
if (KWorkflow.info.isContinueAsNewSuggested) {
    KWorkflow.continueAsNew(state)
}
```

**Resolution:** Added `val info: KWorkflowInfo` property to `KWorkflow` object, matching the proposal and consistent with other property-style APIs (e.g., `typedSearchAttributes`, `metricsScope`).

**Implementation (KWorkflow.kt):**
```kotlin
public val info: KWorkflowInfo
    @JvmName("info")
    get() {
        val javaInfo: WorkflowInfo = Workflow.getInfo()
        return KWorkflowInfoImpl(javaInfo)
    }
```

---

## 2. Query Property Syntax - FIXED

**Proposal (signals-queries.md lines 22-27):**
```kotlin
@WorkflowInterface
interface OrderWorkflow {
    // Queries - always synchronous, can use property syntax
    @QueryMethod
    val status: OrderStatus

    @QueryMethod
    fun getItemCount(): Int
}
```

**Resolution:** Updated `KotlinWorkflowDefinition.findQueryMethods()` to support both function-style and property-style queries. The implementation now checks both `declaredFunctions` and `declaredMemberProperties` for `@QueryMethod` annotations.

**Note:** Due to Kotlin annotation target syntax, the actual usage requires `@get:QueryMethod` instead of `@QueryMethod` on properties:

```kotlin
@WorkflowInterface
interface OrderWorkflow {
    // Property-style query (Kotlin-idiomatic)
    @get:QueryMethod
    val status: OrderStatus

    // Property with custom query name
    @get:QueryMethod(name = "itemCount")
    val count: Int

    // Function-style query (also supported)
    @QueryMethod
    fun getDetails(): String
}
```

**Implementation (KotlinWorkflowDefinition.kt):**
```kotlin
private fun findQueryMethods(workflowInterface: KClass<*>): Map<String, KFunction<*>> {
    // Find query methods from declared functions
    val functionQueries = workflowInterface.declaredFunctions
        .filter { it.findAnnotation<io.temporal.workflow.QueryMethod>() != null }
        .associateBy { func ->
            val annotation = func.findAnnotation<io.temporal.workflow.QueryMethod>()!!
            if (annotation.name.isNotEmpty()) annotation.name else func.name
        }

    // Find query methods from property getters (supports @get:QueryMethod on val properties)
    val propertyQueries = workflowInterface.declaredMemberProperties
        .mapNotNull { prop ->
            val getter = prop.getter
            val annotation = getter.findAnnotation<io.temporal.workflow.QueryMethod>()
            if (annotation != null) {
                val name = if (annotation.name.isNotEmpty()) annotation.name else prop.name
                name to getter
            } else {
                null
            }
        }
        .toMap()

    return functionQueries + propertyQueries
}
```

**Test Added:** `WorkflowApiIntegrationTest.QueryMethod annotation works on Kotlin val properties`

---

## 3. Continue-As-New with Workflow Type Name (String)

**Proposal (continue-as-new.md lines 25-29, 123-128):**
```kotlin
// Continue as different workflow type (for versioning/migration)
KWorkflow.continueAsNew(
    "OrderProcessorV2",
    KContinueAsNewOptions(taskQueue = "orders-v2"),
    migratedState
)
```

API signature from proposal:
```kotlin
fun continueAsNew(
    workflowType: String,
    options: KContinueAsNewOptions,
    vararg args: Any?
): Nothing
```

**Actual Implementation (KWorkflow.kt lines 2419-2426):**
```kotlin
public fun continueAsNew(
    workflowType: String,
    options: KContinueAsNewOptions,
    vararg args: Any?
): Nothing {
    val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(workflowType, options.toJavaOptions(), *args)
}
```

**Status:** CONSISTENT - The implementation matches the proposal.

---

## 4. Type-Safe Continue-As-New with Method Reference

**Proposal (continue-as-new.md lines 31-37, 133-138):**
```kotlin
// Type-safe continue as different workflow using method reference
KWorkflow.continueAsNew(
    OrderProcessorV2::process,
    KContinueAsNewOptions(),
    migratedState
)
```

API signature from proposal:
```kotlin
fun <T> continueAsNew(
    workflow: KFunction<*>,
    options: KContinueAsNewOptions,
    vararg args: Any?
): Nothing
```

**Actual Implementation (KWorkflow.kt lines 2453-2461):**
```kotlin
public fun <T> continueAsNew(
    workflow: KFunction<*>,
    options: KContinueAsNewOptions,
    vararg args: Any?
): Nothing {
    val workflowType = extractWorkflowType(workflow)
    val context = currentContext.get()
        ?: throw IllegalStateException("KWorkflow.continueAsNew must be called from within workflow code")
    context.continueAsNew(workflowType, options.toJavaOptions(), *args)
}
```

**Status:** CONSISTENT - The implementation matches the proposal.

---

## Items Verified as Consistent

The following items from the proposals were verified as correctly implemented:

### Dynamic Handler Registration
- `registerSignalHandler(signalName, handler)` - Implemented
- `registerQueryHandler(queryName, handler)` - Implemented
- `registerUpdateHandler(updateName, handler)` - Implemented
- `registerUpdateHandler(updateName, validator, handler)` - Implemented
- `registerDynamicSignalHandler(handler)` - Implemented
- `registerDynamicQueryHandler(handler)` - Implemented
- `registerDynamicUpdateHandler(handler)` - Implemented
- `registerDynamicUpdateValidator(validator)` - Implemented

### KEncodedValues
- `size` property - Implemented
- `isEmpty()` - Implemented
- `get<T>(index)` - Implemented
- `get<T>(index, genericType)` - Implemented
- `get(index, KClass)` - Implemented
- `component1/2/3` destructuring - Implemented
- `toEncodedValues()` - Implemented

### KWorkflowInfo
- `isContinueAsNewSuggested` - Implemented
- All other workflow info properties - Implemented

### KContinueAsNewOptions
- `workflowRunTimeout` - Implemented
- `taskQueue` - Implemented
- `retryOptions` - Implemented
- `workflowTaskTimeout` - Implemented
- `memo` - Implemented
- `typedSearchAttributes` - Implemented
- `contextPropagators` - Implemented (additional property not in proposal)

### Continue-As-New
- `continueAsNew(vararg args)` - Implemented
- `continueAsNew(options, vararg args)` - Implemented
- `continueAsNew(workflowType, options, vararg args)` - Implemented
- `continueAsNew(workflow, options, vararg args)` - Implemented

### Client API
- `startWorkflow()` methods - Implemented
- `executeWorkflow()` methods - Implemented
- `getWorkflowHandle()` methods - Implemented
- `getUntypedWorkflowHandle()` methods - Implemented
- `signalWithStart()` methods - Implemented
- Update-with-start operations - Implemented

### Workflow Handle API
- `workflowId` property - Implemented
- `runId` property - Implemented
- `signal()` methods - Implemented
- `query()` methods - Implemented
- `executeUpdate()` methods - Implemented
- `cancel()` - Implemented
- `terminate()` - Implemented
- `describe()` - Implemented
- `getResult()` - Implemented
- `result()` on typed handle - Implemented

### Activity Options
- `KActivityOptions` with all properties - Implemented
- `KLocalActivityOptions` - Implemented

### Child Workflow Options
- `KChildWorkflowOptions` with all properties - Implemented
- Includes additional `priority` property not mentioned in proposal

### Worker Registration
- `registerWorkflowImplementationTypes()` - Implemented
- `registerActivitiesImplementations()` - Implemented
- `registerNexusServiceImplementation()` - Implemented

---

## API Changes

### New Kotlin SDK Code (KWorkflow.kt)

The new Kotlin SDK uses property-style APIs exclusively. No deprecated getter methods are provided since the SDK is not yet released:

- `info` property (not `getInfo()`)
- `typedSearchAttributes` property (not `getTypedSearchAttributes()`)
- `previousRunFailure` property (not `getPreviousRunFailure()`)
- `metricsScope` property (not `getMetricsScope()`)
- `currentUpdateInfo` property (not `getCurrentUpdateInfo()`)
- `currentDetails` property (not `setCurrentDetails()` / `getCurrentDetails()`)

### Preexisting Extension Files (Unchanged)

The following extension files predate the Kotlin SDK work (2021-2022) and are maintained unchanged for backwards compatibility:

**WorkflowServiceStubsExt.kt** - Deprecated methods retained:
- `WorkflowServiceStubs()` - deprecated, use `LocalWorkflowServiceStubs()`
- `WorkflowServiceStubs(options)` - deprecated, use `LazyWorkflowServiceStubs()` or `ConnectedWorkflowServiceStubs()`
- `ConnectedWorkflowServiceStubs(options, timeout)` - deprecated, use `ConnectedWorkflowServiceStubs(timeout, options)`

**WorkerExt.kt** - Deprecated methods retained:
- `addWorkflowImplementationFactory(options, factory)` - deprecated, use `registerWorkflowImplementationFactory()`
- `addWorkflowImplementationFactory(factory)` - deprecated, use `registerWorkflowImplementationFactory()`

These deprecations follow Java SDK API changes and are maintained for existing users.

---

## Notes

- The implementation follows the proposals closely
- The implementation includes some additional features not in the proposals (e.g., `contextPropagators` in `KContinueAsNewOptions`, `priority` in `KChildWorkflowOptions`)
- All APIs now use property style (`KWorkflow.info`) which is more Kotlin-idiomatic
- Query property syntax requires `@get:QueryMethod` annotation target (Kotlin limitation)
