# Kotlin SDK Alpha Porting Plan

## Overview

This document outlines the phased plan for porting the existing `temporal-kotlin` implementation to `temporal-kotlin-sdk-alpha`, making it independent of the Java SDK and only dependent on `temporal-core`.

### Current State

**temporal-kotlin (existing):**
- ~90 Kotlin files across activity, client, common, worker, workflow, interceptor packages
- Internal implementation delegating to Java SDK
- Package: `io.temporal.kotlin.*`

**temporal-kotlin-sdk-alpha (Phase 4 complete):**
- ~20 files with API surface (annotations, options, metadata extraction)
- No execution implementation yet
- Package: `io.temporal.kotlinsdk.*`

**temporal-core provides:**
- `GenericWorkflowClient` - protobuf-based client interface
- `WorkerInternal` and `WorkerFactoryInternal` - worker lifecycle interfaces
- `CoreActivityContext` and `CoreActivityInfo` - activity context interfaces
- `CoreWorkflowInfo` - workflow info interface
- `DataConverter`, `PayloadConverter`, `FailureConverter` - converter interfaces

### Dependencies

```
temporal-kotlin-sdk-alpha → temporal-core (NOT temporal-sdk)
temporal-kotlin-testing-alpha → temporal-core-testing (NOT temporal-testing)
```

---

## Phase 1: Common Infrastructure

**Goal:** Port foundational utilities and types that other packages depend on.

### 1.1 Common Package
Files to port/create:
- `common/KEncodedValues.kt` - Wrapper for encoded payloads
- `common/KArgs.kt` - Arguments wrapper
- `common/KPriority.kt` - Task priority enum

### 1.2 Internal Utilities
Files to port/create:
- `internal/InternalTemporalApi.kt` - Internal API annotation
- `internal/service/KotlinMethodReferenceDisassemblyService.kt` - Method reference parsing

### 1.3 Converter Integration
Files to port/create:
- `internal/converters/KDataConverter.kt` - Kotlin wrapper for DataConverter
- Update existing `OptionsConverters.kt` to use temporal-core types

**Estimated files:** ~10

---

## Phase 2: Activity Execution

**Goal:** Implement activity execution using `CoreActivityContext`.

### 2.1 Activity Context Implementation
Files to port/create:
- `internal/activity/KActivityContextImpl.kt` - Implement using `CoreActivityContext`
- `internal/activity/KActivityMetadata.kt` - Activity metadata extraction (exists, may need updates)
- `internal/activity/SuspendActivityContext.kt` - Suspend function support
- `internal/activity/SuspendActivityContextWrapper.kt` - Coroutine wrapper
- `internal/activity/SuspendActivityThreadContext.kt` - Thread context for suspend

### 2.2 Activity Wrappers
Files to port/create:
- `internal/activity/KotlinActivityWrapper.kt` - Wraps Kotlin activity implementations
- `internal/activity/KDynamicActivityWrapper.kt` - Dynamic activity support

### 2.3 Public Activity API
Files to complete (some exist):
- `activity/KActivityInfo.kt` - Port from temporal-kotlin
- `activity/KActivityCancellationType.kt` - Cancellation type enum
- `activity/KDynamicActivity.kt` - Dynamic activity interface

**Estimated files:** ~12

---

## Phase 3: Workflow Execution

**Goal:** Implement workflow execution using temporal-core state machines.

### 3.1 Workflow Context
Files to port/create:
- `internal/workflow/KotlinWorkflowContext.kt` - Core workflow context
- `internal/workflow/WorkflowContextElement.kt` - Coroutine context element
- Update `workflow/KWorkflowContext.kt` to be complete

### 3.2 Workflow Coroutine Support
Files to port/create:
- `internal/workflow/KotlinCoroutineDispatcher.kt` - Deterministic coroutine dispatcher
- `internal/workflow/KotlinDelay.kt` - Workflow delay implementation

### 3.3 Workflow Definition & Factory
Files to port/create:
- `internal/workflow/KotlinWorkflowDefinition.kt` - Workflow definition wrapper
- `internal/workflow/KotlinWorkflowImplementationFactory.kt` - Factory for workflow instances
- `internal/workflow/KotlinReplayWorkflow.kt` - Replay workflow support
- `internal/workflow/KotlinDynamicReplayWorkflow.kt` - Dynamic replay support

### 3.4 Public Workflow API
Files to port/create:
- `workflow/KWorkflow.kt` - Main workflow interface/functions
- `workflow/KWorkflowInfo.kt` - Workflow info using `CoreWorkflowInfo`
- `workflow/KDynamicWorkflow.kt` - Dynamic workflow interface
- `workflow/KDynamicHandlers.kt` - Dynamic signal/query handlers
- `workflow/KChildWorkflowHandle.kt` - Child workflow handle
- `workflow/KExternalWorkflowHandle.kt` - External workflow handle
- `workflow/KContinueAsNewOptions.kt` - Continue-as-new options

**Estimated files:** ~15

---

## Phase 4: Worker Implementation

**Goal:** Implement worker using `WorkerInternal` and `WorkerFactoryInternal`.

### 4.1 Worker Factory
Files to update/port:
- `worker/KWorkerFactory.kt` - Use `WorkerFactoryInternal`
- `worker/KWorkerFactoryOptions.kt` - (exists, may need updates)
- `worker/KWorkerFactoryOptionsBuilder.kt` - Builder pattern

### 4.2 Worker Implementation
Files to update/port:
- `worker/KWorker.kt` - Use `WorkerInternal`
- `worker/KWorkerOptions.kt` - (exists, may need updates)
- `worker/KWorkflowImplementationOptions.kt` - Workflow registration options

### 4.3 Internal Worker Support
Files to port/create:
- `internal/worker/` - Any internal worker utilities
- `internal/plugin/KotlinPlugin.kt` - Kotlin SDK plugin for worker

**Estimated files:** ~8

---

## Phase 5: Client Implementation

**Goal:** Implement client using `GenericWorkflowClient`.

### 5.1 Client Core
Files to update/port:
- `client/KClient.kt` - Use `GenericWorkflowClient`
- `client/KClientOptions.kt` - (exists, may need updates)
- `client/KWorkflowStub.kt` - (exists, needs execution impl)

### 5.2 Workflow Handles
Files to port/create:
- `client/KWorkflowHandle.kt` - Workflow execution handle
- `client/KWorkflowExecutionDescription.kt` - Execution description

### 5.3 Activity Completion
Files to port/create:
- `client/KActivityCompletionClient.kt` - Async activity completion
- `client/KActivityCompletionHandle.kt` - Completion handle

### 5.4 Update & Start Operations
Files to port/create:
- `client/KUpdateWithStartOptions.kt` - Update with start
- `client/KWithStartWorkflowOperation.kt` - Workflow operation
- `client/KOnConflictOptions.kt` - Conflict handling

**Estimated files:** ~12

---

## Phase 6: Schedules

**Goal:** Port schedule functionality using `GenericWorkflowClient` schedule methods.

Files to port (from `client/schedules/`):
- `KSchedule.kt`
- `KScheduleHandle.kt`
- `KScheduleOptions.kt`
- `KScheduleSpec.kt`
- `KScheduleState.kt`
- `KSchedulePolicy.kt`
- `KScheduleInfo.kt`
- `KScheduleDescription.kt`
- `KScheduleUpdate.kt`
- `KScheduleBackfill.kt`
- `KScheduleCalendarSpec.kt`
- `KScheduleIntervalSpec.kt`
- `KScheduleRange.kt`
- `KScheduleAction.kt`
- `KScheduleActionStartWorkflow.kt`
- `KScheduleActionExecution.kt`
- `KScheduleActionResult.kt`
- `KScheduleListInfo.kt`
- `KScheduleListState.kt`
- `KScheduleListSchedule.kt`
- `KScheduleListDescription.kt`
- `KScheduleListAction.kt`

Update `internal/converters/KScheduleConverters.kt`

**Estimated files:** ~23

---

## Phase 7: Interceptors

**Goal:** Port interceptor framework.

Files to port/create:
- `interceptor/KWorkflowClientInterceptor.kt`
- `interceptor/KWorkflowClientCallsInterceptor.kt`
- `interceptor/KWorkerInterceptor.kt`
- `interceptor/KWorkflowInboundCallsInterceptor.kt`
- `interceptor/KWorkflowOutboundCallsInterceptor.kt`
- `interceptor/KActivityInboundCallsInterceptor.kt`

Internal:
- `internal/interceptor/InterceptorChain.kt`
- `internal/interceptor/RootWorkflowInboundCallsInterceptor.kt`
- `internal/interceptor/RootWorkflowOutboundCallsInterceptor.kt`
- `internal/interceptor/RootActivityInboundCallsInterceptor.kt`
- `internal/converters/KWorkflowClientInterceptorConverters.kt`

**Estimated files:** ~11

---

## Phase 8: Testing Infrastructure

**Goal:** Create `temporal-kotlin-testing-alpha` using `temporal-core-testing`.

### 8.1 Module Setup
- Create `temporal-kotlin-testing-alpha/build.gradle`
- Add to `settings.gradle`

### 8.2 Test Environment
Files to create:
- `testing/KTestEnvironmentOptions.kt`
- `testing/KTestWorkflowEnvironment.kt` - Use `CoreTestEnvironment`
- `testing/KTestWorkflowExtension.kt` - JUnit 5 extension
- `testing/KTestActivityEnvironment.kt`
- `testing/KTestActivityExtension.kt`
- `testing/WorkflowInitialTime.kt`

### 8.3 Activity Mocking
Files to create:
- `testing/KActivityMocking.kt`
- `testing/internal/KTestActivityRegistry.kt`
- `testing/internal/KTestDynamicActivityHandler.kt`

**Estimated files:** ~10

---

## Phase 9: Integration & Testing

**Goal:** Port existing tests and verify everything works.

### 9.1 Unit Tests
- Port tests from `temporal-kotlin/src/test/`
- Port tests from `temporal-kotlin-testing/src/test/`

### 9.2 Integration Tests
- Verify workflow execution against Temporal server
- Verify activity execution
- Verify schedule operations
- Verify interceptors

### 9.3 Documentation
- Update API documentation
- Create migration guide from temporal-kotlin

**Estimated: Variable based on test count**

---

## Phase Summary

| Phase | Description | Est. Files | Dependencies |
|-------|-------------|------------|--------------|
| 1 | Common Infrastructure | ~10 | None |
| 2 | Activity Execution | ~12 | Phase 1 |
| 3 | Workflow Execution | ~15 | Phase 1, 2 |
| 4 | Worker Implementation | ~8 | Phase 1, 2, 3 |
| 5 | Client Implementation | ~12 | Phase 1, 4 |
| 6 | Schedules | ~23 | Phase 5 |
| 7 | Interceptors | ~11 | Phase 2, 3, 5 |
| 8 | Testing Infrastructure | ~10 | All above |
| 9 | Integration & Testing | Variable | All above |

**Total estimated new/modified files:** ~100+

---

## Key Challenges

1. **State Machine Integration:** Workflow execution requires integrating with temporal-core state machines which is the most complex part.

2. **Coroutine Dispatcher:** The deterministic coroutine dispatcher must work with temporal-core's execution model.

3. **No Java SDK Types:** All existing code that references Java SDK types needs to be rewritten to use temporal-core or protobuf types.

4. **Backward Compatibility:** Consider whether temporal-kotlin-sdk-alpha should be a drop-in replacement or a new API.

---

## Execution Strategy

1. Execute phases sequentially
2. Each phase should have tests before moving to next
3. Keep temporal-kotlin unchanged as reference
4. Use different package (`io.temporal.kotlinsdk`) to avoid conflicts
