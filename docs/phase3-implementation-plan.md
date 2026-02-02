# Phase 3: Client Features Implementation Plan

## Overview

Phase 3 adds client-side workflow interaction capabilities to `temporal-kotlin-sdk-alpha`, including workflow handles with interaction methods, start/execute workflow, signal-with-start, schedule management, and activity completion.

## Files to Create

### 1. KWorkflowHandle.kt
**Path**: `temporal-kotlin-sdk-alpha/src/main/kotlin/io/temporal/kotlinsdk/client/KWorkflowHandle.kt`

Classes:
- `KWorkflowHandle<T>` - Typed handle with signal/query/update/cancel/terminate/describe
- `KUntypedWorkflowHandle` - Untyped handle for dynamic workflows
- `KTypedWorkflowHandle<T, R>` - Handle with known result type + result() method
- `KUpdateHandle<R>` - Handle for update operations
- `KWorkflowExecutionDescription` - Workflow description data class

Methods per handle:
- signal (0-6 args)
- query (0-6 args)
- executeUpdate (0-6 args, both regular and suspend variants)
- cancel()
- terminate(reason)
- describe()
- getResult() / result()
- getUpdateHandle()

### 2. KScheduleHandle.kt
**Path**: `temporal-kotlin-sdk-alpha/src/main/kotlin/io/temporal/kotlinsdk/client/schedules/KScheduleHandle.kt`

Methods:
- describe()
- update(updater)
- delete()
- pause(note)
- unpause(note)
- trigger()
- backfill(backfills)

### 3. KActivityCompletionClient.kt
**Path**: `temporal-kotlin-sdk-alpha/src/main/kotlin/io/temporal/kotlinsdk/client/KActivityCompletionClient.kt`

Classes:
- `KActivityCompletionClient`
- `KActivityCompletionHandle<R>`

Methods:
- complete(taskToken, result)
- fail(taskToken, exception)
- forTaskToken(taskToken) -> KActivityCompletionHandle

### 4. KWorkflowExecutionDescription.kt
**Path**: `temporal-kotlin-sdk-alpha/src/main/kotlin/io/temporal/kotlinsdk/client/KWorkflowExecutionDescription.kt`

Data class wrapping workflow execution description.

## Files to Modify

### KClient.kt
Add methods:
- startWorkflow (14+ overloads for 0-6 args, regular and suspend)
- executeWorkflow (14+ overloads)
- signalWithStart (multiple overloads)
- getWorkflowHandle() / getUntypedWorkflowHandle()
- createSchedule()
- scheduleHandle()
- listSchedules()
- newActivityCompletionClient()

## Implementation Strategy

Given the large number of methods (50+), implementation will proceed in sub-phases:

### Phase 3.1: Core Handles
- KWorkflowHandle with basic methods (signal, query, cancel, terminate, describe)
- KUntypedWorkflowHandle base class
- KTypedWorkflowHandle with result()
- KUpdateHandle

### Phase 3.2: KClient Workflow Methods
- startWorkflow (all overloads)
- executeWorkflow (all overloads)
- getWorkflowHandle methods

### Phase 3.3: Signal-with-Start
- signalWithStart (all overloads)

### Phase 3.4: Schedule Management
- KScheduleHandle
- createSchedule, scheduleHandle, listSchedules on KClient

### Phase 3.5: Activity Completion
- KActivityCompletionClient
- KActivityCompletionHandle
- newActivityCompletionClient on KClient

### Phase 3.6: Tests
- Unit tests for all handle classes
- Integration tests for client methods

## Test Count Target
- Starting: 380 tests
- Target: 400+ tests (20+ new tests)

## Dependencies
- Java SDK's WorkflowClient, WorkflowStub, ScheduleClient
- Java SDK's ActivityCompletionClient
- kotlinx.coroutines for suspend functions
