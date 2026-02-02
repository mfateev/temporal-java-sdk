# Phase 2 Implementation Plan: Advanced Workflow Features

## Overview
Port advanced workflow features from `temporal-kotlin` to `temporal-kotlin-sdk-alpha`.

## Features to Implement

### 2.1 External Workflow Interaction
- `KExternalWorkflowHandle<T>` - typed handle for signaling/canceling external workflows
- `KUntypedExternalWorkflowHandle` - untyped variant
- `KWorkflow.getExternalWorkflowHandle()` methods

### 2.2 Search Attributes and Memo
- `KWorkflow.typedSearchAttributes` property
- `KWorkflow.getSearchAttribute()` method
- `KWorkflow.upsertTypedSearchAttributes()` method
- `KWorkflow.getMemo()` method
- `KWorkflow.upsertMemo()` method

### 2.3 Missing Workflow State APIs
- `KWorkflow.mutableSideEffect()` - mutable side effect tracking
- `KWorkflow.isCancelRequested()` - cancellation check
- `KWorkflow.isReplaying` - replay detection
- `KWorkflow.logger()` - workflow-safe logger
- `KWorkflow.metricsScope` - metrics scope
- `KWorkflow.getLastCompletionResult()` - cron/retry completion result
- `KWorkflow.previousRunFailure` - previous run failure
- `KWorkflow.isEveryHandlerFinished` - handler completion check
- `KWorkflow.currentDetails` - current details string

### 2.4 Continue-As-New with Options
- Enhance `continueAsNew()` to accept `KContinueAsNewOptions`
- Support workflow type change in continue-as-new

### 2.5 Dynamic Handlers (Fix Implementation)
- Implement `registerDynamicSignalHandler()` properly
- Implement `registerDynamicQueryHandler()` properly
- Implement `registerDynamicUpdateHandler()` properly
- Implement `registerDynamicUpdateValidator()` properly

### 2.6 Child Workflow Handle
- `KChildWorkflowHandle<T, R>` with signal/cancel/result methods
- `KWorkflow.startChildWorkflow()` returning handle
- `KWorkflow.getChildWorkflowHandle()` for existing children

## Files to Create/Modify

### New Files
1. `workflow/KExternalWorkflowHandle.kt` - External workflow handle classes
2. `workflow/KChildWorkflowHandle.kt` - Enhanced child workflow handle

### Files to Modify
1. `workflow/KWorkflow.kt` - Add missing APIs
2. `workflow/KWorkflowContext.kt` - Add context methods
3. `internal/workflow/KWorkflowContextImpl.kt` - Implement context methods
4. `workflow/KContinueAsNewOptions.kt` - Already exists, verify completeness

## Test Plan
- Unit tests for external workflow handle creation
- Unit tests for search attributes and memo
- Unit tests for workflow state APIs
- Integration tests for dynamic handlers
- Integration tests for child workflow with handle

## Success Criteria
- All 363+ existing tests pass
- New unit tests for Phase 2 features pass
- Integration tests demonstrate feature functionality
