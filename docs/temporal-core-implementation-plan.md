# Temporal Core Refactoring - Implementation Plan

## Overview

This document provides the implementation plan for extracting `temporal-core` from `temporal-sdk`. The goal is to create a minimal core module with protobuf-based APIs that both Java SDK and Kotlin SDK can build upon.

## Revised Approach

**Key Decision**: DataConverter and FailureConverter stay in their respective SDKs.
- Java SDK keeps `DataConverter`, `FailureConverter`, and implementations in `temporal-sdk`
- Kotlin SDK will create `KDataConverter`, `KFailureConverter` as Kotlin-idiomatic types
- This avoids complex circular dependency issues and allows each SDK to have idiomatic APIs

---

## Phase 1: Extract temporal-core Module

### Step 1.1: Create Empty temporal-core Module

**Status**: COMPLETED

Created empty `temporal-core` module with basic dependencies on `temporal-serviceclient`.

---

### Step 1.2: Move DataConverter to temporal-core

**Status**: SKIPPED

**Reason**: DataConverter has deep dependencies on `DefaultFailureConverter` which requires SDK-internal classes (`ActivityTaskHandlerImpl`, `POJOWorkflowImplementationFactory`). Instead of complex provider patterns, we'll keep DataConverter in temporal-sdk and create Kotlin-idiomatic `KDataConverter` in temporal-kotlin-sdk-alpha.

---

### Step 1.3: Move GenericWorkflowClient to temporal-core

**Status**: COMPLETED

Moved:
- `io.temporal.common.Experimental` annotation
- `io.temporal.internal.client.external.GenericWorkflowClient` interface
- `io.temporal.internal.client.external.GenericWorkflowClientImpl` implementation

These have minimal dependencies (only `temporal-serviceclient` and `GrpcRetryer` which is already in `temporal-serviceclient`).

---

### Step 1.4: Move Utility Classes to temporal-core

**Status**: COMPLETED

Moved:
- `io.temporal.internal.common.ProtobufTimeUtils`
- `io.temporal.internal.common.ProtoEnumNameUtils`

**Not moved** (SDK dependencies):
- `SearchAttributesUtil` - depends on `io.temporal.common.SearchAttributes`

---

### Step 1.5: Move Tests for Core Classes

**Status**: COMPLETED

Moved:
- `ProtobufTimeUtilsTest`

---

### Phase 1 Completion Criteria

- [x] `temporal-core` module exists
- [x] GenericWorkflowClient moved to temporal-core
- [x] Utility classes moved to temporal-core
- [x] temporal-sdk depends on temporal-core
- [x] ALL temporal-sdk tests pass
- [x] Tests for core classes are in temporal-core and pass
- [x] No SDK public API imports in temporal-core

---

## Current temporal-core Contents

```
temporal-core/src/main/java/
├── io/temporal/common/
│   └── Experimental.java
├── io/temporal/internal/client/external/
│   ├── GenericWorkflowClient.java
│   └── GenericWorkflowClientImpl.java
└── io/temporal/internal/common/
    ├── ProtobufTimeUtils.java
    └── ProtoEnumNameUtils.java

temporal-core/src/test/java/
└── io/temporal/internal/common/
    └── ProtobufTimeUtilsTest.java
```

---

## Phase 2: Kotlin Converter Types

**Status**: PENDING

**Goal**: Create Kotlin-idiomatic converter interfaces in `temporal-kotlin-sdk-alpha`.

### Step 2.1: Create KDataConverter Interface

Create `io.temporal.kotlinsdk.converter.KDataConverter`:
- Kotlin-native API with suspend functions where appropriate
- Methods for payload serialization/deserialization
- Can internally delegate to Java SDK's DataConverter

### Step 2.2: Create KFailureConverter Interface

Create `io.temporal.kotlinsdk.converter.KFailureConverter`:
- Kotlin-native API for exception <-> Failure conversion
- Clean Kotlin exception types

### Step 2.3: Create Default Implementations

- `DefaultKDataConverter` - wraps Java SDK's DefaultDataConverter
- `DefaultKFailureConverter` - wraps Java SDK's DefaultFailureConverter

---

## Phase 3: Update temporal-kotlin-sdk-alpha

**Status**: PENDING

**Goal**: temporal-kotlin-sdk-alpha uses temporal-core for low-level operations and defines its own Kotlin-idiomatic public API.

---

## Architecture Summary

```
temporal-serviceclient (gRPC stubs, protobuf types)
        │
        ▼
temporal-core (GenericWorkflowClient, utilities)
        │
        ├───────────────────┬───────────────────┐
        ▼                   ▼                   ▼
temporal-sdk         temporal-kotlin-sdk-alpha  (future SDKs)
(Java public API)    (Kotlin public API)
- DataConverter      - KDataConverter
- FailureConverter   - KFailureConverter
- WorkflowClient     - KClient
- Worker             - KWorker
```
