# Temporal Core Refactoring - Implementation Plan

## Overview

This document provides the implementation plan for extracting `temporal-core` from `temporal-sdk`. The goal is to create a minimal core module that `temporal-kotlin-sdk-alpha` can depend on WITHOUT depending on `temporal-sdk`.

## How to Use This Plan

**Each step is executed by a fresh agent** that:
1. Reads `CLAUDE.md` for project guidelines
2. Reads this document (`docs/temporal-core-implementation-plan.md`)
3. Executes ONLY the current step
4. Validates the step completed successfully
5. Commits the changes

---

## Mandatory Rules

1. **NO NEW CODE** - Only move existing files and refactor them
2. **MOVE, DON'T COPY** - Files are moved from temporal-sdk to temporal-core
3. **ONE CLASS AT A TIME** - Refactor incrementally, validate after each change
4. **TESTS MUST PASS** - After every move/refactor, run `./gradlew :temporal-sdk:test`
5. **NO TEST DELETION** - Never delete or disable tests

---

## Phase 1: Extract temporal-core Module

### Step 1.1: Create Empty temporal-core Module

**Status**: COMPLETED

**Goal**: Create an empty `temporal-core` module that compiles.

**Tasks**:
1. Create directory `temporal-core/`
2. Create `temporal-core/build.gradle` with dependencies:
   - `api project(':temporal-serviceclient')`
   - Jackson, Guava, SLF4J (same versions as temporal-sdk)
3. Create `temporal-core/src/main/java/` directory
4. Add `include 'temporal-core'` to root `settings.gradle`
5. Run `./gradlew :temporal-core:build` - must succeed

**Validation**:
```bash
./gradlew :temporal-core:build
```

**On Success**: Mark this step COMPLETED, commit with message "Add empty temporal-core module"

---

### Step 1.2: Move DataConverter to temporal-core

**Status**: NOT STARTED

**Depends on**: Step 1.1

**Goal**: Move DataConverter and related classes to temporal-core without changing any code.

**Tasks**:
1. MOVE (not copy) these files from `temporal-sdk` to `temporal-core` (keep same packages):
   - `io/temporal/common/converter/DataConverter.java`
   - `io/temporal/common/converter/DefaultDataConverter.java`
   - `io/temporal/common/converter/PayloadConverter.java`
   - `io/temporal/common/converter/EncodingKeys.java`
   - `io/temporal/common/converter/DataConverterException.java`
   - All other files in `io/temporal/common/converter/`
2. Add `api project(':temporal-core')` to `temporal-sdk/build.gradle`
3. Run `./gradlew :temporal-sdk:test` - ALL tests must pass

**Validation**:
```bash
./gradlew :temporal-sdk:test
```

**On Success**: Mark this step COMPLETED, commit with message "Move DataConverter to temporal-core"

---

### Step 1.3: Move GenericWorkflowClient to temporal-core

**Status**: NOT STARTED

**Depends on**: Step 1.2

**Goal**: Move GenericWorkflowClient and implementation to temporal-core.

**Tasks**:
1. MOVE these files from `temporal-sdk` to `temporal-core` (keep same packages):
   - `io/temporal/internal/client/external/GenericWorkflowClient.java`
   - `io/temporal/internal/client/external/GenericWorkflowClientImpl.java`
   - Any classes these directly depend on (GrpcRetryer, etc.)
2. Run `./gradlew :temporal-sdk:test` - ALL tests must pass

**Validation**:
```bash
./gradlew :temporal-sdk:test
```

**On Success**: Mark this step COMPLETED, commit with message "Move GenericWorkflowClient to temporal-core"

---

### Step 1.4: Move Utility Classes to temporal-core

**Status**: NOT STARTED

**Depends on**: Step 1.3

**Goal**: Move internal utility classes to temporal-core.

**Tasks**:
1. MOVE these files from `temporal-sdk` to `temporal-core` (keep same packages):
   - `io/temporal/internal/common/ProtobufTimeUtils.java`
   - `io/temporal/internal/common/ProtoEnumNameUtils.java`
   - Other utility classes that don't depend on SDK public API
2. Run `./gradlew :temporal-sdk:test` - ALL tests must pass

**Validation**:
```bash
./gradlew :temporal-sdk:test
```

**On Success**: Mark this step COMPLETED, commit with message "Move utility classes to temporal-core"

---

### Step 1.5: Move Tests for Core Classes

**Status**: NOT STARTED

**Depends on**: Step 1.4

**Goal**: Move tests that ONLY test classes in temporal-core.

**Tasks**:
1. Identify test files that test ONLY classes now in temporal-core
2. MOVE those test files to `temporal-core/src/test/java/` (keep same packages)
3. Add test dependencies to `temporal-core/build.gradle`
4. Run both test suites

**Validation**:
```bash
./gradlew :temporal-core:test :temporal-sdk:test
```

**On Success**: Mark this step COMPLETED, commit with message "Move core tests to temporal-core"

---

### Step 1.6: Refactor to Remove SDK Dependencies (One Class at a Time)

**Status**: NOT STARTED

**Depends on**: Step 1.5

**Goal**: Refactor classes in temporal-core to not import SDK public API.

**Tasks**:
For EACH class in temporal-core that imports from `io.temporal.client.*`, `io.temporal.workflow.*`, `io.temporal.activity.*`, or `io.temporal.worker.*`:

1. Identify the SDK import
2. Refactor to use protobuf types directly instead
3. If temporal-sdk needs the old signature, add adapter/converter in temporal-sdk
4. Run `./gradlew :temporal-sdk:test` - must pass
5. Proceed to next class only after tests pass

**Validation**:
```bash
# After each class refactoring:
./gradlew :temporal-sdk:test

# After all refactoring, verify no SDK imports:
grep -r "import io.temporal.client\." temporal-core/src/main/java/ && echo "FAIL: SDK imports found" || echo "OK"
grep -r "import io.temporal.workflow\." temporal-core/src/main/java/ && echo "FAIL: SDK imports found" || echo "OK"
grep -r "import io.temporal.activity\." temporal-core/src/main/java/ && echo "FAIL: SDK imports found" || echo "OK"
grep -r "import io.temporal.worker\." temporal-core/src/main/java/ && echo "FAIL: SDK imports found" || echo "OK"
```

**On Success**: Mark this step COMPLETED, commit with message "Remove SDK dependencies from temporal-core"

---

### Phase 1 Completion Criteria

- [ ] `temporal-core` module exists
- [ ] Classes moved from temporal-sdk (not copied)
- [ ] temporal-sdk depends on temporal-core
- [ ] ALL temporal-sdk tests pass
- [ ] Tests for core classes are in temporal-core and pass
- [ ] No SDK public API imports in temporal-core

---

## Phase 2: Extract temporal-core-testing Module

**Status**: NOT STARTED

**Depends on**: Phase 1 complete

(Details to be added after Phase 1 is complete)

---

## Phase 3: Update temporal-kotlin-sdk-alpha

**Status**: NOT STARTED

**Depends on**: Phase 2 complete

**Goal**: Update temporal-kotlin-sdk-alpha to depend ONLY on temporal-core, NOT on temporal-sdk.

(Details to be added after Phase 2 is complete)
