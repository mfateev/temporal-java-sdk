# temporal-core

Minimal shared runtime for Temporal Java and Kotlin SDKs.

## Purpose

This module contains core infrastructure that is shared between the Java SDK (`temporal-sdk`) and Kotlin SDK (`temporal-kotlin`). It provides:

- State machine infrastructure for workflow execution
- Replay abstractions and context
- Polling infrastructure (pollers, poll tasks)
- Core configuration options
- Utility classes

## Design Principle

Code belongs in `temporal-core` if:
1. It has no SDK-specific dependencies (no `DataConverter`, `ContextPropagator`, `WorkerInterceptor`, etc.)
2. It can be used identically by both Java and Kotlin SDKs
3. It is infrastructure rather than user-facing API

Code stays in SDK modules if:
1. It depends on SDK-specific interfaces (handlers, interceptors, converters)
2. It is part of the user-facing registration or configuration API
3. It requires SDK-specific implementation patterns

## Key Components

### State Machines (`io.temporal.internal.statemachines`)
- `WorkflowStateMachines` - central orchestrator for workflow execution
- Individual state machines for activities, timers, child workflows, etc.

### Replay (`io.temporal.internal.replay`)
- `ReplayWorkflow` - interface for workflow execution implementations
- `ReplayWorkflowContext` - context for workflow operations

### Common Utilities (`io.temporal.internal.common`)
- `CorePayloadConverter` - minimal JSON serialization
- Protocol utilities and SDK flags

### Worker Infrastructure (`io.temporal.internal.worker`)
- Core options classes (`CorePollerOptions`, `CoreWorkerVersioningOptions`, etc.)
- Polling infrastructure (to be moved - see `WORKER_POLLER_REFACTORING.md`)

## Refactoring Plans

See `WORKER_POLLER_REFACTORING.md` for the plan to move worker and poller infrastructure from `temporal-sdk` to this module.

## Dependencies

- `temporal-serviceclient` (api) - gRPC client and stubs
- `guava` - utilities
- `jackson-databind` - JSON processing
- `tally-core` - metrics
