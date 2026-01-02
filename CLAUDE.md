# Claude Instructions

## Interaction Style

When the user asks a question, provide an answer first. Do not immediately start implementing changes based on your interpretation of the question. Wait for explicit instructions to implement.

**Design Confirmation Required**: When proposing a fix or new implementation approach, always wait for explicit user confirmation that the design is acceptable before writing any code. Never jump to implementation based on your own analysis.

## API Design Principles

### Full Compatibility
When creating Kotlin equivalents of Java SDK classes, include ALL options and properties from the Java SDK. Do not selectively include only "commonly used" options. The Kotlin SDK should provide full feature parity with the Java SDK.

### Directory and Package Structure
Follow the Java SDK directory and package structure unless there is a specific, documented reason to deviate. For example:
- `io.temporal.activity` → `io.temporal.kotlin.activity`
- `io.temporal.client` → `io.temporal.kotlin.client`
- `io.temporal.workflow` → `io.temporal.kotlin.workflow`
- `io.temporal.common` → `io.temporal.kotlin.common`

Do not create new package structures like `io.temporal.kotlin.options` that don't exist in the Java SDK.
