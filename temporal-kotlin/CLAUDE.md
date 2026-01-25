# Kotlin SDK Instructions

## Required Reading

Always read the `AGENTS.md` file in the same directory as this file before starting any task. It contains important context about the Kotlin SDK codebase and development practices.

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

### Java Interop Conventions
Do not add `toJava()` or `fromJava()` methods directly on Kotlin public classes. Instead:
- Use extension functions in `JavaInterop.kt` for conversions (e.g., `KWorkflowOptions.toJava()` as an extension)
- Use internal converter classes like `KOptionsConverters` and `KScheduleConverters` for `toJava()` methods
- Use `toKotlin()` extension functions on Java types in `JavaInterop.kt` for Java-to-Kotlin conversions

This keeps the Kotlin API clean and separates interop concerns from the core data classes.
