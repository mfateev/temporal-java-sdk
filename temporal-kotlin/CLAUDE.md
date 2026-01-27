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

## Documentation Guidelines

### Separating Public API Docs from Implementation Details

Use **KDoc (`/** */`)** for public API documentation only. Use **regular comments (`//` or `/* */`)** for implementation details that are useful for maintainers but should not appear in generated documentation.

**Public API documentation (KDoc)** should include:
- What the class/function does from a user's perspective
- Parameter descriptions
- Return value descriptions
- Usage examples
- Exceptions that may be thrown

**Implementation comments (regular comments)** should include:
- Why a particular approach was chosen
- Internal mechanics and delegation patterns
- References to internal classes
- Performance considerations
- Thread-safety notes for maintainers

**Example:**
```kotlin
// Implementation note: This delegates to KotlinWorkflowImplementationFactory for suspend
// workflows and uses KDynamicActivityWrapper to adapt KDynamicActivity implementations.
/**
 * Registers workflow and activity implementations with this worker.
 *
 * @param T The implementation class type
 */
inline fun <reified T : Any> registerWorkflowImplementationTypes() {
    // Internal: Check if suspend workflow to choose correct factory
    ...
}
```

This ensures generated API documentation stays clean for users while preserving valuable context for developers maintaining the code.
