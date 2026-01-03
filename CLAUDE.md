# Claude Instructions

## Interaction Style

**Answer First, Then Wait**: When the user asks a question, provide an answer and then STOP. Wait for user input before taking any action. Do not immediately start implementing changes based on your interpretation of the question.

**Never Jump to Implementation**: After answering a question or proposing a solution, always wait for explicit user confirmation before writing any code or making changes. This applies to:
- Bug fixes
- New features
- Refactoring
- Test implementations
- Any code modifications

**Design Confirmation Required**: When proposing a fix or new implementation approach, always wait for explicit user confirmation that the design is acceptable before writing any code. Never jump to implementation based on your own analysis.

## Test Integrity

**Never Modify Tests to Hide Bugs**: When a test fails, the problem is usually in the code, not the test. Never remove, weaken, or modify tests to make them pass when there's a real issue in the implementation. Instead:
- Investigate the root cause of the failure
- Fix the actual code issue
- Only modify a test if it is genuinely incorrect or testing the wrong behavior

**Ask Before Modifying Tests**: When investigating test execution problems and you believe the solution requires modifying a test, always ask the user and wait for explicit confirmation before changing the test. Explain why you think the test needs to change.

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
