# Temporal Testing Module

## Running Tests with External Service

Tests can run against either the in-memory test server or an external Temporal service.

### Environment Variable

Set `USE_DOCKER_SERVICE=true` to run tests against an external Temporal service:

```bash
USE_DOCKER_SERVICE=true ./gradlew test
```

### Search Attribute Auto-Registration

When using an external Temporal service (e.g., `temporal server start-dev`), the default test search attributes are **not** available by default (unlike Docker Compose setup).

The `TestWorkflowRule` automatically registers these search attributes when `useExternalService=true`:
- `CustomKeywordField` (Keyword)
- `CustomTextField` (Text)
- `CustomStringField` (Keyword)
- `CustomIntField` (Int)
- `CustomDoubleField` (Double)
- `CustomBoolField` (Bool)
- `CustomDatetimeField` (Datetime)
- `MyKeywordListField` (KeywordList)

This registration happens in the `TestWorkflowRule` constructor via `RegisterTestSearchAttributes`, so search attributes are available regardless of whether `doNotStart=true` is used.

### Implementation Details

- `RegisterTestSearchAttributes` uses the OperatorService API to register attributes
- Registration is idempotent - existing attributes are silently ignored
- `SDKTestWorkflowRule` propagates the `useExternalService` flag to `TestWorkflowRule`
