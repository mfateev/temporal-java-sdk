# Activity Heartbeat Guidelines

## When to Heartbeat

Heartbeats are for reporting progress during **long-running activities**. They make sense:

- **During** lengthy operations (loops, batch processing, file transfers)
- To allow cancellation detection mid-execution
- To report progress percentage or status

## When NOT to Heartbeat

- **At the start** - nothing has happened yet, no progress to report
- **At the end** - activity is completing anyway, heartbeat serves no purpose

## Example Pattern

```kotlin
// Good pattern
suspend fun processLargeFile(path: String) {
  val lines = readLines(path)
  lines.forEachIndexed { index, line ->
    process(line)
    if (index > 0 && index % 100 == 0) {
      Activity.heartbeat(index) // Report progress periodically
    }
  }
  // No heartbeat at end - we're done
}
```

## Key Points

1. Heartbeats are for **progress reporting** during execution
2. They enable **cancellation detection** for long-running work
3. Heartbeating at start/end adds no value and clutters code
