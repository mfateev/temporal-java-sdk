package io.temporal.kotlin.client

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.client.WorkflowExecutionDescription
import io.temporal.common.SearchAttributes
import java.lang.reflect.Type
import java.time.Duration
import java.time.Instant

public class KWorkflowExecutionDescription(
  @PublishedApi internal val delegate: WorkflowExecutionDescription
) {
  // From WorkflowExecutionMetadata
  public val execution: WorkflowExecution get() = delegate.execution
  public val workflowType: String get() = delegate.workflowType
  public val taskQueue: String get() = delegate.taskQueue
  public val startTime: Instant get() = delegate.startTime
  public val executionTime: Instant get() = delegate.executionTime
  public val closeTime: Instant? get() = delegate.closeTime
  public val status: WorkflowExecutionStatus get() = delegate.status
  public val historyLength: Long get() = delegate.historyLength
  public val parentNamespace: String? get() = delegate.parentNamespace
  public val parentExecution: WorkflowExecution? get() = delegate.parentExecution
  public val rootExecution: WorkflowExecution? get() = delegate.rootExecution
  public val firstRunId: String? get() = delegate.firstRunId
  public val executionDuration: Duration? get() = delegate.executionDuration
  public val typedSearchAttributes: SearchAttributes get() = delegate.typedSearchAttributes

  // Reified memo access
  @Suppress("UNCHECKED_CAST")
  public inline fun <reified T> memo(key: String): T? =
    delegate.getMemo(key, T::class.java) as T?

  public inline fun <reified T> memo(key: String, genericType: Type): T? =
    delegate.getMemo(key, T::class.java, genericType)

  // From WorkflowExecutionDescription
  public val staticSummary: String? get() = delegate.staticSummary
  public val staticDetails: String? get() = delegate.staticDetails
  public val rawDescription: DescribeWorkflowExecutionResponse get() = delegate.rawDescription

  // Java interop
  public fun toWorkflowExecutionDescription(): WorkflowExecutionDescription = delegate
}
