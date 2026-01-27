/*
 * Copyright (C) 2022 Temporal Technologies, Inc. All Rights Reserved.
 *
 * Copyright (C) 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this material except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin

import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.schedules.Schedule
import io.temporal.client.schedules.ScheduleAction
import io.temporal.client.schedules.ScheduleActionExecution
import io.temporal.client.schedules.ScheduleActionExecutionStartWorkflow
import io.temporal.client.schedules.ScheduleActionResult
import io.temporal.client.schedules.ScheduleActionStartWorkflow
import io.temporal.client.schedules.ScheduleBackfill
import io.temporal.client.schedules.ScheduleCalendarSpec
import io.temporal.client.schedules.ScheduleDescription
import io.temporal.client.schedules.ScheduleInfo
import io.temporal.client.schedules.ScheduleIntervalSpec
import io.temporal.client.schedules.ScheduleListAction
import io.temporal.client.schedules.ScheduleListActionStartWorkflow
import io.temporal.client.schedules.ScheduleListDescription
import io.temporal.client.schedules.ScheduleListInfo
import io.temporal.client.schedules.ScheduleListSchedule
import io.temporal.client.schedules.ScheduleListState
import io.temporal.client.schedules.SchedulePolicy
import io.temporal.client.schedules.ScheduleRange
import io.temporal.client.schedules.ScheduleSpec
import io.temporal.client.schedules.ScheduleState
import io.temporal.common.Priority
import io.temporal.common.RetryOptions
import io.temporal.common.interceptors.WorkflowClientInterceptor
import io.temporal.kotlin.activity.KActivityCancellationType
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.client.KWorkflowOptions
import io.temporal.kotlin.client.schedules.KSchedule
import io.temporal.kotlin.client.schedules.KScheduleAction
import io.temporal.kotlin.client.schedules.KScheduleActionExecution
import io.temporal.kotlin.client.schedules.KScheduleActionExecutionStartWorkflow
import io.temporal.kotlin.client.schedules.KScheduleActionResult
import io.temporal.kotlin.client.schedules.KScheduleActionStartWorkflow
import io.temporal.kotlin.client.schedules.KScheduleBackfill
import io.temporal.kotlin.client.schedules.KScheduleCalendarSpec
import io.temporal.kotlin.client.schedules.KScheduleDescription
import io.temporal.kotlin.client.schedules.KScheduleInfo
import io.temporal.kotlin.client.schedules.KScheduleIntervalSpec
import io.temporal.kotlin.client.schedules.KScheduleListAction
import io.temporal.kotlin.client.schedules.KScheduleListActionStartWorkflow
import io.temporal.kotlin.client.schedules.KScheduleListDescription
import io.temporal.kotlin.client.schedules.KScheduleListInfo
import io.temporal.kotlin.client.schedules.KScheduleListSchedule
import io.temporal.kotlin.client.schedules.KScheduleListState
import io.temporal.kotlin.client.schedules.KSchedulePolicy
import io.temporal.kotlin.client.schedules.KScheduleRange
import io.temporal.kotlin.client.schedules.KScheduleSpec
import io.temporal.kotlin.client.schedules.KScheduleState
import io.temporal.kotlin.common.KPriority
import io.temporal.kotlin.common.KRetryOptions
import io.temporal.kotlin.interceptor.KWorkflowClientInterceptor
import io.temporal.kotlin.internal.converters.KWorkflowClientInterceptorJavaWrapper
import io.temporal.kotlin.internal.converters.WorkflowClientInterceptorKotlinWrapper
import io.temporal.kotlin.worker.KWorker
import io.temporal.kotlin.worker.KWorkerFactory
import io.temporal.kotlin.worker.KWorkflowImplementationOptions
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkflowImplementationOptions
import io.temporal.workflow.Promise
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import java.time.Duration

/**
 * Java SDK interoperability utilities.
 *
 * This object provides access to underlying Java SDK types from Kotlin SDK wrappers.
 * Use these utilities when you need to:
 * - Access Java SDK features not yet exposed in the Kotlin SDK
 * - Integrate with existing Java SDK code
 * - Use advanced Java SDK APIs directly
 *
 * Note: Using these APIs ties your code to Java SDK types and may require updates
 * when the Kotlin SDK evolves. Prefer using the Kotlin SDK APIs when possible.
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.JavaInterop
 *
 * // Access the underlying WorkflowClient
 * val workflowClient: WorkflowClient = JavaInterop.workflowClient(kClient)
 *
 * // Access the underlying WorkerFactory
 * val workerFactory: WorkerFactory = JavaInterop.workerFactory(kWorkerFactory)
 *
 * // Access the underlying Worker
 * val worker: Worker = JavaInterop.worker(kWorker)
 *
 * // Convert a Promise to a Deferred (inside workflows)
 * val deferred: Deferred<String> = JavaInterop.toDeferred(promise)
 * ```
 */
public object JavaInterop {

  /**
   * Returns the underlying [WorkflowClient] from a [KClient].
   *
   * Use this when you need to access Java SDK WorkflowClient features directly.
   *
   * @param client The Kotlin SDK client
   * @return The underlying Java SDK WorkflowClient
   */
  @JvmStatic
  public fun workflowClient(client: KClient): WorkflowClient = client.workflowClient

  /**
   * Returns the underlying [WorkerFactory] from a [KWorkerFactory].
   *
   * Use this when you need to access Java SDK WorkerFactory features directly.
   *
   * @param factory The Kotlin SDK worker factory
   * @return The underlying Java SDK WorkerFactory
   */
  @JvmStatic
  public fun workerFactory(factory: KWorkerFactory): WorkerFactory = factory.workerFactory

  /**
   * Returns the underlying [Worker] from a [KWorker].
   *
   * Use this when you need to access Java SDK Worker features directly.
   *
   * @param worker The Kotlin SDK worker
   * @return The underlying Java SDK Worker
   */
  @JvmStatic
  public fun worker(worker: KWorker): Worker = worker.worker

  /**
   * Converts a Temporal [Promise] to a Kotlin [Deferred].
   *
   * This is useful inside workflows when you need to work with Promise results
   * using Kotlin coroutine APIs.
   *
   * Note: This should only be used inside workflow code where Promises are available.
   *
   * @param promise The Temporal Promise to convert
   * @return A Kotlin Deferred that completes when the Promise completes
   */
  @JvmStatic
  public fun <R> toDeferred(promise: Promise<R>): Deferred<R> {
    val deferred = CompletableDeferred<R>()
    promise.handle { result, exception ->
      if (exception != null) {
        deferred.completeExceptionally(exception)
      } else {
        deferred.complete(result)
      }
      null
    }
    return deferred
  }

  /**
   * Awaits a Temporal [Promise] using Kotlin coroutine suspension.
   *
   * This is useful inside workflows when you need to await Promise results
   * using Kotlin suspend semantics.
   *
   * Note: This should only be used inside workflow code where Promises are available.
   *
   * @param promise The Temporal Promise to await
   * @return The result of the Promise
   */
  @JvmStatic
  public suspend fun <R> await(promise: Promise<R>): R = toDeferred(promise).await()
}

/**
 * Extension property to access the underlying [WorkflowClient] from a [KClient].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorkflowClient
 *
 * val workflowClient: WorkflowClient = kClient.javaWorkflowClient
 * ```
 */
public val KClient.javaWorkflowClient: WorkflowClient
  get() = workflowClient

/**
 * Extension property to access the underlying [WorkerFactory] from a [KWorkerFactory].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorkerFactory
 *
 * val workerFactory: WorkerFactory = kWorkerFactory.javaWorkerFactory
 * ```
 */
public val KWorkerFactory.javaWorkerFactory: WorkerFactory
  get() = workerFactory

/**
 * Extension property to access the underlying [Worker] from a [KWorker].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.javaWorker
 *
 * val worker: Worker = kWorker.javaWorker
 * ```
 */
public val KWorker.javaWorker: Worker
  get() = worker

/**
 * Extension function to convert a Temporal [Promise] to a Kotlin [Deferred].
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.toKotlinDeferred
 *
 * val deferred: Deferred<String> = promise.toKotlinDeferred()
 * ```
 */
public fun <R> Promise<R>.toKotlinDeferred(): Deferred<R> = JavaInterop.toDeferred(this)

/**
 * Extension function to await a Temporal [Promise] using Kotlin coroutine suspension.
 *
 * Example:
 * ```kotlin
 * import io.temporal.kotlin.awaitKotlin
 *
 * val result: String = promise.awaitKotlin()
 * ```
 */
public suspend fun <R> Promise<R>.awaitKotlin(): R = JavaInterop.await(this)

// =============================================================================
// Schedule Type Conversions
// =============================================================================

/**
 * Converts a Java SDK [Schedule] to a Kotlin SDK [KSchedule].
 */
public fun Schedule.toKotlin(): KSchedule = KSchedule(
  action = action.toKotlin(),
  spec = spec.toKotlin(),
  policy = policy?.toKotlin(),
  state = state?.toKotlin()
)

/**
 * Converts a Java SDK [ScheduleAction] to a Kotlin SDK [KScheduleAction].
 */
public fun ScheduleAction.toKotlin(): KScheduleAction = when (this) {
  is ScheduleActionStartWorkflow -> this.toKotlin()
  else -> throw IllegalArgumentException("Unknown schedule action type: ${this::class}")
}

/**
 * Converts a Java SDK [ScheduleActionStartWorkflow] to a Kotlin SDK [KScheduleActionStartWorkflow].
 */
public fun ScheduleActionStartWorkflow.toKotlin(): KScheduleActionStartWorkflow {
  val args = arguments?.let { emptyList<Any?>() } ?: emptyList()
  return KScheduleActionStartWorkflow(
    workflowType = workflowType,
    options = options.toKotlin(),
    arguments = args
  )
}

/**
 * Converts a Java SDK [WorkflowOptions] to a Kotlin SDK [KWorkflowOptions].
 */
public fun WorkflowOptions.toKotlin(): KWorkflowOptions = KWorkflowOptions(
  workflowId = workflowId,
  workflowIdReusePolicy = workflowIdReusePolicy,
  workflowIdConflictPolicy = workflowIdConflictPolicy,
  workflowRunTimeout = workflowRunTimeout?.toKotlin(),
  workflowExecutionTimeout = workflowExecutionTimeout?.toKotlin(),
  workflowTaskTimeout = workflowTaskTimeout?.toKotlin(),
  taskQueue = taskQueue,
  retryOptions = retryOptions?.toKotlin(),
  cronSchedule = cronSchedule,
  memo = memo,
  typedSearchAttributes = typedSearchAttributes,
  contextPropagators = contextPropagators,
  disableEagerExecution = isDisableEagerExecution,
  startDelay = startDelay?.toKotlin(),
  staticSummary = staticSummary,
  staticDetails = staticDetails,
  requestId = requestId,
  completionCallbacks = completionCallbacks,
  links = links,
  priority = priority?.toKotlin(),
  versioningOverride = versioningOverride
)

/**
 * Converts a Java SDK [RetryOptions] to a Kotlin SDK [KRetryOptions].
 */
public fun RetryOptions.toKotlin(): KRetryOptions = KRetryOptions(
  initialInterval = initialInterval.toKotlin(),
  backoffCoefficient = backoffCoefficient,
  maximumInterval = maximumInterval?.toKotlin(),
  maximumAttempts = maximumAttempts,
  doNotRetry = doNotRetry?.toList() ?: emptyList()
)

/**
 * Converts a Java SDK [ScheduleActionExecution] to a Kotlin SDK [KScheduleActionExecution].
 */
public fun ScheduleActionExecution.toKotlin(): KScheduleActionExecution = when (this) {
  is ScheduleActionExecutionStartWorkflow -> this.toKotlin()
  else -> throw IllegalArgumentException("Unknown schedule action execution type: ${this::class}")
}

/**
 * Converts a Java SDK [ScheduleActionExecutionStartWorkflow] to a Kotlin SDK [KScheduleActionExecutionStartWorkflow].
 */
public fun ScheduleActionExecutionStartWorkflow.toKotlin(): KScheduleActionExecutionStartWorkflow =
  KScheduleActionExecutionStartWorkflow(
    workflowId = workflowId,
    firstExecutionRunId = firstExecutionRunId
  )

/**
 * Converts a Java SDK [ScheduleActionResult] to a Kotlin SDK [KScheduleActionResult].
 */
public fun ScheduleActionResult.toKotlin(): KScheduleActionResult = KScheduleActionResult(
  scheduledAt = scheduledAt,
  startedAt = startedAt,
  action = action.toKotlin()
)

/**
 * Converts a Java SDK [ScheduleBackfill] to a Kotlin SDK [KScheduleBackfill].
 */
public fun ScheduleBackfill.toKotlin(): KScheduleBackfill = KScheduleBackfill(
  startAt = startAt,
  endAt = endAt,
  overlapPolicy = overlapPolicy
)

/**
 * Converts a Java SDK [ScheduleCalendarSpec] to a Kotlin SDK [KScheduleCalendarSpec].
 */
public fun ScheduleCalendarSpec.toKotlin(): KScheduleCalendarSpec = KScheduleCalendarSpec(
  seconds = seconds?.map { it.toKotlin() } ?: KScheduleCalendarSpec.BEGINNING,
  minutes = minutes?.map { it.toKotlin() } ?: KScheduleCalendarSpec.BEGINNING,
  hour = hour?.map { it.toKotlin() } ?: KScheduleCalendarSpec.BEGINNING,
  dayOfMonth = dayOfMonth?.map { it.toKotlin() } ?: KScheduleCalendarSpec.ALL_MONTH_DAYS,
  month = month?.map { it.toKotlin() } ?: KScheduleCalendarSpec.ALL_MONTHS,
  year = year?.map { it.toKotlin() } ?: emptyList(),
  dayOfWeek = dayOfWeek?.map { it.toKotlin() } ?: KScheduleCalendarSpec.ALL_WEEK_DAYS,
  comment = comment ?: ""
)

/**
 * Converts a Java SDK [ScheduleDescription] to a Kotlin SDK [KScheduleDescription].
 */
public fun ScheduleDescription.toKotlin(): KScheduleDescription = KScheduleDescription(
  id = id,
  info = info.toKotlin(),
  schedule = schedule.toKotlin(),
  searchAttributes = typedSearchAttributes,
  javaDescription = this
)

/**
 * Converts a Java SDK [ScheduleInfo] to a Kotlin SDK [KScheduleInfo].
 */
public fun ScheduleInfo.toKotlin(): KScheduleInfo = KScheduleInfo(
  numActions = numActions,
  numActionsMissedCatchupWindow = numActionsMissedCatchupWindow,
  numActionsSkippedOverlap = numActionsSkippedOverlap,
  runningActions = runningActions?.map { it.toKotlin() } ?: emptyList(),
  recentActions = recentActions?.map { it.toKotlin() } ?: emptyList(),
  nextActionTimes = nextActionTimes ?: emptyList(),
  createdAt = createdAt,
  lastUpdatedAt = lastUpdatedAt
)

/**
 * Converts a Java SDK [ScheduleIntervalSpec] to a Kotlin SDK [KScheduleIntervalSpec].
 */
public fun ScheduleIntervalSpec.toKotlin(): KScheduleIntervalSpec = KScheduleIntervalSpec(
  every = every,
  offset = offset ?: Duration.ZERO
)

/**
 * Converts a Java SDK [ScheduleListAction] to a Kotlin SDK [KScheduleListAction].
 */
public fun ScheduleListAction.toKotlin(): KScheduleListAction = when (this) {
  is ScheduleListActionStartWorkflow -> this.toKotlin()
  else -> throw IllegalArgumentException("Unknown schedule list action type: ${this::class.java}")
}

/**
 * Converts a Java SDK [ScheduleListActionStartWorkflow] to a Kotlin SDK [KScheduleListActionStartWorkflow].
 */
public fun ScheduleListActionStartWorkflow.toKotlin(): KScheduleListActionStartWorkflow =
  KScheduleListActionStartWorkflow(workflow = workflow)

/**
 * Converts a Java SDK [ScheduleListDescription] to a Kotlin SDK [KScheduleListDescription].
 */
public fun ScheduleListDescription.toKotlin(): KScheduleListDescription = KScheduleListDescription(
  scheduleId = scheduleId,
  schedule = schedule.toKotlin(),
  info = info.toKotlin(),
  searchAttributes = searchAttributes,
  javaDescription = this
)

/**
 * Converts a Java SDK [ScheduleListInfo] to a Kotlin SDK [KScheduleListInfo].
 */
public fun ScheduleListInfo.toKotlin(): KScheduleListInfo = KScheduleListInfo(
  recentActions = recentActions?.map { it.toKotlin() } ?: emptyList(),
  nextActionTimes = nextActionTimes ?: emptyList()
)

/**
 * Converts a Java SDK [ScheduleListSchedule] to a Kotlin SDK [KScheduleListSchedule].
 */
public fun ScheduleListSchedule.toKotlin(): KScheduleListSchedule = KScheduleListSchedule(
  action = action.toKotlin(),
  spec = spec.toKotlin(),
  state = state.toKotlin()
)

/**
 * Converts a Java SDK [ScheduleListState] to a Kotlin SDK [KScheduleListState].
 */
public fun ScheduleListState.toKotlin(): KScheduleListState = KScheduleListState(
  note = note,
  paused = isPaused
)

/**
 * Converts a Java SDK [SchedulePolicy] to a Kotlin SDK [KSchedulePolicy].
 */
public fun SchedulePolicy.toKotlin(): KSchedulePolicy = KSchedulePolicy(
  overlap = overlap,
  catchupWindow = catchupWindow,
  pauseOnFailure = isPauseOnFailure
)

/**
 * Converts a Java SDK [ScheduleRange] to a Kotlin SDK [KScheduleRange].
 */
public fun ScheduleRange.toKotlin(): KScheduleRange = KScheduleRange(start, end, step)

/**
 * Converts a Java SDK [ScheduleSpec] to a Kotlin SDK [KScheduleSpec].
 */
public fun ScheduleSpec.toKotlin(): KScheduleSpec = KScheduleSpec(
  calendars = calendars?.map { it.toKotlin() } ?: emptyList(),
  intervals = intervals?.map { it.toKotlin() } ?: emptyList(),
  cronExpressions = cronExpressions ?: emptyList(),
  skip = skip?.map { it.toKotlin() } ?: emptyList(),
  startAt = startAt,
  endAt = endAt,
  jitter = jitter,
  timeZoneName = timeZoneName
)

/**
 * Converts a Java SDK [ScheduleState] to a Kotlin SDK [KScheduleState].
 */
public fun ScheduleState.toKotlin(): KScheduleState = KScheduleState(
  note = note,
  paused = isPaused,
  limitedActions = isLimitedAction,
  remainingActions = remainingActions
)

// =============================================================================
// Activity Options Type Conversions
// =============================================================================

/**
 * Converts a Java SDK [ActivityOptions] to a Kotlin SDK [KActivityOptions].
 */
public fun ActivityOptions.toKotlin(): KActivityOptions = KActivityOptions(
  startToCloseTimeout = startToCloseTimeout?.toKotlin(),
  scheduleToCloseTimeout = scheduleToCloseTimeout?.toKotlin(),
  scheduleToStartTimeout = scheduleToStartTimeout?.toKotlin(),
  heartbeatTimeout = heartbeatTimeout?.toKotlin(),
  taskQueue = taskQueue,
  retryOptions = retryOptions?.toKotlin(),
  cancellationType = cancellationType?.toKotlin(),
  disableEagerExecution = isEagerExecutionDisabled
)

/**
 * Converts a Java SDK [LocalActivityOptions] to a Kotlin SDK [KLocalActivityOptions].
 */
public fun LocalActivityOptions.toKotlin(): KLocalActivityOptions = KLocalActivityOptions(
  startToCloseTimeout = startToCloseTimeout?.toKotlin(),
  scheduleToCloseTimeout = scheduleToCloseTimeout?.toKotlin(),
  localRetryThreshold = localRetryThreshold?.toKotlin(),
  retryOptions = retryOptions?.toKotlin()
)

// =============================================================================
// Worker Type Conversions
// =============================================================================

/**
 * Converts a Java SDK [WorkflowImplementationOptions] to a Kotlin SDK [KWorkflowImplementationOptions].
 */
public fun WorkflowImplementationOptions.toKotlin(): KWorkflowImplementationOptions =
  KWorkflowImplementationOptions(
    failWorkflowExceptionTypes = failWorkflowExceptionTypes?.map { it.kotlin } ?: emptyList(),
    activityOptions = activityOptions.mapValues { (_, v) -> v.toKotlin() },
    defaultActivityOptions = defaultActivityOptions?.toKotlin(),
    localActivityOptions = localActivityOptions.mapValues { (_, v) -> v.toKotlin() },
    defaultLocalActivityOptions = defaultLocalActivityOptions?.toKotlin(),
    nexusServiceOptions = nexusServiceOptions,
    defaultNexusServiceOptions = defaultNexusServiceOptions,
    enableUpsertVersionSearchAttributes = isEnableUpsertVersionSearchAttributes
  )

// =============================================================================
// Client Interceptor Conversions
// =============================================================================

/**
 * Converts a Kotlin SDK [KWorkflowClientInterceptor] to a Java SDK [WorkflowClientInterceptor].
 *
 * This wraps the Kotlin interceptor for use with the Java SDK, handling the conversion
 * from suspend functions to blocking calls.
 */
// Implementation: Uses runBlocking internally to bridge suspend functions to blocking calls.
public fun KWorkflowClientInterceptor.toJava(): WorkflowClientInterceptor =
  KWorkflowClientInterceptorJavaWrapper(this)

/**
 * Converts a Java SDK [WorkflowClientInterceptor] to a Kotlin SDK [KWorkflowClientInterceptor].
 *
 * This wraps the Java interceptor for use with the Kotlin SDK, handling the conversion
 * from blocking calls to suspend functions.
 */
// Implementation: Uses withContext(Dispatchers.IO) internally to bridge blocking calls to suspend functions.
public fun WorkflowClientInterceptor.toKotlin(): KWorkflowClientInterceptor =
  WorkflowClientInterceptorKotlinWrapper(this)

// =============================================================================
// Activity Cancellation Type Conversions
// =============================================================================

/**
 * Converts a Kotlin SDK [KActivityCancellationType] to a Java SDK [ActivityCancellationType].
 */
public fun KActivityCancellationType.toJava(): ActivityCancellationType = when (this) {
  KActivityCancellationType.WAIT_CANCELLATION_COMPLETED -> ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
  KActivityCancellationType.TRY_CANCEL -> ActivityCancellationType.TRY_CANCEL
  KActivityCancellationType.ABANDON -> ActivityCancellationType.ABANDON
}

/**
 * Converts a Java SDK [ActivityCancellationType] to a Kotlin SDK [KActivityCancellationType].
 */
public fun ActivityCancellationType.toKotlin(): KActivityCancellationType = when (this) {
  ActivityCancellationType.WAIT_CANCELLATION_COMPLETED -> KActivityCancellationType.WAIT_CANCELLATION_COMPLETED
  ActivityCancellationType.TRY_CANCEL -> KActivityCancellationType.TRY_CANCEL
  ActivityCancellationType.ABANDON -> KActivityCancellationType.ABANDON
}

// =============================================================================
// Priority Conversions
// =============================================================================

/**
 * Converts a Kotlin SDK [KPriority] to a Java SDK [Priority].
 */
public fun KPriority.toJava(): Priority = Priority.newBuilder()
  .setPriorityKey(priorityKey)
  .apply { fairnessKey?.let { setFairnessKey(it) } }
  .setFairnessWeight(fairnessWeight)
  .build()

/**
 * Converts a Java SDK [Priority] to a Kotlin SDK [KPriority].
 */
public fun Priority.toKotlin(): KPriority = KPriority(
  priorityKey = priorityKey,
  fairnessKey = fairnessKey,
  fairnessWeight = fairnessWeight
)
