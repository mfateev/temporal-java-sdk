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

package io.temporal.kotlin.internal

import com.uber.m3.tally.Scope
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes
import io.temporal.api.common.v1.ActivityType
import io.temporal.api.common.v1.Memo
import io.temporal.api.common.v1.Payloads
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.common.v1.WorkflowType
import io.temporal.api.failure.v1.Failure
import io.temporal.api.sdk.v1.UserMetadata
import io.temporal.api.taskqueue.v1.TaskQueue
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse
import io.temporal.common.RetryOptions
import io.temporal.common.SearchAttributeUpdate
import io.temporal.common.SearchAttributes
import io.temporal.common.converter.DataConverter
import io.temporal.common.converter.EncodedValues
import io.temporal.internal.common.ProtobufTimeUtils
import io.temporal.internal.common.SearchAttributesUtil
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.internal.statemachines.ExecuteActivityParameters
import io.temporal.internal.statemachines.ExecuteLocalActivityParameters
import io.temporal.internal.statemachines.LocalActivityCallback
import io.temporal.internal.statemachines.StartChildWorkflowExecutionParameters
import io.temporal.kotlin.common.KEncodedValues
import io.temporal.kotlin.interceptor.KWorkflowOutboundCallsInterceptor
import io.temporal.workflow.ChildWorkflowCancellationType
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.UpdateInfo
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.suspendCancellableCoroutine
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.Random
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Internal context that wraps [ReplayWorkflowContext] for Kotlin workflows.
 *
 * This class provides suspend function wrappers for Temporal workflow operations,
 * allowing Kotlin coroutine-based workflows to interact with the Temporal runtime.
 */
/**
 * Type alias for signal handlers.
 * Signal handlers receive the signal name and encoded arguments.
 */
typealias SignalHandler = suspend (args: KEncodedValues) -> Unit

/**
 * Type alias for dynamic signal handlers that handle any signal.
 * Dynamic handlers receive both the signal name and encoded arguments.
 */
typealias DynamicSignalHandler = suspend (signalName: String, args: KEncodedValues) -> Unit

/**
 * Type alias for query handlers.
 * Query handlers receive the encoded arguments and return a result.
 * Note: Query handlers are NOT suspend functions as queries must return immediately.
 */
typealias QueryHandler<R> = (args: KEncodedValues) -> R

/**
 * Type alias for dynamic query handlers that handle any query.
 * Dynamic handlers receive both the query name and encoded arguments.
 */
typealias DynamicQueryHandler = (queryName: String, args: KEncodedValues) -> Any?

/**
 * Type alias for update handlers.
 * Update handlers receive the encoded arguments and return a result.
 */
typealias UpdateHandler = suspend (args: KEncodedValues) -> Any?

/**
 * Type alias for update validators.
 * Validators receive the encoded arguments and throw if validation fails.
 */
typealias UpdateValidator = (args: KEncodedValues) -> Unit

/**
 * Type alias for dynamic update handlers.
 * Dynamic handlers receive both the update name and encoded arguments.
 */
typealias DynamicUpdateHandler = suspend (updateName: String, args: KEncodedValues) -> Any?

/**
 * Type alias for dynamic update validators.
 * Validators receive both the update name and encoded arguments.
 */
typealias DynamicUpdateValidator = (updateName: String, args: KEncodedValues) -> Unit

/**
 * Simple implementation of [UpdateInfo] for tracking current update context.
 */
internal class KUpdateInfo(
  private val updateName: String,
  private val updateId: String
) : UpdateInfo {
  override fun getUpdateName(): String = updateName
  override fun getUpdateId(): String = updateId
}

@InternalTemporalApi
@PublishedApi
internal class KotlinWorkflowContext(
  internal val replayContext: ReplayWorkflowContext,
  internal val dataConverter: DataConverter = DataConverter.getDefaultInstance()
) {

  /**
   * Reference to the dispatcher for explicit dispatch operations.
   * Set by KotlinReplayWorkflow after construction.
   */
  @Volatile
  internal var dispatcher: KotlinCoroutineDispatcher? = null

  /**
   * Reference to the coroutine scope for launching async coroutines.
   * Set by KotlinReplayWorkflow after construction.
   */
  @Volatile
  internal var coroutineScope: CoroutineScope? = null

  // ==================== Dynamic Handler Storage ====================

  /**
   * Registered signal handlers by signal name.
   */
  internal val signalHandlers = ConcurrentHashMap<String, SignalHandler>()

  /**
   * Dynamic signal handler for unhandled signals.
   */
  @Volatile
  internal var dynamicSignalHandler: DynamicSignalHandler? = null

  /**
   * Registered query handlers by query name.
   */
  internal val queryHandlers = ConcurrentHashMap<String, QueryHandler<*>>()

  /**
   * Dynamic query handler for unhandled queries.
   */
  @Volatile
  internal var dynamicQueryHandler: DynamicQueryHandler? = null

  /**
   * Registered update handlers by update name.
   */
  internal val updateHandlers = ConcurrentHashMap<String, UpdateHandler>()

  /**
   * Registered update validators by update name.
   */
  internal val updateValidators = ConcurrentHashMap<String, UpdateValidator>()

  /**
   * Dynamic update handler for unhandled updates.
   */
  @Volatile
  internal var dynamicUpdateHandler: DynamicUpdateHandler? = null

  /**
   * Dynamic update validator for unhandled updates.
   */
  @Volatile
  internal var dynamicUpdateValidator: DynamicUpdateValidator? = null

  // ==================== Interceptor ====================

  /**
   * Outbound interceptor for workflow calls.
   * Set by the interceptor chain during initialization.
   */
  @Volatile
  internal var outboundInterceptor: KWorkflowOutboundCallsInterceptor? = null

  // ==================== Handler Tracking ====================

  /**
   * Counter for running signal handlers.
   */
  internal val runningSignalHandlers = AtomicInteger(0)

  /**
   * Counter for running update handlers.
   */
  internal val runningUpdateHandlers = AtomicInteger(0)

  /**
   * Current update info (set during update handler execution).
   */
  internal val currentUpdateInfo = AtomicReference<UpdateInfo?>(null)

  /**
   * Current workflow details (user-settable).
   */
  @Volatile
  internal var currentDetails: String? = null

  /**
   * Returns the workflow execution info.
   */
  val workflowExecution: WorkflowExecution
    get() = replayContext.workflowExecution

  /**
   * Returns the workflow type.
   */
  val workflowType: WorkflowType
    get() = replayContext.workflowType

  /**
   * Returns the current workflow time in milliseconds.
   * This is deterministic and returns the same value during replay.
   */
  val currentTimeMillis: Long
    get() = replayContext.currentTimeMillis()

  /**
   * Returns true if the workflow is replaying from history.
   */
  val isReplaying: Boolean
    get() = replayContext.isReplaying

  /**
   * Returns the workflow run ID.
   */
  val runId: String
    get() = replayContext.runId

  /**
   * Returns the workflow ID.
   */
  val workflowId: String
    get() = replayContext.workflowId

  /**
   * Returns the workflow namespace.
   */
  val namespace: String
    get() = replayContext.namespace

  /**
   * Returns a deterministic random number generator.
   */
  fun newRandom(): Random = replayContext.newRandom()

  /**
   * Returns a deterministic UUID.
   */
  fun randomUUID(): UUID = replayContext.randomUUID()

  /**
   * Creates a timer that completes after the specified duration.
   *
   * @param duration the duration to wait
   * @return Unit when the timer fires
   * @throws kotlinx.coroutines.CancellationException if the timer is cancelled
   */
  suspend fun createTimer(duration: Duration): Unit = suspendCancellableCoroutine { cont ->
    val cancellationHandle = replayContext.newTimer(
      duration,
      null as UserMetadata?
    ) { exception ->
      if (exception != null) {
        cont.resumeWithException(exception)
      } else {
        cont.resume(Unit)
      }
    }

    cont.invokeOnCancellation { cause ->
      cancellationHandle.apply(
        cause as? RuntimeException
          ?: RuntimeException(cause?.message ?: "Timer cancelled")
      )
    }
  }

  /**
   * Executes an activity and returns its result.
   *
   * @param parameters the activity execution parameters
   * @return the activity result payload
   * @throws Exception if the activity fails
   */
  suspend fun executeActivity(
    parameters: ExecuteActivityParameters
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    val output = replayContext.scheduleActivityTask(
      parameters
    ) { result: Optional<Payloads>, failure: Failure? ->
      if (failure != null) {
        cont.resumeWithException(
          dataConverter.failureToException(failure)
        )
      } else {
        cont.resume(result)
      }
    }

    cont.invokeOnCancellation { cause ->
      output.cancellationHandle.apply(
        cause as? Exception
          ?: RuntimeException(cause?.message ?: "Activity cancelled")
      )
    }
  }

  /**
   * Executes a local activity and returns its result.
   *
   * @param parameters the local activity execution parameters
   * @return the activity result payload
   * @throws Exception if the activity fails
   */
  suspend fun executeLocalActivity(
    parameters: ExecuteLocalActivityParameters
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    val cancellationHandle = replayContext.scheduleLocalActivityTask(
      parameters,
      LocalActivityCallback { result, exception ->
        if (exception != null) {
          cont.resumeWithException(
            dataConverter.failureToException(exception.failure)
          )
        } else {
          cont.resume(result)
        }
      }
    )

    cont.invokeOnCancellation { cause ->
      cancellationHandle.apply()
    }
  }

  /**
   * Starts a child workflow execution.
   *
   * @param parameters the child workflow parameters
   * @return pair of workflow execution (when started) and result payload (when completed)
   */
  suspend fun startChildWorkflow(
    parameters: StartChildWorkflowExecutionParameters
  ): Pair<WorkflowExecution, Optional<Payloads>> {
    // Use a holder to pass the completion result/exception between callbacks
    var completionResult: Optional<Payloads>? = null
    var completionException: Exception? = null
    var completionCont: CancellableContinuation<Optional<Payloads>>? = null
    var completed = false

    // First, wait for the child to start
    val execution = suspendCancellableCoroutine<WorkflowExecution> { startCont ->
      val cancellationHandle = replayContext.startChildWorkflow(
        parameters,
        { execution: WorkflowExecution?, startException: Exception? ->
          if (startException != null) {
            startCont.resumeWithException(startException)
          } else if (execution != null) {
            startCont.resume(execution)
          }
        },
        { result: Optional<Payloads>, exception: Exception? ->
          // Store completion data
          completionResult = result
          completionException = exception
          completed = true
          // If completion continuation is already waiting, resume it
          completionCont?.let { cont ->
            if (exception != null) {
              cont.resumeWithException(exception)
            } else {
              cont.resume(result)
            }
          }
        }
      )

      startCont.invokeOnCancellation { cause ->
        cancellationHandle.apply(
          cause as? Exception
            ?: RuntimeException(cause?.message ?: "Child workflow cancelled")
        )
      }
    }

    // Then wait for completion
    val result = suspendCancellableCoroutine<Optional<Payloads>> { cont ->
      // If already completed (e.g., during replay), resume immediately
      if (completed) {
        if (completionException != null) {
          cont.resumeWithException(completionException!!)
        } else {
          cont.resume(completionResult!!)
        }
      } else {
        // Store continuation for the completion callback to use
        completionCont = cont
      }
    }

    return Pair(execution, result)
  }

  /**
   * Executes a side effect - a non-deterministic operation whose result is recorded.
   *
   * @param func the function to execute
   * @return the result payload
   */
  suspend fun sideEffect(
    func: () -> Optional<Payloads>
  ): Optional<Payloads> = suspendCancellableCoroutine { cont ->
    replayContext.sideEffect(
      { func() },
      null as UserMetadata?
    ) { result ->
      cont.resume(result)
    }
  }

  /**
   * Gets the version for a change ID, used for workflow versioning.
   *
   * @param changeId the change identifier
   * @param minSupported minimum supported version
   * @param maxSupported maximum supported version
   * @return the version to use
   */
  suspend fun getVersion(
    changeId: String,
    minSupported: Int,
    maxSupported: Int
  ): Int = suspendCancellableCoroutine { cont ->
    replayContext.getVersion(
      changeId,
      minSupported,
      maxSupported
    ) { version, exception ->
      if (exception != null) {
        cont.resumeWithException(exception)
      } else {
        cont.resume(version)
      }
    }
  }

  /**
   * Checks if cancellation has been requested for this workflow.
   */
  val isCancelRequested: Boolean
    get() = replayContext.isCancelRequested

  /**
   * Fails the current workflow task.
   */
  fun failWorkflowTask(failure: Throwable) {
    replayContext.failWorkflowTask(failure)
  }

  // ==================== Search Attributes ====================

  /**
   * Returns the current search attributes as a typed [SearchAttributes] object.
   */
  fun getTypedSearchAttributes(): SearchAttributes {
    val protoSearchAttributes = replayContext.searchAttributes
    return SearchAttributesUtil.decodeTyped(protoSearchAttributes)
  }

  /**
   * Updates search attributes by applying the given updates.
   *
   * @param updates the search attribute updates to apply
   */
  fun upsertTypedSearchAttributes(vararg updates: SearchAttributeUpdate<*>) {
    val protoSearchAttributes = SearchAttributesUtil.encodeTypedUpdates(*updates)
    replayContext.upsertSearchAttributes(protoSearchAttributes)
  }

  // ==================== Memo ====================

  /**
   * Gets a memo value by key.
   *
   * @param key the memo key
   * @param valueClass the expected value class
   * @return the memo value, or null if not found
   */
  fun <T> getMemo(key: String, valueClass: Class<T>): T? {
    val payload = replayContext.getMemo(key) ?: return null
    return dataConverter.fromPayload(payload, valueClass, valueClass)
  }

  /**
   * Updates workflow memo with the given key-value pairs.
   *
   * @param memo map of memo key-value pairs to upsert
   */
  fun upsertMemo(memo: Map<String, Any?>) {
    val memoBuilder = Memo.newBuilder()
    for ((key, value) in memo) {
      val payload = if (value != null) {
        dataConverter.toPayload(value).orElse(null)
      } else {
        null
      }
      if (payload != null) {
        memoBuilder.putFields(key, payload)
      }
    }
    replayContext.upsertMemo(memoBuilder.build())
  }

  // ==================== Cron/Continue-As-New Support ====================

  /**
   * Gets the result from the last successful run of this workflow.
   * Useful for cron workflows or continue-as-new chains.
   *
   * @param resultClass the expected result class
   * @return the last completion result, or null if none
   */
  fun <R> getLastCompletionResult(resultClass: Class<R>): R? {
    val payloads = replayContext.lastCompletionResult ?: return null
    return dataConverter.fromPayloads(0, Optional.of(payloads), resultClass, resultClass)
  }

  /**
   * Gets the failure from the previous run of this workflow, if any.
   * Useful for cron workflows or continue-as-new chains.
   *
   * @return the previous run failure, or null if the previous run succeeded
   */
  fun getPreviousRunFailure(): Exception? {
    val failure = replayContext.previousRunFailure ?: return null
    return RuntimeException(failure.message)
  }

  // ==================== Replay and Metrics ====================

  /**
   * Returns the metrics scope for this workflow.
   */
  fun getMetricsScope(): Scope {
    return replayContext.metricsScope
  }

  // ==================== Update Info ====================

  /**
   * Returns information about the currently executing update, if any.
   *
   * @return the current update info, or null if not in an update handler
   */
  fun getCurrentUpdateInfo(): UpdateInfo? {
    return currentUpdateInfo.get()
  }

  // ==================== Handler Completion Check ====================

  /**
   * Returns true if all signal and update handlers have completed.
   *
   * This is useful for ensuring graceful completion before continuing-as-new
   * or completing the workflow.
   *
   * @return true if all handlers have finished
   */
  fun isEveryHandlerFinished(): Boolean {
    return runningSignalHandlers.get() == 0 && runningUpdateHandlers.get() == 0
  }

  // ==================== Workflow Details ====================

  /**
   * Sets the current workflow details.
   *
   * Details are user-defined strings that can be used to provide
   * additional context about the workflow's current state.
   *
   * @param details the details string to set
   */
  fun setCurrentDetails(details: String?) {
    currentDetails = details
  }

  /**
   * Gets the current workflow details.
   *
   * @return the current details, or null if not set
   */
  fun getCurrentDetails(): String? {
    return currentDetails
  }

  // ==================== Mutable Side Effect ====================

  /**
   * Executes a mutable side effect.
   *
   * Similar to [sideEffect], but only records a new marker if the value has changed.
   * The function receives the previous value (if any) and returns the new value.
   *
   * @param id unique identifier for this mutable side effect
   * @param resultClass the expected result class
   * @param func function that takes the previous value and returns the new value
   * @return the result of the function
   */
  fun <R> mutableSideEffect(
    id: String,
    resultClass: Class<R>,
    func: (R?) -> R
  ): R {
    var unserializedResult: R? = null
    val resultHolder = AtomicReference<Optional<Payloads>>(Optional.empty())

    replayContext.mutableSideEffect(
      id,
      null, // userMetadata
      { storedValue: Optional<Payloads> ->
        // Deserialize previous value
        val previousValue: R? = if (storedValue.isPresent) {
          dataConverter.fromPayloads(0, storedValue, resultClass, resultClass)
        } else {
          null
        }
        // Execute user function
        val newValue = func(previousValue)
        unserializedResult = newValue

        // If value changed, return serialized new value; otherwise empty
        if (previousValue != newValue) {
          dataConverter.toPayloads(newValue)
        } else {
          Optional.empty()
        }
      },
      { resultPayloads: Optional<Payloads> ->
        // Callback with the final result
        resultHolder.set(resultPayloads)
      }
    )

    // Return the unserialized result if we have it (optimization)
    unserializedResult?.let { return it }

    // Otherwise deserialize from the result
    val resultPayloads = resultHolder.get()
    if (!resultPayloads.isPresent) {
      throw IllegalStateException("mutableSideEffect did not produce a result for id=$id")
    }
    return dataConverter.fromPayloads(0, resultPayloads, resultClass, resultClass)
  }

  // ==================== Higher-Level Activity Methods ====================

  /**
   * Executes an activity by name with options.
   *
   * @param activityName the activity type name
   * @param options the activity options
   * @param resultClass the expected result class
   * @param args arguments to pass to the activity
   * @return the activity result
   */
  suspend fun <R> executeActivityByName(
    activityName: String,
    options: ActivityOptions,
    resultClass: Class<R>,
    vararg args: Any?
  ): R {
    val input = serializeArgs(*args)
    val parameters = buildActivityParameters(activityName, options, input)
    val resultPayloads = executeActivity(parameters)
    return deserializeResult(resultPayloads, resultClass)
  }

  /**
   * Executes a local activity by name with options.
   *
   * @param activityName the activity type name
   * @param options the local activity options
   * @param resultClass the expected result class
   * @param args arguments to pass to the activity
   * @return the activity result
   */
  suspend fun <R> executeLocalActivityByName(
    activityName: String,
    options: LocalActivityOptions,
    resultClass: Class<R>,
    vararg args: Any?
  ): R {
    val input = serializeArgs(*args)
    val parameters = buildLocalActivityParameters(activityName, options, input)
    val resultPayloads = executeLocalActivity(parameters)
    return deserializeResult(resultPayloads, resultClass)
  }

  /**
   * Executes a child workflow by type name with options.
   *
   * @param workflowType the child workflow type name
   * @param options the child workflow options
   * @param resultClass the expected result class
   * @param args arguments to pass to the child workflow
   * @return the child workflow result
   */
  suspend fun <R> executeChildWorkflowByName(
    workflowType: String,
    options: ChildWorkflowOptions,
    resultClass: Class<R>,
    vararg args: Any?
  ): R {
    val input = serializeArgs(*args)
    val parameters = buildChildWorkflowParameters(workflowType, options, input)
    val (_, resultPayloads) = startChildWorkflow(parameters)
    return deserializeResult(resultPayloads, resultClass)
  }

  // ==================== Helper Methods ====================

  private fun serializeArgs(vararg args: Any?): Optional<Payloads> {
    return if (args.isEmpty()) {
      Optional.empty()
    } else {
      dataConverter.toPayloads(*args)
    }
  }

  private fun <R> deserializeResult(payloads: Optional<Payloads>, resultClass: Class<R>): R {
    @Suppress("UNCHECKED_CAST")
    return if (payloads.isPresent && resultClass != Unit::class.java && resultClass != Void.TYPE) {
      dataConverter.fromPayload(payloads.get().getPayloads(0), resultClass, resultClass) as R
    } else {
      null as R
    }
  }

  private fun buildActivityParameters(
    activityName: String,
    options: ActivityOptions,
    input: Optional<Payloads>
  ): ExecuteActivityParameters {
    val taskQueue = options.taskQueue ?: replayContext.taskQueue
    val attributes = ScheduleActivityTaskCommandAttributes.newBuilder()
      .setActivityType(ActivityType.newBuilder().setName(activityName))
      .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))

    options.scheduleToStartTimeout?.let {
      attributes.setScheduleToStartTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.startToCloseTimeout?.let {
      attributes.setStartToCloseTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.scheduleToCloseTimeout?.let {
      attributes.setScheduleToCloseTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.heartbeatTimeout?.let {
      attributes.setHeartbeatTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }

    input.ifPresent { attributes.setInput(it) }

    options.retryOptions?.let { retryOptions ->
      attributes.setRetryPolicy(toRetryPolicy(retryOptions))
    }

    val cancellationType = options.cancellationType ?: ActivityCancellationType.TRY_CANCEL
    return ExecuteActivityParameters(attributes, cancellationType, null)
  }

  private fun buildLocalActivityParameters(
    activityName: String,
    options: LocalActivityOptions,
    input: Optional<Payloads>
  ): ExecuteLocalActivityParameters {
    val validatedOptions = LocalActivityOptions.newBuilder(options).validateAndBuildWithDefaults()
    val originalScheduledTime = replayContext.currentTimeMillis()

    val activityTask = PollActivityTaskQueueResponse.newBuilder()
      .setActivityId(replayContext.randomUUID().toString())
      .setWorkflowNamespace(replayContext.namespace)
      .setWorkflowType(replayContext.workflowType)
      .setWorkflowExecution(replayContext.workflowExecution)
      .setScheduledTime(ProtobufTimeUtils.toProtoTimestamp(Instant.ofEpochMilli(originalScheduledTime)))
      .setActivityType(ActivityType.newBuilder().setName(activityName))
      .setAttempt(1)

    validatedOptions.scheduleToCloseTimeout?.let {
      activityTask.setScheduleToCloseTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    validatedOptions.startToCloseTimeout?.let {
      activityTask.setStartToCloseTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }

    input.ifPresent { activityTask.setInput(it) }

    validatedOptions.retryOptions?.let { retryOptions ->
      activityTask.setRetryPolicy(toRetryPolicy(RetryOptions.newBuilder(retryOptions).validateBuildWithDefaults()))
    }

    val localRetryThreshold = validatedOptions.localRetryThreshold
      ?: replayContext.workflowTaskTimeout.multipliedBy(3)

    return ExecuteLocalActivityParameters(
      activityTask,
      validatedOptions.scheduleToStartTimeout,
      originalScheduledTime,
      null, // previousLocalExecutionFailure
      validatedOptions.isDoNotIncludeArgumentsIntoMarker,
      localRetryThreshold,
      null // metadata
    )
  }

  private fun buildChildWorkflowParameters(
    workflowType: String,
    options: ChildWorkflowOptions,
    input: Optional<Payloads>
  ): StartChildWorkflowExecutionParameters {
    val workflowId = options.workflowId ?: "${replayContext.workflowId}_${replayContext.randomUUID()}"
    val taskQueue = options.taskQueue ?: replayContext.taskQueue

    val attributes = StartChildWorkflowExecutionCommandAttributes.newBuilder()
      .setWorkflowId(workflowId)
      .setWorkflowType(WorkflowType.newBuilder().setName(workflowType).build())
      .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))

    input.ifPresent { attributes.setInput(it) }

    options.workflowExecutionTimeout?.let {
      attributes.setWorkflowExecutionTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.workflowRunTimeout?.let {
      attributes.setWorkflowRunTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.workflowTaskTimeout?.let {
      attributes.setWorkflowTaskTimeout(ProtobufTimeUtils.toProtoDuration(it))
    }

    val cancellationType = options.cancellationType ?: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED

    return StartChildWorkflowExecutionParameters(attributes, cancellationType, null)
  }

  private fun toRetryPolicy(options: RetryOptions): io.temporal.api.common.v1.RetryPolicy {
    val builder = io.temporal.api.common.v1.RetryPolicy.newBuilder()

    options.initialInterval?.let {
      builder.setInitialInterval(ProtobufTimeUtils.toProtoDuration(it))
    }
    options.maximumInterval?.let {
      builder.setMaximumInterval(ProtobufTimeUtils.toProtoDuration(it))
    }
    builder.setBackoffCoefficient(options.backoffCoefficient)
    builder.setMaximumAttempts(options.maximumAttempts)

    options.doNotRetry?.let { doNotRetry ->
      builder.addAllNonRetryableErrorTypes(doNotRetry.toList())
    }

    return builder.build()
  }

  // ==================== Condition/Await Methods ====================

  /**
   * List of pending condition waiters that need to be notified when events arrive.
   */
  internal val conditionWaiters = mutableListOf<CancellableContinuation<Unit>>()

  /**
   * Awaits until the given condition evaluates to true.
   *
   * This method suspends the coroutine and checks the condition after each
   * workflow event (signal, timer, activity completion, etc.).
   *
   * @param condition the condition to wait for
   */
  suspend fun awaitCondition(condition: () -> Boolean) {
    // Check if condition is already true
    while (!condition()) {
      // Suspend until something happens (signal, timer, etc.)
      suspendCancellableCoroutine<Unit> { cont ->
        conditionWaiters.add(cont)
        cont.invokeOnCancellation {
          conditionWaiters.remove(cont)
        }
      }
    }
  }

  /**
   * Awaits until the given condition evaluates to true or timeout expires.
   *
   * @param timeout maximum time to wait
   * @param condition the condition to wait for
   * @return true if condition was satisfied, false if timeout expired
   */
  suspend fun awaitCondition(timeout: Duration, condition: () -> Boolean): Boolean {
    // Check if condition is already true
    if (condition()) return true

    val startTime = currentTimeMillis
    val timeoutMillis = timeout.toMillis()

    while (!condition()) {
      val elapsed = currentTimeMillis - startTime
      if (elapsed >= timeoutMillis) {
        return false
      }

      // Wait for either the condition to be signaled or timeout
      // Use the same conditionWaiters list but with Unit type
      suspendCancellableCoroutine<Unit> { cont ->
        conditionWaiters.add(cont)

        // Also set up a timer to wake us up for timeout check
        val remaining = timeoutMillis - elapsed
        val timerDuration = Duration.ofMillis(remaining.coerceAtMost(100)) // Check every 100ms at most
        replayContext.newTimer(timerDuration, null) { exception ->
          // When timer fires, resume to re-check the condition
          if (cont.isActive) {
            conditionWaiters.remove(cont)
            if (exception != null) {
              cont.resumeWithException(exception)
            } else {
              cont.resume(Unit)
            }
          }
        }

        cont.invokeOnCancellation {
          conditionWaiters.remove(cont)
        }
      }
    }

    return true
  }

  /**
   * Notifies all condition waiters that they should re-check their conditions.
   * Called by the dispatcher after all ready tasks have been processed.
   *
   * Uses explicit dispatch to add resumptions to the end of the queue,
   * ensuring all pending work completes before condition waiters
   * re-check their conditions.
   */
  internal fun notifyConditionWaiters() {
    val waiters = conditionWaiters.toList()
    conditionWaiters.clear()
    val disp = dispatcher
    waiters.forEach { cont ->
      if (cont.isActive) {
        // Explicitly dispatch to add to end of queue, avoiding inline execution
        // This ensures other pending work (like signal handlers) completes first
        if (disp != null) {
          disp.dispatch(cont.context, Runnable { cont.resume(Unit) })
        } else {
          cont.resume(Unit)
        }
      }
    }
  }

  // ==================== Signal Handler Registration ====================

  /**
   * Registers a signal handler for a specific signal name.
   *
   * @param signalName the name of the signal to handle
   * @param handler the handler function to invoke when the signal is received
   */
  fun registerSignalHandler(signalName: String, handler: SignalHandler) {
    if (signalHandlers.containsKey(signalName)) {
      throw IllegalArgumentException("Signal handler already registered for: $signalName")
    }
    signalHandlers[signalName] = handler
  }

  /**
   * Registers a dynamic signal handler for all unhandled signals.
   *
   * @param handler the handler function to invoke for unhandled signals
   */
  fun registerDynamicSignalHandler(handler: DynamicSignalHandler) {
    if (dynamicSignalHandler != null) {
      throw IllegalArgumentException("Dynamic signal handler already registered")
    }
    dynamicSignalHandler = handler
  }

  // ==================== Query Handler Registration ====================

  /**
   * Registers a query handler for a specific query name.
   *
   * @param queryName the name of the query to handle
   * @param handler the handler function to invoke when the query is received
   */
  fun <R> registerQueryHandler(queryName: String, handler: QueryHandler<R>) {
    if (queryHandlers.containsKey(queryName)) {
      throw IllegalArgumentException("Query handler already registered for: $queryName")
    }
    queryHandlers[queryName] = handler
  }

  /**
   * Registers a dynamic query handler for all unhandled queries.
   *
   * @param handler the handler function to invoke for unhandled queries
   */
  fun registerDynamicQueryHandler(handler: DynamicQueryHandler) {
    if (dynamicQueryHandler != null) {
      throw IllegalArgumentException("Dynamic query handler already registered")
    }
    dynamicQueryHandler = handler
  }

  /**
   * Creates KEncodedValues from an Optional<Payloads>.
   */
  fun createEncodedValues(payloads: Optional<Payloads>): KEncodedValues {
    return KEncodedValues(EncodedValues(payloads, dataConverter))
  }

  // ==================== Update Handler Registration ====================

  /**
   * Registers an update handler for a specific update name.
   *
   * @param updateName the name of the update to handle
   * @param handler the handler function to invoke when the update is received
   */
  fun registerUpdateHandler(updateName: String, handler: UpdateHandler) {
    if (updateHandlers.containsKey(updateName)) {
      throw IllegalArgumentException("Update handler already registered for: $updateName")
    }
    updateHandlers[updateName] = handler
  }

  /**
   * Registers an update validator for a specific update name.
   *
   * @param updateName the name of the update to validate
   * @param validator the validator function to invoke before the handler
   */
  fun registerUpdateValidator(updateName: String, validator: UpdateValidator) {
    if (updateValidators.containsKey(updateName)) {
      throw IllegalArgumentException("Update validator already registered for: $updateName")
    }
    updateValidators[updateName] = validator
  }

  /**
   * Registers a dynamic update handler for all unhandled updates.
   *
   * @param handler the handler function to invoke for unhandled updates
   */
  fun registerDynamicUpdateHandler(handler: DynamicUpdateHandler) {
    if (dynamicUpdateHandler != null) {
      throw IllegalArgumentException("Dynamic update handler already registered")
    }
    dynamicUpdateHandler = handler
  }

  /**
   * Registers a dynamic update validator for all unhandled updates.
   *
   * @param validator the validator function to invoke for unhandled updates
   */
  fun registerDynamicUpdateValidator(validator: DynamicUpdateValidator) {
    if (dynamicUpdateValidator != null) {
      throw IllegalArgumentException("Dynamic update validator already registered")
    }
    dynamicUpdateValidator = validator
  }

  // ==================== Child Workflow Handle Methods ====================

  /**
   * Starts a child workflow and returns a handle for interaction.
   *
   * @param workflowType the child workflow type name
   * @param options the child workflow options
   * @param resultClass the expected result class
   * @param args arguments to pass to the child workflow
   * @return a handle for interacting with the child workflow
   */
  suspend fun <T, R> startChildWorkflowWithHandle(
    workflowType: String,
    options: ChildWorkflowOptions,
    resultClass: Class<R>,
    vararg args: Any?
  ): io.temporal.kotlin.workflow.KChildWorkflowHandle<T, R> {
    val input = serializeArgs(*args)
    val parameters = buildChildWorkflowParameters(workflowType, options, input)

    // Use a holder to pass the completion result/exception between callbacks
    var completionResult: Optional<Payloads>? = null
    var completionException: Exception? = null
    var completionCont: CancellableContinuation<Optional<Payloads>>? = null
    var completed = false

    // First, wait for the child to start
    val execution = suspendCancellableCoroutine<WorkflowExecution> { startCont ->
      val cancellationHandle = replayContext.startChildWorkflow(
        parameters,
        { execution: WorkflowExecution?, startException: Exception? ->
          if (startException != null) {
            startCont.resumeWithException(startException)
          } else if (execution != null) {
            startCont.resume(execution)
          }
        },
        { result: Optional<Payloads>, exception: Exception? ->
          // Store completion data
          completionResult = result
          completionException = exception
          completed = true
          // If completion continuation is already waiting, resume it
          completionCont?.let { cont ->
            if (exception != null) {
              cont.resumeWithException(exception)
            } else {
              cont.resume(result)
            }
          }
        }
      )

      startCont.invokeOnCancellation { cause ->
        cancellationHandle.apply(
          cause as? Exception
            ?: RuntimeException(cause?.message ?: "Child workflow cancelled")
        )
      }
    }

    // Create a suspend function that waits for completion
    val resultProvider: suspend () -> Optional<Payloads> = {
      if (completed) {
        if (completionException != null) {
          throw completionException!!
        }
        completionResult!!
      } else {
        suspendCancellableCoroutine { cont ->
          completionCont = cont
        }
      }
    }

    @OptIn(InternalTemporalApi::class)
    return io.temporal.kotlin.workflow.KChildWorkflowHandle(
      workflowId = execution.workflowId,
      firstExecutionRunId = execution.runId,
      resultClass = resultClass,
      context = this,
      dataConverter = dataConverter,
      resultProvider = resultProvider
    )
  }

  /**
   * Gets a handle to an existing child workflow by workflow ID.
   *
   * @param workflowId the child workflow's workflow ID
   * @param resultClass the expected result class
   * @return a handle for interacting with the child workflow
   */
  @OptIn(InternalTemporalApi::class)
  fun <T, R> getChildWorkflowHandle(
    workflowId: String,
    resultClass: Class<R>
  ): io.temporal.kotlin.workflow.KChildWorkflowHandle<T, R> {
    // For existing child workflows, we don't have the result provider
    // This is a simplified implementation that throws on result()
    return io.temporal.kotlin.workflow.KChildWorkflowHandle(
      workflowId = workflowId,
      firstExecutionRunId = "", // Unknown for existing workflows
      resultClass = resultClass,
      context = this,
      dataConverter = dataConverter,
      resultProvider = {
        throw UnsupportedOperationException(
          "Cannot get result from a handle obtained via getChildWorkflowHandle. " +
            "Use startChildWorkflow to get a handle that can await results."
        )
      }
    )
  }

  // ==================== Async Execution ====================

  /**
   * Launches a coroutine in the workflow context and returns immediately.
   *
   * The block starts executing immediately (eager execution).
   * Returns a [Deferred] that can be used to await the result.
   *
   * @param block the suspend function to execute asynchronously
   * @return a [Deferred] representing the result of the async operation
   * @throws IllegalStateException if coroutine scope is not initialized
   */
  fun <T> async(block: suspend () -> T): Deferred<T> {
    val scope = coroutineScope
      ?: throw IllegalStateException("Coroutine scope not initialized")

    return scope.async { block() }
  }

  // ==================== Continue-As-New ====================

  /**
   * Continues the workflow as a new execution with the given arguments.
   *
   * This method sets up the continue-as-new command and throws a
   * [ContinueAsNewException] to signal the workflow should complete.
   *
   * @param workflowType optional workflow type (null uses current type)
   * @param options optional continue-as-new options
   * @param args arguments to pass to the new execution
   * @throws ContinueAsNewException always
   */
  fun continueAsNew(
    workflowType: String?,
    options: io.temporal.workflow.ContinueAsNewOptions?,
    vararg args: Any?
  ): Nothing {
    val attributes = io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.newBuilder()

    // Set workflow type (use current if not specified)
    if (workflowType != null) {
      attributes.setWorkflowType(WorkflowType.newBuilder().setName(workflowType))
    }

    // Apply options if provided
    options?.let { opts ->
      opts.workflowRunTimeout?.let {
        attributes.setWorkflowRunTimeout(ProtobufTimeUtils.toProtoDuration(it))
      }
      opts.workflowTaskTimeout?.let {
        attributes.setWorkflowTaskTimeout(ProtobufTimeUtils.toProtoDuration(it))
      }
      opts.taskQueue?.takeIf { it.isNotEmpty() }?.let {
        attributes.setTaskQueue(TaskQueue.newBuilder().setName(it))
      }
      opts.retryOptions?.let { retryOpts ->
        attributes.setRetryPolicy(toRetryPolicy(RetryOptions.newBuilder(retryOpts).validateBuildWithDefaults()))
      }
      opts.memo?.takeIf { it.isNotEmpty() }?.let { memo ->
        attributes.setMemo(
          io.temporal.api.common.v1.Memo.newBuilder()
            .putAllFields(
              memo.mapValues { (_, v) ->
                dataConverter.toPayload(v).orElse(null)
              }.filterValues { it != null }
            )
        )
      }
    }

    // Serialize arguments
    val input = serializeArgs(*args)
    input.ifPresent { attributes.setInput(it) }

    // Register continue-as-new with the replay context
    replayContext.continueAsNewOnCompletion(attributes.build())

    // Throw exception to unwind the coroutine stack
    throw ContinueAsNewException("Workflow is continuing as new")
  }
}

/**
 * Exception thrown to signal that the workflow should continue as new.
 * This exception is caught by the workflow runner and converted to a
 * continue-as-new completion.
 */
@InternalTemporalApi
class ContinueAsNewException(message: String) : CancellationException(message)
