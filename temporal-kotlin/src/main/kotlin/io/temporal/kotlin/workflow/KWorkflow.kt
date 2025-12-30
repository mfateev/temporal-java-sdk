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

@file:OptIn(InternalTemporalApi::class)

package io.temporal.kotlin.workflow

import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.KotlinWorkflowContext
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInfo
import java.time.Instant
import java.util.Random
import java.util.UUID

/**
 * Provides access to Temporal workflow APIs from within Kotlin workflow code.
 *
 * This object provides Kotlin-friendly wrappers for Temporal workflow operations
 * including time, random number generation, versioning, and side effects.
 *
 * All methods must be called from within workflow code only.
 *
 * Example:
 * ```kotlin
 * @WorkflowInterface
 * interface MyWorkflow {
 *   @WorkflowMethod
 *   suspend fun execute(): String
 * }
 *
 * class MyWorkflowImpl : MyWorkflow {
 *   override suspend fun execute(): String {
 *     val info = KWorkflow.getInfo()
 *     val currentTime = KWorkflow.currentTime()
 *     return "Workflow ${info.workflowId} at $currentTime"
 *   }
 * }
 * ```
 */
public object KWorkflow {

  /**
   * Thread-local storage for the current Kotlin workflow context.
   * This is set by the workflow runner when executing workflow code.
   */
  @InternalTemporalApi
  internal val currentContext = ThreadLocal<KotlinWorkflowContext?>()

  /**
   * Returns information about the current workflow execution.
   *
   * @return the workflow information with Kotlin-friendly nullable types
   * @throws IllegalStateException if called outside of workflow code
   */
  public fun getInfo(): KWorkflowInfo {
    // For now, delegate to the Java SDK's Workflow.getInfo()
    // Once we have full Kotlin context integration, we can use our own context
    val javaInfo: WorkflowInfo = Workflow.getInfo()
    return KWorkflowInfoImpl(javaInfo)
  }

  /**
   * Returns the current workflow time as an [Instant].
   *
   * This is deterministic and returns the same value during replay.
   * Must be used instead of [System.currentTimeMillis] or [Instant.now]
   * to ensure deterministic workflow execution.
   *
   * @return the current workflow time
   */
  public fun currentTime(): Instant {
    return Instant.ofEpochMilli(Workflow.currentTimeMillis())
  }

  /**
   * Returns the current workflow time in milliseconds since epoch.
   *
   * This is deterministic and returns the same value during replay.
   *
   * @return current time in milliseconds
   */
  public fun currentTimeMillis(): Long {
    return Workflow.currentTimeMillis()
  }

  /**
   * Generates a deterministic UUID.
   *
   * Must be used instead of [UUID.randomUUID] to ensure deterministic
   * workflow execution during replay.
   *
   * @return a deterministic UUID
   */
  public fun randomUUID(): UUID {
    return Workflow.randomUUID()
  }

  /**
   * Returns a deterministic random number generator.
   *
   * Must be used instead of [java.util.Random] or other random generators
   * to ensure deterministic workflow execution during replay.
   *
   * @return a deterministic random generator
   */
  public fun newRandom(): Random {
    return Workflow.newRandom()
  }

  /**
   * Gets a version for a particular change.
   *
   * Used to safely perform backwards incompatible changes to workflow
   * definitions. Not supported for the replaying code that has already
   * recorded activity calls for the change-id.
   *
   * @param changeId identifier of a particular change
   * @param minSupported minimum supported version
   * @param maxSupported maximum supported version
   * @return the version to use for the change
   */
  public fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    return Workflow.getVersion(changeId, minSupported, maxSupported)
  }

  /**
   * Executes a non-deterministic function and records its result.
   *
   * Use this for operations like generating random values from external
   * sources or getting the current date/time from system clock.
   *
   * The function is executed only during the first workflow execution.
   * During replay, the recorded value is returned without re-executing
   * the function.
   *
   * Warning: Do not use sideEffect to modify workflow state.
   * Only use the returned value.
   *
   * Example:
   * ```kotlin
   * val randomValue = KWorkflow.sideEffect {
   *   SecureRandom().nextInt(100)
   * }
   * ```
   *
   * @param R the result type
   * @param resultClass the class of the result type
   * @param func the function to execute
   * @return the result of the function (or recorded value during replay)
   */
  public fun <R> sideEffect(resultClass: Class<R>, func: () -> R): R {
    return Workflow.sideEffect(resultClass) { func() }
  }

  /**
   * Reified version of [sideEffect] for easier Kotlin usage.
   *
   * Example:
   * ```kotlin
   * val randomValue: Int = KWorkflow.sideEffect {
   *   SecureRandom().nextInt(100)
   * }
   * ```
   *
   * @param R the result type
   * @param func the function to execute
   * @return the result of the function (or recorded value during replay)
   */
  public inline fun <reified R> sideEffect(noinline func: () -> R): R {
    return sideEffect(R::class.java, func)
  }

  /**
   * Checks if cancellation has been requested for this workflow.
   *
   * @return true if cancellation has been requested
   */
  public fun isCancelRequested(): Boolean {
    return try {
      val context = currentContext.get()
      context?.isCancelRequested ?: false
    } catch (e: Exception) {
      false
    }
  }

  /**
   * Default version constant for workflow versioning.
   *
   * Use this as minSupported when introducing a new version
   * to allow workflows that haven't recorded any version yet.
   */
  public const val DEFAULT_VERSION: Int = Workflow.DEFAULT_VERSION
}
