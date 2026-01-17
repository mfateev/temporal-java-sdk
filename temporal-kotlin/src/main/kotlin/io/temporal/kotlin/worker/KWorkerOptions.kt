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

package io.temporal.kotlin.worker

import io.temporal.worker.WorkerOptions
import io.temporal.worker.WorkflowImplementationOptions
import io.temporal.worker.tuning.WorkerTuner
import java.time.Duration
import kotlin.reflect.KClass

/**
 * Options for configuring a [KWorker].
 *
 * Workflows and activities are specified at construction time, following
 * the Python/.NET SDK pattern for simplified worker setup.
 *
 * Example:
 * ```kotlin
 * val worker = KWorker(
 *     client,
 *     KWorkerOptions(
 *         taskQueue = "my-task-queue",
 *         workflows = listOf(
 *             GreetingWorkflowImpl::class,
 *             OrderWorkflowImpl::class
 *         ),
 *         activities = listOf(
 *             GreetingActivitiesImpl(),
 *             OrderActivitiesImpl()
 *         ),
 *         maxConcurrentActivityExecutionSize = 100
 *     )
 * )
 * ```
 *
 * @property taskQueue The task queue name this worker listens on
 * @property workflows Workflow implementation classes to register
 * @property activities Activity implementation instances to register
 * @property workflowImplementationOptions Options for workflow implementations
 * @property maxConcurrentActivityExecutionSize Maximum number of activities executed in parallel
 * @property maxConcurrentWorkflowTaskExecutionSize Maximum number of workflow tasks executed in parallel
 * @property maxConcurrentLocalActivityExecutionSize Maximum number of local activities executed in parallel
 * @property maxConcurrentNexusExecutionSize Maximum number of nexus tasks executed in parallel
 * @property maxWorkerActivitiesPerSecond Maximum activities started per second by this worker
 * @property maxTaskQueueActivitiesPerSecond Maximum activities started per second for the task queue
 * @property maxConcurrentWorkflowTaskPollers Maximum number of workflow task pollers
 * @property maxConcurrentActivityTaskPollers Maximum number of activity task pollers
 * @property maxConcurrentNexusTaskPollers Maximum number of nexus task pollers
 * @property localActivityWorkerOnly If true, only process local activities
 * @property defaultDeadlockDetectionTimeout Deadlock detection timeout in milliseconds
 * @property maxHeartbeatThrottleInterval Maximum interval between heartbeats
 * @property defaultHeartbeatThrottleInterval Default interval between heartbeats
 * @property stickyQueueScheduleToStartTimeout Sticky queue schedule to start timeout
 * @property disableEagerExecution Whether to disable eager activity execution
 * @property buildId Build ID for worker versioning
 * @property useBuildIdForVersioning Whether to use build ID for versioning
 * @property stickyTaskQueueDrainTimeout Timeout for draining sticky task queue on shutdown
 * @property workerTuner Custom worker tuner for resource management
 * @property identity Worker identity override
 */
public data class KWorkerOptions(
  val taskQueue: String,
  val workflows: List<KClass<*>> = emptyList(),
  val activities: List<Any> = emptyList(),
  val workflowImplementationOptions: WorkflowImplementationOptions? = null,
  val maxConcurrentActivityExecutionSize: Int? = null,
  val maxConcurrentWorkflowTaskExecutionSize: Int? = null,
  val maxConcurrentLocalActivityExecutionSize: Int? = null,
  val maxConcurrentNexusExecutionSize: Int? = null,
  val maxWorkerActivitiesPerSecond: Double? = null,
  val maxTaskQueueActivitiesPerSecond: Double? = null,
  val maxConcurrentWorkflowTaskPollers: Int? = null,
  val maxConcurrentActivityTaskPollers: Int? = null,
  val maxConcurrentNexusTaskPollers: Int? = null,
  val localActivityWorkerOnly: Boolean? = null,
  val defaultDeadlockDetectionTimeout: Long? = null,
  val maxHeartbeatThrottleInterval: Duration? = null,
  val defaultHeartbeatThrottleInterval: Duration? = null,
  val stickyQueueScheduleToStartTimeout: Duration? = null,
  val disableEagerExecution: Boolean? = null,
  val buildId: String? = null,
  val useBuildIdForVersioning: Boolean? = null,
  val stickyTaskQueueDrainTimeout: Duration? = null,
  val workerTuner: WorkerTuner? = null,
  val identity: String? = null
) {
  /**
   * Converts this KWorkerOptions to Java WorkerOptions.
   *
   * @return WorkerOptions configured with the values from this data class
   */
  internal fun toWorkerOptions(): WorkerOptions {
    val builder = WorkerOptions.newBuilder()

    maxConcurrentActivityExecutionSize?.let { builder.setMaxConcurrentActivityExecutionSize(it) }
    maxConcurrentWorkflowTaskExecutionSize?.let { builder.setMaxConcurrentWorkflowTaskExecutionSize(it) }
    maxConcurrentLocalActivityExecutionSize?.let { builder.setMaxConcurrentLocalActivityExecutionSize(it) }
    maxConcurrentNexusExecutionSize?.let { builder.setMaxConcurrentNexusExecutionSize(it) }
    maxWorkerActivitiesPerSecond?.let { builder.setMaxWorkerActivitiesPerSecond(it) }
    maxTaskQueueActivitiesPerSecond?.let { builder.setMaxTaskQueueActivitiesPerSecond(it) }
    maxConcurrentWorkflowTaskPollers?.let { builder.setMaxConcurrentWorkflowTaskPollers(it) }
    maxConcurrentActivityTaskPollers?.let { builder.setMaxConcurrentActivityTaskPollers(it) }
    maxConcurrentNexusTaskPollers?.let { builder.setMaxConcurrentNexusTaskPollers(it) }
    localActivityWorkerOnly?.let { builder.setLocalActivityWorkerOnly(it) }
    defaultDeadlockDetectionTimeout?.let { builder.setDefaultDeadlockDetectionTimeout(it) }
    maxHeartbeatThrottleInterval?.let { builder.setMaxHeartbeatThrottleInterval(it) }
    defaultHeartbeatThrottleInterval?.let { builder.setDefaultHeartbeatThrottleInterval(it) }
    stickyQueueScheduleToStartTimeout?.let { builder.setStickyQueueScheduleToStartTimeout(it) }
    disableEagerExecution?.let { builder.setDisableEagerExecution(it) }
    buildId?.let { builder.setBuildId(it) }
    useBuildIdForVersioning?.let { builder.setUseBuildIdForVersioning(it) }
    stickyTaskQueueDrainTimeout?.let { builder.setStickyTaskQueueDrainTimeout(it) }
    workerTuner?.let { builder.setWorkerTuner(it) }
    identity?.let { builder.setIdentity(it) }

    return builder.build()
  }
}
