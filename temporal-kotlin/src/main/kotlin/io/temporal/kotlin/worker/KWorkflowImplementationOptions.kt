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

import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KLocalActivityOptions
import io.temporal.workflow.NexusServiceOptions
import kotlin.reflect.KClass

/**
 * Options for configuring workflow implementations.
 *
 * This is the Kotlin equivalent of [io.temporal.worker.WorkflowImplementationOptions].
 *
 * Example:
 * ```kotlin
 * val options = KWorkflowImplementationOptions(
 *     failWorkflowExceptionTypes = listOf(IllegalArgumentException::class),
 *     defaultActivityOptions = KActivityOptions(
 *         startToCloseTimeout = 30.seconds
 *     ),
 *     activityOptions = mapOf(
 *         "sendEmail" to KActivityOptions(startToCloseTimeout = 60.seconds)
 *     )
 * )
 * ```
 *
 * @property failWorkflowExceptionTypes List of exception types that should cause the workflow to
 *           fail instead of blocking. The default behavior is to fail workflow on
 *           [io.temporal.failure.TemporalFailure] or any of its subclasses. Any other exceptions
 *           thrown from workflow code are treated as bugs that can be fixed by a new deployment,
 *           so the workflow is not failed but stuck in a retry loop trying to execute the code
 *           that led to the unexpected exception. This option allows specifying additional
 *           exception types which should lead to workflow failure instead of blockage.
 *           To fail workflow on any exception, add [Throwable::class] to this list.
 *
 * @property activityOptions Activity options per activity type name. These options are merged
 *           with those provided to [io.temporal.workflow.Workflow.newActivityStub] which have
 *           the highest precedence.
 *
 * @property defaultActivityOptions Default activity options used when no specific options are
 *           provided for an activity type. These have the lowest precedence and are overwritten
 *           by options passed to newActivityStub and by per-activity-type options.
 *
 * @property localActivityOptions Local activity options per activity type name. These options
 *           are merged with those provided to [io.temporal.workflow.Workflow.newLocalActivityStub]
 *           which have the highest precedence.
 *
 * @property defaultLocalActivityOptions Default local activity options used when no specific
 *           options are provided for a local activity type. These have the lowest precedence
 *           and are overwritten by options passed to newLocalActivityStub and by
 *           per-activity-type options.
 *
 * @property nexusServiceOptions Nexus service options per service name. These options are merged
 *           with those provided to [io.temporal.workflow.Workflow.newNexusServiceStub] which have
 *           the highest precedence.
 *
 * @property defaultNexusServiceOptions Default Nexus service options used when no specific options
 *           are provided for a service. Used for stubs created with newNexusServiceStub without
 *           options.
 *
 * @property enableUpsertVersionSearchAttributes When true, enables upserting version search
 *           attributes on [io.temporal.workflow.Workflow.getVersion]. This causes the SDK to
 *           automatically add the TemporalChangeVersion search attributes to the workflow when
 *           getVersion is called. This search attribute is a keyword list of all the getVersion
 *           calls made in the workflow. The format of each entry is "ChangeID-Version".
 *           This allows for easy discovery of what versions are being used in your namespace.
 *           Note: This change is backwards compatible, so it is safe to enable or disable this
 *           option with running workflows. However, if this option is enabled, it is not safe to
 *           rollback to a previous version of the SDK that does not support this option.
 *           The default value is false.
 */
public data class KWorkflowImplementationOptions(
  val failWorkflowExceptionTypes: List<KClass<out Throwable>> = emptyList(),
  val activityOptions: Map<String, KActivityOptions> = emptyMap(),
  val defaultActivityOptions: KActivityOptions? = null,
  val localActivityOptions: Map<String, KLocalActivityOptions> = emptyMap(),
  val defaultLocalActivityOptions: KLocalActivityOptions? = null,
  val nexusServiceOptions: Map<String, NexusServiceOptions> = emptyMap(),
  val defaultNexusServiceOptions: NexusServiceOptions? = null,
  /**
   * This property is experimental and may be changed or removed without notice.
   */
  val enableUpsertVersionSearchAttributes: Boolean = false
) {
  public companion object {
    /**
     * Default instance with all default values.
     */
    @JvmField
    public val DEFAULT: KWorkflowImplementationOptions = KWorkflowImplementationOptions()
  }
}
