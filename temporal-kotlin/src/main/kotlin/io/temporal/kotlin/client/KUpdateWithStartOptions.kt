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

package io.temporal.kotlin.client

import io.temporal.client.WorkflowUpdateStage

/**
 * Options for update-with-start operations.
 *
 * Bundles the workflow start operation with update-specific options for atomic execution.
 *
 * Example:
 * ```kotlin
 * val startOp = client.newWithStartWorkflowOperation(
 *     OrderWorkflow::processOrder,
 *     KWorkflowOptions(
 *         workflowId = "order-123",
 *         taskQueue = "orders",
 *         workflowIdConflictPolicy = WorkflowIdConflictPolicy.USE_EXISTING
 *     ),
 *     order
 * )
 *
 * // Execute and wait for update completion
 * val result = client.executeUpdateWithStart(
 *     OrderWorkflow::addItem,
 *     KUpdateWithStartOptions(startWorkflowOperation = startOp),
 *     newItem
 * )
 *
 * // Or start async and return after acceptance
 * val handle = client.startUpdateWithStart(
 *     OrderWorkflow::addItem,
 *     KUpdateWithStartOptions(
 *         startWorkflowOperation = startOp,
 *         waitForStage = WorkflowUpdateStage.ACCEPTED
 *     ),
 *     newItem
 * )
 * val result = handle.result()
 * ```
 *
 * @param T the workflow interface type
 * @param R the workflow result type
 * @param UR the update result type
 * @property startWorkflowOperation The workflow start operation (required)
 * @property waitForStage Stage to wait for before returning (required for startUpdateWithStart).
 *                        Must be ACCEPTED or COMPLETED (ADMITTED is not allowed).
 *                        Default is ACCEPTED.
 * @property updateId Optional update ID for idempotency. If not provided, a UUID will be generated.
 */
public data class KUpdateWithStartOptions<T, R, UR>(
  val startWorkflowOperation: KWithStartWorkflowOperation<T, R>,
  val waitForStage: WorkflowUpdateStage = WorkflowUpdateStage.ACCEPTED,
  val updateId: String? = null
)
