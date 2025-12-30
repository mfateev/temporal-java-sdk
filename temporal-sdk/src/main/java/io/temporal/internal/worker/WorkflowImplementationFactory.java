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

package io.temporal.internal.worker;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.internal.replay.ReplayWorkflowFactory;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Internal interface for pluggable workflow execution models.
 *
 * <p>This interface allows external modules (such as temporal-kotlin) to provide custom workflow
 * execution implementations while integrating seamlessly with the Temporal Java SDK worker
 * infrastructure.
 *
 * <p>Implementations of this interface are responsible for:
 *
 * <ul>
 *   <li>Managing workflow type registration
 *   <li>Creating workflow instances for execution
 *   <li>Providing workflow type metadata to the worker
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Custom workflow implementation factories can be registered with a worker to provide
 * alternative workflow execution models. For example, the Kotlin SDK uses this to provide
 * coroutine-based workflow execution.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Implementations must be thread-safe as methods may be called concurrently from multiple worker
 * threads.
 *
 * @see io.temporal.worker.Worker#registerWorkflowImplementationFactory
 */
public interface WorkflowImplementationFactory extends ReplayWorkflowFactory {

  /**
   * Creates a workflow instance for the given workflow type and execution.
   *
   * <p>This method is called by the worker when a workflow task is received and a workflow instance
   * needs to be created or restored.
   *
   * <p>Implementations should:
   *
   * <ul>
   *   <li>Return {@code null} if this factory does not handle the given workflow type
   *   <li>Return a fully initialized {@link ReplayWorkflow} instance if handled
   *   <li>Throw an exception only for unrecoverable errors
   * </ul>
   *
   * @param workflowType the type of workflow to create, as registered with the Temporal service
   * @param workflowExecution the execution context containing workflow and run IDs
   * @return a {@link ReplayWorkflow} instance to handle this workflow, or {@code null} if this
   *     factory does not handle the given workflow type
   * @throws Exception if an unrecoverable error occurs during workflow creation
   */
  @Nullable
  @Override
  ReplayWorkflow getWorkflow(
      @Nonnull WorkflowType workflowType, @Nonnull WorkflowExecution workflowExecution)
      throws Exception;

  /**
   * Returns the set of workflow type names registered with this factory.
   *
   * <p>This method is used by the worker to:
   *
   * <ul>
   *   <li>Report registered workflow types to the Temporal service
   *   <li>Validate that workflow types are not registered with multiple factories
   *   <li>Provide diagnostic information
   * </ul>
   *
   * @return an unmodifiable set of workflow type names, never {@code null}
   */
  @Nonnull
  Set<String> getRegisteredWorkflowTypes();

  /**
   * Returns whether this factory has any workflow types registered.
   *
   * <p>This is used by the worker to determine if the factory should be consulted during workflow
   * task processing. A factory with no registered types may be skipped for efficiency.
   *
   * @return {@code true} if at least one workflow type is registered, {@code false} otherwise
   */
  @Override
  default boolean isAnyTypeSupported() {
    return !getRegisteredWorkflowTypes().isEmpty();
  }
}
