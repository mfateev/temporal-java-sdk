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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A composite factory that delegates workflow creation to registered factories.
 *
 * <p>This factory aggregates multiple {@link WorkflowImplementationFactory} instances and a default
 * factory. When {@link #getWorkflow(WorkflowType, WorkflowExecution)} is called, it iterates
 * through the registered factories in order and returns the first non-null result. If no factory
 * can handle the workflow type, it delegates to the default factory.
 *
 * <p>This design supports pluggable workflow execution models, allowing external modules (such as
 * temporal-kotlin) to provide custom workflow implementations that integrate with the standard Java
 * SDK worker infrastructure.
 *
 * <p>Thread Safety: This class is thread-safe as it only holds immutable references after
 * construction.
 */
final class CompositeReplayWorkflowFactory implements ReplayWorkflowFactory {

  private final List<WorkflowImplementationFactory> factories;
  private final ReplayWorkflowFactory defaultFactory;

  /**
   * Creates a new composite factory.
   *
   * <p>Note: The factories list is stored by reference and read at workflow creation time, allowing
   * custom factories to be registered after construction but before the worker starts. This is
   * necessary because the Worker class allows factory registration after construction.
   *
   * @param factories the list of custom factories to consult in order; may be empty; stored by
   *     reference
   * @param defaultFactory the default factory to use if no custom factory handles the workflow type
   */
  CompositeReplayWorkflowFactory(
      @Nonnull List<WorkflowImplementationFactory> factories,
      @Nonnull ReplayWorkflowFactory defaultFactory) {
    // Store reference to allow late registration of factories
    this.factories = factories;
    this.defaultFactory = defaultFactory;
  }

  /**
   * Creates a workflow instance for the given workflow type and execution.
   *
   * <p>Iterates through registered factories in registration order and returns the first non-null
   * result. If no custom factory can handle the workflow type, delegates to the default factory.
   *
   * @param workflowType the type of workflow to create
   * @param workflowExecution the execution context
   * @return a {@link ReplayWorkflow} instance to handle this workflow
   * @throws Exception if an error occurs during workflow creation
   */
  @Nullable
  @Override
  public ReplayWorkflow getWorkflow(
      @Nonnull WorkflowType workflowType, @Nonnull WorkflowExecution workflowExecution)
      throws Exception {
    // Consult custom factories in registration order
    for (WorkflowImplementationFactory factory : factories) {
      ReplayWorkflow workflow = factory.getWorkflow(workflowType, workflowExecution);
      if (workflow != null) {
        return workflow;
      }
    }
    // Fall back to the default factory
    return defaultFactory.getWorkflow(workflowType, workflowExecution);
  }

  /**
   * Returns whether any workflow types are supported by this composite factory.
   *
   * @return {@code true} if at least one registered factory or the default factory supports any
   *     workflow type
   */
  @Override
  public boolean isAnyTypeSupported() {
    // Check if any custom factory has registered types
    for (WorkflowImplementationFactory factory : factories) {
      if (factory.isAnyTypeSupported()) {
        return true;
      }
    }
    // Check the default factory
    return defaultFactory.isAnyTypeSupported();
  }

  /**
   * Returns the set of all registered workflow types across all factories.
   *
   * <p>This includes types from all custom factories and the default factory (if it implements
   * {@link WorkflowImplementationFactory}).
   *
   * @return an unmodifiable set of all registered workflow type names
   */
  @Nonnull
  Set<String> getRegisteredWorkflowTypes() {
    Set<String> allTypes = new HashSet<>();

    // Collect types from custom factories
    for (WorkflowImplementationFactory factory : factories) {
      allTypes.addAll(factory.getRegisteredWorkflowTypes());
    }

    // Collect types from default factory if it supports the interface
    if (defaultFactory instanceof WorkflowImplementationFactory) {
      allTypes.addAll(
          ((WorkflowImplementationFactory) defaultFactory).getRegisteredWorkflowTypes());
    }

    return Collections.unmodifiableSet(allTypes);
  }

  @Override
  public String toString() {
    return "CompositeReplayWorkflowFactory{"
        + "customFactories="
        + factories.size()
        + ", defaultFactory="
        + defaultFactory
        + '}';
  }
}
