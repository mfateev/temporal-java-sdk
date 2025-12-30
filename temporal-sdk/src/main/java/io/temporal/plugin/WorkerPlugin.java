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

package io.temporal.plugin;

import io.temporal.common.converter.DataConverter;
import io.temporal.internal.worker.WorkflowImplementationFactory;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerOptions;
import javax.annotation.Nullable;

/**
 * Plugin interface for extending Worker functionality.
 *
 * <p>Plugins enable language-specific or framework-specific extensions to Temporal workers. They
 * can:
 *
 * <ul>
 *   <li>Modify WorkerOptions before worker creation
 *   <li>Intercept workflow type registration and provide custom factories
 *   <li>Register activity implementations after worker creation
 * </ul>
 *
 * <p>Plugins are registered at the WorkerFactory level and are applied to all workers created by
 * that factory.
 *
 * <p>Example implementation for a language-specific plugin:
 *
 * <pre>{@code
 * public class MyLanguagePlugin implements WorkerPlugin {
 *     private MyWorkflowFactory factory;
 *
 *     @Override
 *     public WorkerOptions.Builder configureWorker(WorkerOptions.Builder builder) {
 *         return builder.setDefaultDeadlockDetectionTimeout(1500);
 *     }
 *
 *     @Override
 *     public WorkflowImplementationFactory getFactoryForType(
 *             Class<?> clazz, DataConverter dataConverter) {
 *         if (!isMyLanguageWorkflow(clazz)) {
 *             return null; // Let default factory handle it
 *         }
 *         if (factory == null) {
 *             factory = new MyWorkflowFactory(dataConverter);
 *         }
 *         factory.registerWorkflowImplementationType(clazz);
 *         return factory;
 *     }
 * }
 * }</pre>
 *
 * <p>Usage with WorkerFactory:
 *
 * <pre>{@code
 * WorkerFactory factory = WorkerFactory.newInstance(
 *     client,
 *     WorkerFactoryOptions.newBuilder()
 *         .addPlugin(new MyLanguagePlugin())
 *         .build()
 * );
 * Worker worker = factory.newWorker("task-queue");
 * // Plugin automatically handles workflow types during registration
 * worker.registerWorkflowImplementationTypes(
 *     MyLanguageWorkflow.class,  // Handled by plugin
 *     StandardJavaWorkflow.class  // Handled by default factory
 * );
 * }</pre>
 */
public interface WorkerPlugin {

  /**
   * Called before Worker is created to allow modification of worker options.
   *
   * <p>Plugins can use this hook to set worker configuration options such as concurrency limits,
   * timeouts, or other worker-level settings.
   *
   * <p>Multiple plugins are called in registration order. Each plugin receives the builder returned
   * by the previous plugin.
   *
   * @param builder the WorkerOptions builder to modify
   * @return the modified builder (may be same instance or new)
   */
  default WorkerOptions.Builder configureWorker(WorkerOptions.Builder builder) {
    return builder;
  }

  /**
   * Called for each workflow implementation type being registered via {@link
   * Worker#registerWorkflowImplementationTypes}.
   *
   * <p>If this plugin handles the workflow type (e.g., it's a Kotlin suspend workflow), it should:
   *
   * <ol>
   *   <li>Register the type with its internal factory
   *   <li>Return the factory instance
   * </ol>
   *
   * <p>If this plugin does not handle the type, return {@code null} to let the next plugin or the
   * default POJO factory handle it.
   *
   * <p>Multiple plugins are consulted in registration order. The first plugin to return a non-null
   * factory wins.
   *
   * @param workflowImplementationType the workflow implementation class being registered
   * @param dataConverter the DataConverter from WorkflowClient options
   * @return a factory that handles this workflow type, or {@code null} to delegate
   */
  @Nullable
  default WorkflowImplementationFactory getFactoryForType(
      Class<?> workflowImplementationType, DataConverter dataConverter) {
    return null;
  }

  /**
   * Called after Worker is created to allow registration of activity implementations or other
   * setup.
   *
   * <p>Note: For workflow registration, prefer using {@link #getFactoryForType} which integrates
   * with the standard {@link Worker#registerWorkflowImplementationTypes} API.
   *
   * <p>Multiple plugins are called in registration order.
   *
   * @param worker the created worker
   * @param dataConverter the DataConverter from WorkflowClient options
   */
  default void onWorkerCreated(Worker worker, DataConverter dataConverter) {
    // Default no-op
  }
}
