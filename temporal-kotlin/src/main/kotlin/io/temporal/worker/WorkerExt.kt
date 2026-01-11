@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.worker

import io.temporal.kotlin.TemporalDsl

/**
 * Registers workflow implementation classes with a worker.
 *
 * @see Worker.registerWorkflowImplementationTypes
 */
inline fun Worker.registerWorkflowImplementationTypes(
  vararg workflowImplementationClasses: Class<*>,
  options: @TemporalDsl WorkflowImplementationOptions.Builder.() -> Unit
) {
  registerWorkflowImplementationTypes(
    WorkflowImplementationOptions(options),
    *workflowImplementationClasses
  )
}

/**
 * Registers a single workflow implementation class with a worker.
 *
 * @param T workflow implementation type to register
 * @see Worker.registerWorkflowImplementationTypes
 */
inline fun <reified T : Any> Worker.registerWorkflowImplementationType() {
  registerWorkflowImplementationTypes(T::class.java)
}

/**
 * Registers a single workflow implementation class with a worker.
 *
 * @param T workflow implementation type to register
 * @see Worker.registerWorkflowImplementationTypes
 */
inline fun <reified T : Any> Worker.registerWorkflowImplementationType(
  options: @TemporalDsl WorkflowImplementationOptions.Builder.() -> Unit
) {
  registerWorkflowImplementationTypes(T::class.java, options = options)
}

/**
 * Configures a factory to use when an instance of a workflow implementation is created.
 * Please read an original [Worker.registerWorkflowImplementationFactory] method doc because
 * this method has a limited usage.
 *
 * @param T Workflow interface that this factory implements
 * @param factory factory that when called creates a new instance of the workflow implementation
 * object.
 * @param options custom workflow implementation options for a worker
 * @see Worker.registerWorkflowImplementationFactory
 */
inline fun <reified T : Any> Worker.registerWorkflowImplementationFactory(
  options: WorkflowImplementationOptions,
  noinline factory: () -> T
) {
  registerWorkflowImplementationFactory(T::class.java, factory, options)
}

/**
 * Configures a factory to use when an instance of a workflow implementation is created. <br>
 * Please read an original [Worker.registerWorkflowImplementationFactory] method doc because this method has a limited usage
 *
 * @param T Workflow interface that this factory implements
 * @param factory factory that when called creates a new instance of the workflow implementation
 * object.
 * @see Worker.registerWorkflowImplementationFactory
 */
inline fun <reified T : Any> Worker.registerWorkflowImplementationFactory(
  noinline factory: () -> T
) {
  registerWorkflowImplementationFactory(T::class.java, factory)
}
