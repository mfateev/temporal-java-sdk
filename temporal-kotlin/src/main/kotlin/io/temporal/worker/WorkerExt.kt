@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.worker

import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.TemporalDsl
import io.temporal.kotlin.internal.KotlinWorkflowDefinition
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.worker.KotlinPlugin
import kotlin.reflect.KClass

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
 *
 * @param T Workflow interface that this factory implements
 * @param factory factory that when called creates a new instance of the workflow implementation
 * object.
 * @see Worker.addWorkflowImplementationFactory
 * @deprecated See deprecation notes on [Worker.addWorkflowImplementationFactory]
 */
@Deprecated(
  "Use registerWorkflowImplementationFactory instead",
  ReplaceWith("this.registerWorkflowImplementationFactory(options, factory)")
)
@Suppress("Deprecation")
inline fun <reified T : Any> Worker.addWorkflowImplementationFactory(
  options: WorkflowImplementationOptions,
  noinline factory: () -> T
) {
  addWorkflowImplementationFactory(options, T::class.java, factory)
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
 * This method may behave differently from your expectations!
 * Read deprecation and migration notes on [Worker.addWorkflowImplementationFactory].
 * Configures a factory to use when an instance of a workflow implementation is created.
 *
 * ```kotlin
 * worker.addWorkflowImplementationFactory<ChildWorkflow> {
 *   val child = mock<ChildWorkflow>()
 *   when(child.workflow(anyString(), anyString())).thenReturn("result1")
 *   child
 * }
 * ```
 *
 * @param T Workflow interface that this factory implements
 * @param factory factory that when called creates a new instance of the workflow implementation
 * object.
 * @see Worker.addWorkflowImplementationFactory
 */
@Deprecated("Use registerWorkflowImplementationFactory instead", ReplaceWith("this.registerWorkflowImplementationFactory(factory)"))
@Suppress("Deprecation")
inline fun <reified T : Any> Worker.addWorkflowImplementationFactory(
  noinline factory: () -> T
) {
  addWorkflowImplementationFactory(T::class.java, factory)
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

// ==================== Kotlin Coroutine Workflow Registration ====================

/**
 * Registers Kotlin workflow implementation classes with automatic detection of suspend functions.
 *
 * This extension automatically routes workflow implementations to the appropriate factory:
 * - Suspend function workflows are registered with [KotlinWorkflowImplementationFactory]
 * - Non-suspend workflows are registered with the standard Java SDK factory
 *
 * Example:
 * ```kotlin
 * val plugin = KotlinPlugin()
 * worker.registerKotlinWorkflowImplementationTypes(
 *   plugin,
 *   MyWorkflowImpl::class,
 *   AnotherWorkflowImpl::class
 * )
 * ```
 *
 * @param plugin the Kotlin plugin providing configuration for coroutine workflows
 * @param workflowImplementationClasses the workflow implementation classes to register
 * @throws IllegalArgumentException if a class doesn't implement a valid workflow interface
 * @see KotlinPlugin
 */
fun Worker.registerKotlinWorkflowImplementationTypes(
  plugin: KotlinPlugin,
  vararg workflowImplementationClasses: KClass<*>
) {
  val suspendWorkflows = mutableListOf<Class<*>>()
  val javaWorkflows = mutableListOf<Class<*>>()

  // Partition workflows by whether they use suspend functions
  for (kClass in workflowImplementationClasses) {
    val javaClass = kClass.java
    if (KotlinWorkflowDefinition.isSuspendWorkflow(javaClass)) {
      suspendWorkflows.add(javaClass)
    } else {
      javaWorkflows.add(javaClass)
    }
  }

  // Register suspend workflows with Kotlin factory
  if (suspendWorkflows.isNotEmpty()) {
    val factory = plugin.createFactory(DataConverter.getDefaultInstance())
    suspendWorkflows.forEach { factory.registerWorkflowImplementationType(it) }
    registerWorkflowImplementationFactory(factory)
  }

  // Register non-suspend workflows with standard Java factory
  if (javaWorkflows.isNotEmpty()) {
    registerWorkflowImplementationTypes(*javaWorkflows.toTypedArray())
  }
}

/**
 * Registers Kotlin workflow implementation classes with a default [KotlinPlugin].
 *
 * This is a convenience overload that uses default plugin options.
 *
 * Example:
 * ```kotlin
 * worker.registerKotlinWorkflowImplementationTypes(
 *   MyWorkflowImpl::class,
 *   AnotherWorkflowImpl::class
 * )
 * ```
 *
 * @param workflowImplementationClasses the workflow implementation classes to register
 * @throws IllegalArgumentException if a class doesn't implement a valid workflow interface
 */
fun Worker.registerKotlinWorkflowImplementationTypes(
  vararg workflowImplementationClasses: KClass<*>
) {
  registerKotlinWorkflowImplementationTypes(KotlinPlugin(), *workflowImplementationClasses)
}

/**
 * Registers a single Kotlin workflow implementation type.
 *
 * Example:
 * ```kotlin
 * worker.registerKotlinWorkflowImplementationType<MyWorkflowImpl>()
 * ```
 *
 * @param T the workflow implementation type to register
 */
inline fun <reified T : Any> Worker.registerKotlinWorkflowImplementationType() {
  registerKotlinWorkflowImplementationTypes(T::class)
}

/**
 * Registers a single Kotlin workflow implementation type with a custom plugin.
 *
 * Example:
 * ```kotlin
 * val plugin = KotlinPlugin { deadlockDetectionTimeout = 2000L }
 * worker.registerKotlinWorkflowImplementationType<MyWorkflowImpl>(plugin)
 * ```
 *
 * @param T the workflow implementation type to register
 * @param plugin the Kotlin plugin providing configuration
 */
inline fun <reified T : Any> Worker.registerKotlinWorkflowImplementationType(plugin: KotlinPlugin) {
  registerKotlinWorkflowImplementationTypes(plugin, T::class)
}
