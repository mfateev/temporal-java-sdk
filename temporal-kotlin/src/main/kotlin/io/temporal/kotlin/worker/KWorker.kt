@file:OptIn(io.temporal.kotlin.internal.InternalTemporalApi::class)

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

import io.temporal.activity.ActivityInterface
import io.temporal.common.metadata.POJOActivityInterfaceMetadata
import io.temporal.kotlin.activity.KDynamicActivity
import io.temporal.kotlin.client.KClient
import io.temporal.kotlin.interceptor.KWorkerInterceptor
import io.temporal.kotlin.internal.InternalTemporalApi
import io.temporal.kotlin.internal.activity.KDynamicActivityWrapper
import io.temporal.kotlin.internal.activity.KotlinActivityWrapper
import io.temporal.kotlin.internal.converters.KOptionsConverters
import io.temporal.kotlin.internal.plugin.KotlinPlugin
import io.temporal.kotlin.internal.plugin.KotlinPluginOptions
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkflowImplementationOptions
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.reflect.KClass

/**
 * Kotlin worker that provides idiomatic APIs for registering
 * Kotlin workflows and activities (including suspend activities).
 *
 * Use [worker] property for direct access to the underlying Java Worker
 * when interoperating with Java workflows/activities.
 *
 * ## Activity Registration
 *
 * This worker uses [TypedDynamicActivity][io.temporal.activity.TypedDynamicActivity] wrappers
 * to register Kotlin activities directly with the Java SDK. Each activity method is wrapped
 * in a [KotlinActivityWrapper] that handles both suspend and non-suspend methods.
 *
 * This design ensures:
 * - Proper handling of Kotlin suspend functions
 * - No conflicts with Java DynamicActivity registrations
 * - Compatibility with test mocking frameworks
 * - Full support for activity interceptors
 *
 * Example using KWorkerFactory:
 * ```kotlin
 * val factory = KWorkerFactory(client)
 * val kWorker = factory.newWorker("task-queue")
 *
 * // Register workflow using reified generics
 * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
 *
 * // Register activities (works with both suspend and non-suspend)
 * kWorker.registerActivitiesImplementations(MyActivitiesImpl())
 *
 * factory.start()
 * ```
 *
 * Example using simplified pattern:
 * ```kotlin
 * val worker = KWorker(
 *     client,
 *     KWorkerOptions(
 *         taskQueue = "my-task-queue",
 *         workflows = listOf(GreetingWorkflowImpl::class),
 *         activities = listOf(GreetingActivitiesImpl())
 *     )
 * )
 * worker.run()  // Blocks until shutdown
 * ```
 */
public class KWorker private constructor(
  /**
   * The underlying Java Worker for interop scenarios.
   *
   * This property is marked as internal API to hide Java SDK types from the public API.
   * Use the Kotlin-specific methods on this class instead.
   */
  @property:InternalTemporalApi
  public val worker: Worker,
  /** Kotlin worker interceptors for activity interception */
  internal val workerInterceptors: List<KWorkerInterceptor>,
  /** Coroutine dispatcher for suspend activities */
  private val activityDispatcher: CoroutineDispatcher,
  /** The internal WorkerFactory (null when created via KWorkerFactory) */
  private val internalWorkerFactory: WorkerFactory?,
  /** KotlinPlugin for dynamic workflow registration */
  private val kotlinPlugin: KotlinPlugin?
) {

  /**
   * Creates a KWorker wrapping an existing Java Worker.
   *
   * This constructor is primarily used for:
   * - Interoperability with existing Java Worker instances
   * - KWorkerFactory creating workers
   * - Testing environments wrapping test workers
   *
   * For simplified worker setup, use [KWorker.invoke] instead.
   *
   * This constructor is marked as internal API to hide Java SDK types from the public API.
   *
   * @param worker The underlying Java Worker to wrap
   * @param workerInterceptors Optional Kotlin worker interceptors
   * @param activityDispatcher Optional coroutine dispatcher for suspend activities
   */
  @InternalTemporalApi
  @JvmOverloads
  public constructor(
    worker: Worker,
    workerInterceptors: List<KWorkerInterceptor> = emptyList(),
    activityDispatcher: CoroutineDispatcher = Dispatchers.Default
  ) : this(worker, workerInterceptors, activityDispatcher, null, null)

  /**
   * Creates a KWorker wrapping an existing Java Worker with KotlinPlugin support.
   *
   * This constructor is used by test environments to create workers that can
   * register dynamic workflows via the KotlinPlugin.
   *
   * This constructor is marked as internal API to hide Java SDK types from the public API.
   *
   * @param worker The underlying Java Worker to wrap
   * @param kotlinPlugin The KotlinPlugin for dynamic workflow registration
   * @param workerInterceptors Optional Kotlin worker interceptors
   * @param activityDispatcher Optional coroutine dispatcher for suspend activities
   */
  @InternalTemporalApi
  public constructor(
    worker: Worker,
    kotlinPlugin: KotlinPlugin,
    workerInterceptors: List<KWorkerInterceptor> = emptyList(),
    activityDispatcher: CoroutineDispatcher = Dispatchers.Default
  ) : this(worker, workerInterceptors, activityDispatcher, null, kotlinPlugin)

  companion object {
    /**
     * Creates a new Kotlin worker with simplified configuration.
     *
     * This factory method follows the Python/.NET SDK pattern for simplified worker setup,
     * allowing workflows and activities to be specified at construction time.
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
     *
     * // Block until shutdown or fatal error
     * worker.run()
     * ```
     *
     * @param client The KClient to use for workflow interactions
     * @param options Configuration options including task queue, workflows, activities, and worker settings
     * @return A new KWorker instance
     */
    public operator fun invoke(
      client: KClient,
      options: KWorkerOptions
    ): KWorker {
      // Create WorkerFactory with KotlinPlugin
      val kotlinPlugin = KotlinPlugin.create(KotlinPluginOptions())
      val factoryOptions = WorkerFactoryOptions.newBuilder()
        .addPlugin(kotlinPlugin)
        .build()
      val workerFactory = WorkerFactory.newInstance(client.workflowClient, factoryOptions)

      // Create worker
      val worker = workerFactory.newWorker(options.taskQueue, options.toWorkerOptions())

      // Create KWorker instance
      val kWorker = KWorker(
        worker = worker,
        workerInterceptors = emptyList(),
        activityDispatcher = Dispatchers.Default,
        internalWorkerFactory = workerFactory,
        kotlinPlugin = kotlinPlugin
      )

      // Register workflows (including dynamic workflow if specified)
      val allWorkflows = options.workflows + listOfNotNull(options.dynamicWorkflow)
      if (allWorkflows.isNotEmpty()) {
        if (options.workflowImplementationOptions != null) {
          val javaOptions = KOptionsConverters.toJava(options.workflowImplementationOptions)
          kWorker.registerWorkflowImplementationTypes(javaOptions, *allWorkflows.toTypedArray())
        } else {
          kWorker.registerWorkflowImplementationTypes(*allWorkflows.toTypedArray())
        }
      }

      // Register activities
      if (options.activities.isNotEmpty()) {
        kWorker.registerActivitiesImplementations(*options.activities.toTypedArray())
      }

      // Register dynamic activity
      options.dynamicActivity?.let { dynamicActivity ->
        worker.registerActivitiesImplementations(
          KDynamicActivityWrapper(dynamicActivity)
        )
      }

      return kWorker
    }
  }

  // ========== Lifecycle Methods ==========

  /**
   * Starts the worker and suspends until shutdown, cancellation, or a fatal error occurs.
   *
   * This method starts the underlying worker factory and then awaits termination.
   * It properly respects coroutine cancellation, making it suitable for use with
   * shutdown hooks or structured concurrency.
   *
   * Example:
   * ```kotlin
   * fun main() = runBlocking {
   *   val worker = KWorker(client, options)
   *   val job = launch { worker.run() }
   *
   *   // Hook Ctrl+C to cancel the worker
   *   Runtime.getRuntime().addShutdownHook(Thread {
   *     runBlocking { job.cancelAndJoin() }
   *   })
   *
   *   job.join()
   * }
   * ```
   *
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public suspend fun run() {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "run() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.start() instead."
      )

    factory.start()

    // Block until cancelled or factory terminates
    suspendCancellableCoroutine<Unit> { cont ->
      // Start a thread that waits for factory termination
      val waiterThread = thread(name = "kworker-termination-waiter") {
        factory.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        if (cont.isActive) {
          cont.resumeWith(Result.success(Unit))
        }
      }

      cont.invokeOnCancellation {
        // On cancellation, shutdown the factory which will cause awaitTermination to return
        factory.shutdown()
        // Wait briefly for the waiter thread to complete
        waiterThread.join(5000)
      }
    }

    // Ensure graceful shutdown completes
    withContext(NonCancellable + Dispatchers.IO) {
      factory.shutdown()
      factory.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  /**
   * Starts the worker without blocking.
   *
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun start() {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "start() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.start() instead."
      )
    factory.start()
  }

  /**
   * Initiates an orderly shutdown.
   *
   * The worker will stop accepting new tasks but will finish processing
   * any tasks that have already started.
   *
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun shutdown() {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "shutdown() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.shutdown() instead."
      )
    factory.shutdown()
  }

  /**
   * Initiates an immediate shutdown.
   *
   * The worker will attempt to stop all processing immediately.
   *
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun shutdownNow() {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "shutdownNow() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.shutdownNow() instead."
      )
    factory.shutdownNow()
  }

  /**
   * Waits for the worker to terminate.
   *
   * @param timeout Maximum time to wait for termination
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public suspend fun awaitTermination(timeout: Duration) {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "awaitTermination() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.awaitTermination() instead."
      )

    withContext(Dispatchers.IO) {
      factory.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Checks if the worker has been started.
   *
   * @return true if the worker has been started
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun isStarted(): Boolean {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "isStarted() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.isStarted() instead."
      )
    return factory.isStarted
  }

  /**
   * Checks if shutdown has been initiated.
   *
   * @return true if shutdown has been initiated
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun isShutdown(): Boolean {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "isShutdown() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.isShutdown() instead."
      )
    return factory.isShutdown
  }

  /**
   * Checks if the worker has terminated.
   *
   * @return true if the worker has terminated
   * @throws IllegalStateException if called on a worker created via KWorkerFactory
   */
  public fun isTerminated(): Boolean {
    val factory = internalWorkerFactory
      ?: throw IllegalStateException(
        "isTerminated() can only be called on workers created with KWorker(client, options) constructor. " +
          "For workers created via KWorkerFactory, use factory.isTerminated() instead."
      )
    return factory.isTerminated
  }

  // ========== Workflow Registration ==========

  /**
   * Register Kotlin workflow implementation types using reified generics.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl>()
   * ```
   */
  public inline fun <reified T : Any> registerWorkflowImplementationTypes() {
    worker.registerWorkflowImplementationTypes(T::class.java)
  }

  /**
   * Register Kotlin workflow implementation types using KClass.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes(
   *     MyWorkflowImpl::class,
   *     AnotherWorkflowImpl::class
   * )
   * ```
   *
   * @param workflowClasses Workflow implementation classes to register
   */
  public fun registerWorkflowImplementationTypes(vararg workflowClasses: KClass<*>) {
    worker.registerWorkflowImplementationTypes(
      *workflowClasses.map { it.java }.toTypedArray()
    )
  }

  /**
   * Register Kotlin workflow implementation types with options using KClass.
   *
   * This method is marked as internal API to hide Java SDK types from the public API.
   *
   * Example:
   * ```kotlin
   * val options = WorkflowImplementationOptions.newBuilder()
   *     .setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
   *     .build()
   * kWorker.registerWorkflowImplementationTypes(options, MyWorkflowImpl::class)
   * ```
   *
   * @param options WorkflowImplementationOptions instance
   * @param workflowClasses Workflow implementation classes to register
   */
  @InternalTemporalApi
  public fun registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions,
    vararg workflowClasses: KClass<*>
  ) {
    worker.registerWorkflowImplementationTypes(
      options,
      *workflowClasses.map { it.java }.toTypedArray()
    )
  }

  /**
   * Register Kotlin workflow implementation types with options DSL.
   *
   * This method is marked as internal API to hide Java SDK types from the public API.
   *
   * Example:
   * ```kotlin
   * kWorker.registerWorkflowImplementationTypes<MyWorkflowImpl> {
   *     setFailWorkflowExceptionTypes(IllegalArgumentException::class.java)
   * }
   * ```
   *
   * @param options DSL builder for WorkflowImplementationOptions
   */
  @InternalTemporalApi
  public inline fun <reified T : Any> registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions.Builder.() -> Unit
  ) {
    val opts = WorkflowImplementationOptions.newBuilder().apply(options).build()
    worker.registerWorkflowImplementationTypes(opts, T::class.java)
  }

  // ========== Activity Registration ==========

  /**
   * Register activity implementations.
   *
   * This method handles both suspend and non-suspend activity methods by wrapping
   * each method in a [KotlinActivityWrapper] and registering it as a
   * [TypedDynamicActivity][io.temporal.activity.TypedDynamicActivity] with the Java SDK.
   *
   * Example:
   * ```kotlin
   * @ActivityInterface
   * interface MyActivities {
   *     fun syncOperation(): String           // Regular method
   *     suspend fun asyncOperation(): Data    // Suspend method
   * }
   *
   * kWorker.registerActivitiesImplementations(MyActivitiesImpl())
   * ```
   *
   * @param activities Activity implementation instances to register
   */
  public fun registerActivitiesImplementations(vararg activities: Any) {
    for (activity in activities) {
      registerActivityImplementation(activity)
    }
  }

  /**
   * Register a single activity implementation.
   *
   * Handles both regular activities (with @ActivityInterface) and dynamic activities
   * (implementing KDynamicActivity). Dynamic activities are wrapped with KDynamicActivityWrapper.
   */
  private fun registerActivityImplementation(activity: Any) {
    // Check if this is a dynamic activity
    if (activity is KDynamicActivity) {
      worker.registerActivitiesImplementations(KDynamicActivityWrapper(activity))
      return
    }

    val implClass = activity::class.java

    // Find all activity interfaces implemented by this class
    val activityInterfaces = findActivityInterfaces(implClass)
    if (activityInterfaces.isEmpty()) {
      throw IllegalArgumentException(
        "Implementation does not implement any @ActivityInterface annotated interfaces: ${implClass.name}"
      )
    }

    // Create wrappers for all activity methods and register them
    val wrappers = mutableListOf<KotlinActivityWrapper>()

    for (activityInterface in activityInterfaces) {
      val metadata = POJOActivityInterfaceMetadata.newInstance(activityInterface)
      for (methodMetadata in metadata.methodsMetadata) {
        val activityTypeName = methodMetadata.activityTypeName
        val interfaceMethod = methodMetadata.method

        // Find the implementation method (may be different for suspend functions)
        val implMethod = findImplementationMethod(implClass, interfaceMethod)

        val wrapper = KotlinActivityWrapper(
          activityTypeName = activityTypeName,
          implementation = activity,
          method = implMethod,
          dispatcher = activityDispatcher
        )
        wrappers.add(wrapper)
      }
    }

    // Register all wrappers with the Java worker
    worker.registerActivitiesImplementations(*wrappers.toTypedArray())
  }

  /**
   * Find the implementation method for an interface method.
   *
   * For suspend functions, the implementation method will have a Continuation parameter.
   */
  private fun findImplementationMethod(
    implClass: Class<*>,
    interfaceMethod: java.lang.reflect.Method
  ): java.lang.reflect.Method {
    val methodName = interfaceMethod.name
    val interfaceParams = interfaceMethod.parameterTypes

    // First try exact match (for non-suspend methods)
    try {
      return implClass.getMethod(methodName, *interfaceParams)
    } catch (_: NoSuchMethodException) {
      // Not found, continue to search for suspend variant
    }

    // For suspend methods, the implementation has an extra Continuation parameter
    // Look for a method with the same name and compatible parameter count
    val continuationClass = kotlin.coroutines.Continuation::class.java
    for (method in implClass.methods) {
      if (method.name == methodName) {
        val params = method.parameterTypes
        // Suspend method: same params + Continuation at the end
        if (params.size == interfaceParams.size + 1 &&
          continuationClass.isAssignableFrom(params.last())
        ) {
          // Verify the other params match
          var matches = true
          for (i in interfaceParams.indices) {
            if (interfaceParams[i] != params[i]) {
              matches = false
              break
            }
          }
          if (matches) {
            return method
          }
        }
      }
    }

    throw IllegalStateException(
      "Could not find implementation method for ${interfaceMethod.name} in ${implClass.name}"
    )
  }

  /**
   * Find all activity interfaces implemented by a class.
   */
  private fun findActivityInterfaces(clazz: Class<*>): List<Class<*>> {
    val result = mutableListOf<Class<*>>()

    fun collectInterfaces(cls: Class<*>) {
      for (iface in cls.interfaces) {
        if (iface.isAnnotationPresent(ActivityInterface::class.java)) {
          result.add(iface)
        }
        collectInterfaces(iface)
      }
      cls.superclass?.let { collectInterfaces(it) }
    }

    collectInterfaces(clazz)
    return result.distinct()
  }

  // ========== Nexus Registration ==========

  /**
   * Register Nexus service implementations.
   *
   * Example:
   * ```kotlin
   * @NexusServiceInterface
   * interface MyNexusService {
   *     @NexusOperationInterface
   *     fun doSomething(input: String): String
   * }
   *
   * class MyNexusServiceImpl : MyNexusService {
   *     override fun doSomething(input: String): String = "result"
   * }
   *
   * kWorker.registerNexusServiceImplementation(MyNexusServiceImpl())
   * ```
   *
   * @param services Nexus service implementation instances to register
   */
  public fun registerNexusServiceImplementation(vararg services: Any) {
    worker.registerNexusServiceImplementation(*services)
  }
}
