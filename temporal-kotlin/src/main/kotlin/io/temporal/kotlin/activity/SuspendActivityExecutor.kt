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

package io.temporal.kotlin.activity

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.worker.Worker
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.javaMethod

/**
 * Configuration options for suspend activity registration.
 *
 * @property dispatcher The coroutine dispatcher for executing suspend activities.
 *   Defaults to [Dispatchers.Default].
 */
public data class SuspendActivityOptions(
  val dispatcher: CoroutineDispatcher = Dispatchers.Default
)

/**
 * Registers activity implementations that may contain suspend functions.
 *
 * This extension function detects suspend functions in activity interfaces and
 * creates appropriate executors:
 * - Suspend functions: Use [SuspendActivityInvoker] with coroutine-based execution
 * - Regular functions: Delegated to standard Java SDK POJO activity handling
 *
 * Example:
 * ```kotlin
 * @ActivityInterface
 * interface MyActivities {
 *     suspend fun fetchData(url: String): Data  // Suspend - runs on coroutine
 *     fun computeHash(data: ByteArray): String  // Regular - runs on thread pool
 * }
 *
 * worker.registerSuspendActivities(MyActivitiesImpl())
 * ```
 *
 * @param activityImplementations The activity implementation instances to register
 * @param options Configuration options for suspend activities
 */
public fun Worker.registerSuspendActivities(
  vararg activityImplementations: Any,
  options: SuspendActivityOptions = SuspendActivityOptions()
) {
  for (impl in activityImplementations) {
    registerSuspendActivityImplementation(impl, options)
  }
}

/**
 * Registers a single activity implementation with suspend function support.
 */
private fun Worker.registerSuspendActivityImplementation(
  activityImplementation: Any,
  options: SuspendActivityOptions
) {
  val implClass = activityImplementation::class

  // Find all activity interfaces implemented by this class
  val activityInterfaces = findActivityInterfaces(implClass)

  if (activityInterfaces.isEmpty()) {
    // No @ActivityInterface found - delegate to standard registration
    registerActivitiesImplementations(activityImplementation)
    return
  }

  // Check if any methods are suspend functions
  val hasSuspendMethods = activityInterfaces.any { iface ->
    iface.memberFunctions.any { method ->
      isActivityMethod(method) && method.isSuspend
    }
  }

  if (!hasSuspendMethods) {
    // No suspend methods - delegate to standard registration
    registerActivitiesImplementations(activityImplementation)
    return
  }

  // Has suspend methods - need to register via our custom mechanism
  // For now, we create a wrapper that handles both suspend and non-suspend methods
  val wrapper = SuspendActivityWrapper(
    activityImplementation = activityImplementation,
    activityInterfaces = activityInterfaces,
    dispatcher = options.dispatcher
  )

  // Register the wrapper as a dynamic activity
  registerActivitiesImplementations(wrapper.createDynamicActivity())
}

/**
 * Finds all interfaces annotated with @ActivityInterface in the class hierarchy.
 */
private fun findActivityInterfaces(kClass: KClass<*>): List<KClass<*>> {
  val result = mutableListOf<KClass<*>>()

  fun collectInterfaces(cls: KClass<*>) {
    // Check direct interfaces
    for (supertype in cls.supertypes) {
      val classifier = supertype.classifier
      if (classifier is KClass<*>) {
        if (classifier.java.isInterface && classifier.findAnnotation<ActivityInterface>() != null) {
          result.add(classifier)
        }
        collectInterfaces(classifier)
      }
    }
  }

  collectInterfaces(kClass)
  return result.distinct()
}

/**
 * Checks if a function is an activity method (has @ActivityMethod or is in @ActivityInterface).
 */
private fun isActivityMethod(method: KFunction<*>): Boolean {
  // Check for explicit @ActivityMethod annotation
  if (method.findAnnotation<ActivityMethod>() != null) {
    return true
  }

  // In @ActivityInterface, all public methods are activity methods by default
  // (unless they have default implementations in Java interfaces)
  val javaMethod = method.javaMethod ?: return false
  return !javaMethod.isDefault
}

/**
 * Gets the activity type name for a method.
 */
internal fun getActivityTypeName(method: KFunction<*>, interfaceClass: KClass<*>): String {
  val annotation = method.findAnnotation<ActivityMethod>()
  if (annotation != null && annotation.name.isNotEmpty()) {
    return annotation.name
  }
  return method.name
}
