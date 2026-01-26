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

package io.temporal.kotlin.internal.activity

import io.temporal.common.metadata.POJOActivityInterfaceMetadata
import io.temporal.kotlin.activity.KDynamicActivity
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

/**
 * Registry for Kotlin activity implementations used for testing and mocking.
 *
 * This class maintains a mapping of activity type names to their implementations,
 * supporting both regular and suspend activity methods. It's designed for use with
 * [KDynamicActivityHandler] to enable runtime registration of activity mocks in tests.
 *
 * Note: For production use, prefer [KWorker.registerActivitiesImplementations] which uses TypedDynamicActivity
 * for better performance and type safety.
 */
public class KActivityRegistry {

  private data class ActivityEntry(
    val implementation: Any,
    val method: Method,
    val kFunction: KFunction<*>?,
    val isSuspend: Boolean
  )

  private val activities = ConcurrentHashMap<String, ActivityEntry>()

  /**
   * A fallback dynamic activity handler that will be used when an activity type
   * is not found in the registry. This allows combining registry-based activities
   * with a catch-all KDynamicActivity.
   */
  @Volatile
  public var dynamicActivityFallback: KDynamicActivity? = null

  /**
   * Register an activity implementation (for real implementations).
   *
   * Scans all @ActivityInterface interfaces implemented by the object and registers
   * each activity method.
   *
   * @param implementation Activity implementation instance
   */
  public fun register(implementation: Any) {
    val interfaces = KActivityMetadata.findActivityInterfaces(implementation::class.java)
    if (interfaces.isEmpty()) {
      throw IllegalArgumentException(
        "Implementation does not implement any @ActivityInterface: ${implementation::class.java.name}"
      )
    }

    for (iface in interfaces) {
      registerInterface(implementation, iface)
    }
  }

  /**
   * Register a mock implementation for testing.
   *
   * Similar to [register] but designed for mock objects which may have proxy classes.
   *
   * @param mockImplementation Mock activity implementation
   */
  public fun registerMockImplementation(mockImplementation: Any) {
    // For mocks, we need to find the activity interface from the mock's interfaces
    val interfaces = KActivityMetadata.findActivityInterfaces(mockImplementation::class.java)
    if (interfaces.isEmpty()) {
      throw IllegalArgumentException(
        "Mock does not implement any @ActivityInterface: ${mockImplementation::class.java.name}"
      )
    }

    for (iface in interfaces) {
      registerInterface(mockImplementation, iface)
    }
  }

  private fun registerInterface(implementation: Any, activityInterface: Class<*>) {
    val implClass = implementation::class.java
    val metadata = POJOActivityInterfaceMetadata.newInstance(activityInterface)

    for (methodMetadata in metadata.methodsMetadata) {
      val activityTypeName = methodMetadata.activityTypeName
      val interfaceMethod = methodMetadata.method
      val implMethod = KActivityMetadata.findImplementationMethod(implClass, interfaceMethod)
      val kFunction = implMethod.kotlinFunction
      val isSuspend = KActivityMetadata.isSuspendMethod(implMethod)

      activities[activityTypeName] = ActivityEntry(
        implementation = implementation,
        method = implMethod,
        kFunction = kFunction,
        isSuspend = isSuspend
      )
    }
  }

  /**
   * Check if an activity type is registered.
   */
  public fun hasActivity(activityType: String): Boolean = activities.containsKey(activityType)

  /**
   * Get all registered activity type names.
   */
  public fun getRegisteredTypes(): Set<String> = activities.keys.toSet()

  /**
   * Execute an activity by type name.
   *
   * @param activityType The activity type name
   * @param args Arguments to pass to the activity
   * @return The activity result
   * @throws IllegalArgumentException if activity type is not registered
   */
  public fun execute(activityType: String, args: Array<Any?>): Any? {
    val entry = activities[activityType]
      ?: throw IllegalArgumentException(
        "Unknown activity type: $activityType. Known types: ${activities.keys}"
      )

    return if (entry.isSuspend) {
      executeSuspend(entry, args)
    } else {
      executeRegular(entry, args)
    }
  }

  private fun executeRegular(entry: ActivityEntry, args: Array<Any?>): Any? {
    return entry.kFunction?.call(entry.implementation, *args)
      ?: entry.method.invoke(entry.implementation, *args)
  }

  private fun executeSuspend(entry: ActivityEntry, args: Array<Any?>): Any? {
    // For suspend functions, we need to use runBlocking since DynamicActivity.execute
    // is a blocking call
    return kotlinx.coroutines.runBlocking {
      entry.kFunction!!.callSuspend(entry.implementation, *args)
    }
  }
}
