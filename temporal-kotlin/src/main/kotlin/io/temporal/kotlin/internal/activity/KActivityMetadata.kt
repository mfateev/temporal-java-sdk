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

import io.temporal.activity.ActivityInterface
import io.temporal.kotlin.internal.InternalTemporalApi
import java.lang.reflect.Method
import kotlin.coroutines.Continuation

/**
 * Utility for Kotlin-specific activity metadata operations.
 *
 * This object provides methods to handle the mapping between activity interface methods
 * and their Kotlin implementations, particularly for suspend functions which have an
 * additional Continuation parameter in the implementation.
 */
@InternalTemporalApi
public object KActivityMetadata {

  /**
   * Find all activity interfaces implemented by a class.
   *
   * Recursively searches through the class hierarchy and all implemented interfaces
   * to find interfaces annotated with @ActivityInterface.
   *
   * @param implClass The implementation class to search
   * @return List of interfaces annotated with @ActivityInterface
   */
  public fun findActivityInterfaces(implClass: Class<*>): List<Class<*>> {
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

    collectInterfaces(implClass)
    return result.distinct()
  }

  /**
   * Find the implementation method for an interface method.
   *
   * For regular methods, this returns the method with matching signature.
   * For Kotlin suspend functions, the implementation method has an additional
   * Continuation parameter at the end, so this method handles that mapping.
   *
   * @param implClass The implementation class
   * @param interfaceMethod The interface method to find implementation for
   * @return The implementation method
   * @throws IllegalStateException if no matching implementation method is found
   */
  public fun findImplementationMethod(implClass: Class<*>, interfaceMethod: Method): Method {
    val methodName = interfaceMethod.name
    val interfaceParams = interfaceMethod.parameterTypes

    // First try exact match (for non-suspend methods)
    try {
      return implClass.getMethod(methodName, *interfaceParams)
    } catch (_: NoSuchMethodException) {
      // Not found, continue to search for suspend variant
    }

    // For suspend methods, the implementation has an extra Continuation parameter
    return findSuspendMethod(implClass, methodName, interfaceParams)
      ?: throw IllegalStateException(
        "Could not find implementation method for ${interfaceMethod.name} in ${implClass.name}"
      )
  }

  /**
   * Find a suspend method variant that matches the given interface method.
   */
  private fun findSuspendMethod(
    implClass: Class<*>,
    methodName: String,
    interfaceParams: Array<Class<*>>
  ): Method? {
    val continuationClass = Continuation::class.java
    return implClass.methods.firstOrNull { method ->
      method.name == methodName && isSuspendMethodMatch(method, interfaceParams, continuationClass)
    }
  }

  /**
   * Check if a method is a suspend method that matches the expected interface parameters.
   */
  private fun isSuspendMethodMatch(
    method: Method,
    interfaceParams: Array<Class<*>>,
    continuationClass: Class<*>
  ): Boolean {
    val params = method.parameterTypes
    return params.size == interfaceParams.size + 1 &&
      continuationClass.isAssignableFrom(params.last()) &&
      paramsMatch(interfaceParams, params)
  }

  /**
   * Check if a method is a Kotlin suspend function.
   *
   * Suspend functions are compiled with an additional Continuation parameter
   * as the last parameter.
   *
   * @param method The method to check
   * @return true if the method is a suspend function
   */
  public fun isSuspendMethod(method: Method): Boolean {
    val params = method.parameterTypes
    return params.isNotEmpty() && Continuation::class.java.isAssignableFrom(params.last())
  }

  /**
   * Check if interface parameters match implementation parameters (excluding Continuation).
   */
  private fun paramsMatch(interfaceParams: Array<Class<*>>, implParams: Array<Class<*>>): Boolean {
    for (i in interfaceParams.indices) {
      if (interfaceParams[i] != implParams[i]) {
        return false
      }
    }
    return true
  }
}
