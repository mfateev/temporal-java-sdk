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

import io.temporal.activity.Activity
import io.temporal.activity.DynamicActivity
import io.temporal.common.converter.EncodedValues
import kotlinx.coroutines.CoroutineDispatcher
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.javaMethod

/**
 * Wraps a Kotlin activity implementation containing suspend functions as a DynamicActivity.
 *
 * This wrapper enables registration of suspend activities with the Java SDK by:
 * 1. Implementing DynamicActivity to intercept all activity invocations
 * 2. Routing suspend methods to [SuspendActivityInvoker] for coroutine execution
 * 3. Routing non-suspend methods to direct reflection invocation
 *
 * The DynamicActivity pattern is used because:
 * - It allows us to intercept activity calls before they reach the POJO executor
 * - We can apply different execution strategies based on whether the method is suspend
 * - It integrates cleanly with the existing Java SDK activity registration
 */
internal class SuspendActivityWrapper(
  private val activityImplementation: Any,
  private val activityInterfaces: List<KClass<*>>,
  private val dispatcher: CoroutineDispatcher
) {
  // Map of activity type name to method info
  private val methodMap: Map<String, MethodInfo> = buildMethodMap()

  private data class MethodInfo(
    val method: KFunction<*>,
    val isSuspend: Boolean,
    val invoker: SuspendActivityInvoker?
  )

  private fun buildMethodMap(): Map<String, MethodInfo> {
    val map = mutableMapOf<String, MethodInfo>()

    for (iface in activityInterfaces) {
      for (method in iface.memberFunctions) {
        if (!isActivityMethod(method)) continue

        val activityType = getActivityTypeName(method, iface)
        val invoker = if (method.isSuspend) {
          // Find the implementation method
          val implMethod = findImplementationMethod(method)
          SuspendActivityInvoker(activityImplementation, implMethod, dispatcher)
        } else {
          null
        }

        map[activityType] = MethodInfo(
          method = findImplementationMethod(method),
          isSuspend = method.isSuspend,
          invoker = invoker
        )
      }
    }

    return map
  }

  /**
   * Finds the implementation method corresponding to an interface method.
   */
  private fun findImplementationMethod(interfaceMethod: KFunction<*>): KFunction<*> {
    val implClass = activityImplementation::class
    val methodName = interfaceMethod.name
    val paramCount = interfaceMethod.parameters.size - 1 // Exclude 'this' parameter

    // Find matching method in implementation class
    return implClass.memberFunctions.find { implMethod ->
      implMethod.name == methodName &&
        implMethod.parameters.size - 1 == paramCount &&
        implMethod.isSuspend == interfaceMethod.isSuspend
    } ?: throw IllegalStateException(
      "Could not find implementation for method $methodName in ${implClass.simpleName}"
    )
  }

  /**
   * Checks if a function is an activity method.
   */
  private fun isActivityMethod(method: KFunction<*>): Boolean {
    val javaMethod = method.javaMethod ?: return false
    // Exclude default interface methods and Object methods
    return !javaMethod.isDefault &&
      method.name != "equals" &&
      method.name != "hashCode" &&
      method.name != "toString"
  }

  /**
   * Creates a DynamicActivity that routes calls to the appropriate executor.
   */
  fun createDynamicActivity(): DynamicActivity {
    return DynamicActivity { encodedValues ->
      val context = Activity.getExecutionContext()
      val activityType = context.info.activityType

      val methodInfo = methodMap[activityType]
        ?: throw IllegalArgumentException(
          "Unknown activity type: $activityType. Known types: ${methodMap.keys}"
        )

      if (methodInfo.isSuspend) {
        // Use the suspend invoker - returns immediately, completes via callback
        val args = deserializeArgs(encodedValues, methodInfo.method)
        methodInfo.invoker!!.invoke(context, args)
        // Mark as manual completion since SuspendActivityInvoker handles it
        // The invoker already called useLocalManualCompletion()
        null
      } else {
        // Regular method - invoke directly
        val args = deserializeArgs(encodedValues, methodInfo.method)
        invokeRegularMethod(methodInfo.method, args)
      }
    }
  }

  /**
   * Deserializes activity arguments from EncodedValues.
   */
  private fun deserializeArgs(encodedValues: EncodedValues, method: KFunction<*>): Array<Any?> {
    val params = method.parameters.drop(1) // Skip 'this' parameter
    if (params.isEmpty()) {
      return emptyArray()
    }

    val args = Array<Any?>(params.size) { index ->
      val param = params[index]
      val paramType = param.type.classifier as? KClass<*>
        ?: throw IllegalStateException("Cannot determine type for parameter ${param.name}")
      encodedValues.get(index, paramType.java)
    }

    return args
  }

  /**
   * Invokes a regular (non-suspend) method via reflection.
   */
  private fun invokeRegularMethod(method: KFunction<*>, args: Array<Any?>): Any? {
    return method.call(activityImplementation, *args)
  }
}
