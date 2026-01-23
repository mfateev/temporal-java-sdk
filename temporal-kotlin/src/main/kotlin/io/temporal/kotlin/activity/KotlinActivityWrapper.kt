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
import io.temporal.activity.TypedDynamicActivity
import io.temporal.common.converter.EncodedValues
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.lang.reflect.Method
import kotlin.coroutines.Continuation
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

/**
 * Wraps a Kotlin activity method (suspend or regular) as a [TypedDynamicActivity].
 *
 * This wrapper is registered directly with the Java SDK worker, allowing
 * the Kotlin SDK to handle activity invocation without going through
 * the Java SDK's POJO activity registration (which doesn't support suspend functions).
 *
 * For suspend methods: Uses `useLocalManualCompletion()` to achieve non-blocking execution.
 * For regular methods: Invokes directly and returns the result.
 *
 * @param activityTypeName The activity type name for registration
 * @param implementation The activity implementation instance
 * @param method The Java method from the interface (used for parameter types)
 * @param dispatcher The coroutine dispatcher for suspend activities
 */
internal class KotlinActivityWrapper(
  private val activityTypeName: String,
  private val implementation: Any,
  private val method: Method,
  private val dispatcher: CoroutineDispatcher = Dispatchers.Default
) : TypedDynamicActivity {

  private val isSuspend: Boolean = isSuspendMethod(method)
  private val kFunction: KFunction<*>? = method.kotlinFunction

  // Parameter types excluding Continuation for suspend methods
  private val parameterTypes: Array<Class<*>> = if (isSuspend) {
    method.parameterTypes.dropLast(1).toTypedArray()
  } else {
    method.parameterTypes
  }

  private val genericParameterTypes: Array<java.lang.reflect.Type> = if (isSuspend) {
    method.genericParameterTypes.dropLast(1).toTypedArray()
  } else {
    method.genericParameterTypes
  }

  override fun getActivityType(): String = activityTypeName

  override fun execute(args: EncodedValues): Any? {
    // Decode arguments (excluding Continuation parameter for suspend methods)
    val decodedArgs = decodeArgs(args)

    return if (isSuspend) {
      executeSuspend(decodedArgs)
    } else {
      executeRegular(decodedArgs)
    }
  }

  private fun decodeArgs(encodedValues: EncodedValues): Array<Any?> {
    if (parameterTypes.isEmpty()) {
      return emptyArray()
    }
    return Array(parameterTypes.size) { index ->
      encodedValues.get(index, parameterTypes[index], genericParameterTypes[index])
    }
  }

  private fun executeRegular(args: Array<Any?>): Any? {
    return kFunction?.call(implementation, *args)
      ?: method.invoke(implementation, *args)
  }

  private fun executeSuspend(args: Array<Any?>): Any? {
    val context = Activity.getExecutionContext()

    // Get manual completion client - this marks the activity for async completion
    // and ensures the slot permit is released when we call complete/fail/reportCancellation
    val completionClient = context.useLocalManualCompletion()

    // Create a supervisor job so failures don't propagate to parent scopes
    val job = SupervisorJob()
    val scope = CoroutineScope(dispatcher + job)

    // Launch coroutine - this returns immediately, freeing the executor thread
    scope.launch {
      // Create activity context for coroutine access
      val suspendContext = SuspendActivityContext(context, completionClient, job)

      // Create thread context element to propagate context to coroutine threads
      // This enables KActivityContext.current.heartbeat() to work correctly in suspend activities
      val suspendExecutionContext = SuspendActivityExecutionContext(context, completionClient)
      val threadContextElement = SuspendActivityThreadContextElement(suspendExecutionContext)

      withContext(SuspendActivityContextElement(suspendContext) + threadContextElement) {
        try {
          // Execute the suspend function
          val result = kFunction!!.callSuspend(implementation, *args)

          // Complete successfully - uses Dispatchers.IO for the blocking gRPC call
          withContext(Dispatchers.IO) {
            completionClient.complete(result)
          }
        } catch (e: CancellationException) {
          // Coroutine was cancelled (activity cancellation detected via heartbeat)
          withContext(Dispatchers.IO + NonCancellable) {
            completionClient.reportCancellation(null)
          }
        } catch (e: Throwable) {
          // Activity failed with an exception
          withContext(Dispatchers.IO + NonCancellable) {
            completionClient.fail(e)
          }
        }
      }
    }

    // Return null - actual result sent via completion client
    return null
  }

  companion object {
    /**
     * Determines if a method is a Kotlin suspend function.
     * Suspend functions have a Continuation parameter as the last parameter.
     */
    private fun isSuspendMethod(method: Method): Boolean {
      val params = method.parameterTypes
      return params.isNotEmpty() &&
        Continuation::class.java.isAssignableFrom(params.last())
    }
  }
}
