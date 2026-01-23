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

package io.temporal.kotlin.testing

import io.temporal.activity.Activity
import io.temporal.activity.ActivityInterface
import io.temporal.activity.DynamicActivity
import io.temporal.common.converter.EncodedValues
import io.temporal.common.metadata.POJOActivityInterfaceMetadata
import io.temporal.failure.ApplicationFailure
import kotlinx.coroutines.runBlocking
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.Continuation
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

/**
 * Registry for activity mocks used in testing.
 *
 * This registry holds mappings from activity type names to mock implementations.
 * It is used by [KMockDynamicActivityHandler] to route activity calls to mocks.
 *
 * Thread-safe for concurrent access during test execution.
 */
internal class KActivityMockRegistry {
    // Map from activity type name to (mock instance, method)
    private val handlers = ConcurrentHashMap<String, MockHandler>()

    internal data class MockHandler(
        val mock: Any,
        val method: Method,
        val isSuspend: Boolean,
    )

    /**
     * Register an activity mock.
     *
     * Extracts all activity methods from the mock's interfaces and registers them.
     *
     * @param mock The mock instance implementing one or more activity interfaces
     */
    fun register(mock: Any) {
        val mockClass = mock::class.java

        // Find all activity interfaces implemented by the mock
        val activityInterfaces = findActivityInterfaces(mockClass)
        if (activityInterfaces.isEmpty()) {
            throw IllegalArgumentException(
                "Mock does not implement any @ActivityInterface annotated interfaces: ${mockClass.name}",
            )
        }

        // Register each activity method
        for (activityInterface in activityInterfaces) {
            val metadata = POJOActivityInterfaceMetadata.newInstance(activityInterface)
            for (methodMetadata in metadata.methodsMetadata) {
                val activityType = methodMetadata.activityTypeName
                val interfaceMethod = methodMetadata.method
                val isSuspend = isSuspendMethod(interfaceMethod)

                handlers[activityType] = MockHandler(
                    mock = mock,
                    method = interfaceMethod,
                    isSuspend = isSuspend,
                )
            }
        }
    }

    /**
     * Find the handler for the given activity type.
     *
     * @param activityType The activity type name
     * @return The mock handler, or null if not registered
     */
    fun findHandler(activityType: String): MockHandler? = handlers[activityType]

    /**
     * Clear all registered mocks.
     */
    fun clear() {
        handlers.clear()
    }

    /**
     * Check if any mocks are registered.
     */
    fun isEmpty(): Boolean = handlers.isEmpty()

    /**
     * Get all registered activity types (for error messages).
     */
    fun registeredTypes(): Set<String> = handlers.keys.toSet()

    private fun findActivityInterfaces(clazz: Class<*>): List<Class<*>> {
        val result = mutableListOf<Class<*>>()
        for (iface in clazz.interfaces) {
            if (iface.isAnnotationPresent(ActivityInterface::class.java)) {
                result.add(iface)
            }
            // Check parent interfaces recursively
            result.addAll(findActivityInterfaces(iface))
        }
        return result.distinct()
    }

    private fun isSuspendMethod(method: Method): Boolean {
        // A suspend function has Continuation as its last parameter
        val params = method.parameterTypes
        return params.isNotEmpty() &&
            Continuation::class.java.isAssignableFrom(params.last())
    }
}

/**
 * Dynamic activity handler that routes activity calls to registered mocks.
 *
 * This handler is automatically registered by [KTestWorkflowExtension] and intercepts
 * all activity invocations that don't have an explicitly registered implementation.
 *
 * When an activity is called:
 * 1. The handler looks up the mock in [KActivityMockRegistry]
 * 2. If found, it decodes the arguments and invokes the mock method
 * 3. If not found, it throws a clear error message
 *
 * Supports both regular and suspend activity mocks.
 */
internal class KMockDynamicActivityHandler(
    private val registry: KActivityMockRegistry,
) : DynamicActivity {

    override fun execute(args: EncodedValues): Any? {
        val context = Activity.getExecutionContext()
        val activityType = context.info.activityType

        val handler = registry.findHandler(activityType)
            ?: throw ApplicationFailure.newNonRetryableFailure(
                buildErrorMessage(activityType),
                "ActivityNotRegistered",
            )

        // Decode arguments based on method signature
        val decodedArgs = decodeArgs(args, handler.method, handler.isSuspend)

        // Invoke the mock
        return invokeMock(handler.mock, handler.method, handler.isSuspend, decodedArgs)
    }

    private fun buildErrorMessage(activityType: String): String {
        val registeredTypes = registry.registeredTypes()
        return if (registeredTypes.isEmpty()) {
            "No activity implementation or mock registered for type: $activityType. " +
                "No mocks have been registered. " +
                "Use testEnv.registerActivitiesImplementations() to register activity mocks."
        } else {
            "No activity implementation or mock registered for type: $activityType. " +
                "Registered types: $registeredTypes"
        }
    }

    private fun decodeArgs(
        encodedValues: EncodedValues,
        method: Method,
        isSuspend: Boolean,
    ): Array<Any?> {
        val paramTypes = method.parameterTypes
        // For suspend methods, exclude the Continuation parameter
        val argCount = if (isSuspend) paramTypes.size - 1 else paramTypes.size

        if (argCount == 0) {
            return emptyArray()
        }

        return Array(argCount) { index ->
            val paramType = paramTypes[index]
            val genericType = method.genericParameterTypes[index]
            encodedValues.get(index, paramType, genericType)
        }
    }

    private fun invokeMock(
        mock: Any,
        method: Method,
        isSuspend: Boolean,
        args: Array<Any?>,
    ): Any? {
        return try {
            if (isSuspend) {
                // For suspend methods, use Kotlin reflection with runBlocking
                val kotlinFunction = method.kotlinFunction
                    ?: throw IllegalStateException(
                        "Could not get Kotlin function for suspend method: ${method.name}",
                    )

                runBlocking {
                    kotlinFunction.callSuspend(mock, *args)
                }
            } else {
                // Regular method - direct invocation
                method.invoke(mock, *args)
            }
        } catch (e: InvocationTargetException) {
            // Unwrap the actual exception
            throw e.targetException
        }
    }
}
