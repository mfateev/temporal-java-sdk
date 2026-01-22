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

package io.temporal.kotlin.workflow

import io.temporal.kotlin.common.KEncodedValues

/**
 * Handler for dynamic signals - receives any signal not handled by @SignalMethod.
 *
 * Example:
 * ```kotlin
 * KWorkflow.registerDynamicSignalHandler { signalName, args ->
 *     when (signalName) {
 *         "updateName" -> name = args.get<String>(0)
 *         "addItem" -> items.add(args.get<Item>(0))
 *         else -> println("Unknown signal: $signalName")
 *     }
 * }
 * ```
 */
public fun interface KDynamicSignalHandler {
  /**
   * Handle a signal.
   *
   * @param signalName The name of the signal
   * @param args The signal arguments
   */
  public fun handle(signalName: String, args: KEncodedValues)
}

/**
 * Handler for dynamic queries - receives any query not handled by @QueryMethod.
 *
 * Example:
 * ```kotlin
 * KWorkflow.registerDynamicQueryHandler { queryName, args ->
 *     when (queryName) {
 *         "getStatus" -> status
 *         "getItem" -> items[args.get<Int>(0)]
 *         else -> null
 *     }
 * }
 * ```
 */
public fun interface KDynamicQueryHandler {
  /**
   * Handle a query and return a result.
   *
   * @param queryName The name of the query
   * @param args The query arguments
   * @return The query result
   */
  public fun handle(queryName: String, args: KEncodedValues): Any?
}

/**
 * Handler for dynamic updates - receives any update not handled by @UpdateMethod.
 *
 * Example:
 * ```kotlin
 * KWorkflow.registerDynamicUpdateHandler(
 *     handler = { updateName, args ->
 *         when (updateName) {
 *             "setName" -> {
 *                 name = args.get<String>(0)
 *                 name
 *             }
 *             else -> throw IllegalArgumentException("Unknown update: $updateName")
 *         }
 *     },
 *     validator = { updateName, args ->
 *         if (updateName == "setName" && args.get<String>(0).isEmpty()) {
 *             throw IllegalArgumentException("Name cannot be empty")
 *         }
 *     }
 * )
 * ```
 */
public fun interface KDynamicUpdateHandler {
  /**
   * Handle an update and return a result.
   *
   * @param updateName The name of the update
   * @param args The update arguments
   * @return The update result
   */
  public suspend fun handle(updateName: String, args: KEncodedValues): Any?
}

/**
 * Validator for dynamic updates - validates before the update handler is called.
 *
 * Throw an exception to reject the update.
 */
public fun interface KDynamicUpdateValidator {
  /**
   * Validate an update request. Throw an exception to reject.
   *
   * @param updateName The name of the update
   * @param args The update arguments
   */
  public fun validate(updateName: String, args: KEncodedValues)
}
