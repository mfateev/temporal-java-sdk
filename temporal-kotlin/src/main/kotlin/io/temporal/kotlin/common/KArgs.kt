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

package io.temporal.kotlin.common

/**
 * Marker interface for type-safe multi-argument wrappers.
 *
 * When calling activities, workflows, signals, or other Temporal operations with
 * 2 or more arguments, use `kargs()` to wrap arguments in a type-safe container.
 *
 * Example:
 * ```kotlin
 * // 2 arguments
 * KWorkflow.executeActivity(
 *     GreetingActivities::composeGreeting,
 *     kargs("Hello", name),
 *     KActivityOptions(startToCloseTimeout = 30.seconds)
 * )
 * ```
 *
 * @see kargs
 */
public sealed interface KArgs {
  /**
   * Returns the arguments as an array.
   */
  public fun toArray(): Array<Any?>
}

/**
 * Type-safe wrapper for 2 arguments.
 */
public data class KArgs2<A1, A2>(val a1: A1, val a2: A2) : KArgs {
  override fun toArray(): Array<Any?> = arrayOf(a1, a2)
}

/**
 * Type-safe wrapper for 3 arguments.
 */
public data class KArgs3<A1, A2, A3>(val a1: A1, val a2: A2, val a3: A3) : KArgs {
  override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3)
}

/**
 * Type-safe wrapper for 4 arguments.
 */
public data class KArgs4<A1, A2, A3, A4>(val a1: A1, val a2: A2, val a3: A3, val a4: A4) : KArgs {
  override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4)
}

/**
 * Type-safe wrapper for 5 arguments.
 */
public data class KArgs5<A1, A2, A3, A4, A5>(
  val a1: A1,
  val a2: A2,
  val a3: A3,
  val a4: A4,
  val a5: A5
) : KArgs {
  override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4, a5)
}

/**
 * Type-safe wrapper for 6 arguments.
 */
public data class KArgs6<A1, A2, A3, A4, A5, A6>(
  val a1: A1,
  val a2: A2,
  val a3: A3,
  val a4: A4,
  val a5: A5,
  val a6: A6
) : KArgs {
  override fun toArray(): Array<Any?> = arrayOf(a1, a2, a3, a4, a5, a6)
}

/**
 * Creates a type-safe wrapper for 2 arguments.
 *
 * Example:
 * ```kotlin
 * KWorkflow.executeActivity(
 *     Activities::process,
 *     kargs(orderId, customerId),
 *     options
 * )
 * ```
 */
public fun <A1, A2> kargs(a1: A1, a2: A2): KArgs2<A1, A2> = KArgs2(a1, a2)

/**
 * Creates a type-safe wrapper for 3 arguments.
 */
public fun <A1, A2, A3> kargs(a1: A1, a2: A2, a3: A3): KArgs3<A1, A2, A3> = KArgs3(a1, a2, a3)

/**
 * Creates a type-safe wrapper for 4 arguments.
 */
public fun <A1, A2, A3, A4> kargs(a1: A1, a2: A2, a3: A3, a4: A4): KArgs4<A1, A2, A3, A4> =
  KArgs4(a1, a2, a3, a4)

/**
 * Creates a type-safe wrapper for 5 arguments.
 */
public fun <A1, A2, A3, A4, A5> kargs(
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5
): KArgs5<A1, A2, A3, A4, A5> = KArgs5(a1, a2, a3, a4, a5)

/**
 * Creates a type-safe wrapper for 6 arguments.
 */
public fun <A1, A2, A3, A4, A5, A6> kargs(
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5,
  a6: A6
): KArgs6<A1, A2, A3, A4, A5, A6> = KArgs6(a1, a2, a3, a4, a5, a6)
