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

import io.temporal.common.converter.EncodedValues
import java.lang.reflect.Type

/**
 * Kotlin-friendly wrapper for accessing encoded workflow/activity arguments.
 *
 * Provides type-safe access to serialized values with reified generics and
 * Kotlin destructuring support.
 *
 * Example:
 * ```kotlin
 * // Access by index with reified type
 * val name: String = args.get(0)
 * val count: Int = args.get(1)
 *
 * // Destructuring support
 * val (name: String, count: Int) = args
 * ```
 *
 * @property size The number of encoded values
 */
// Wraps Java SDK's EncodedValues for Kotlin-idiomatic access
public class KEncodedValues(@PublishedApi internal val delegate: EncodedValues) {

  /**
   * The number of encoded values.
   */
  public val size: Int get() = delegate.size

  /**
   * Returns true if there are no encoded values.
   */
  public fun isEmpty(): Boolean = size == 0

  /**
   * Gets a value at the specified index, decoded to the reified type.
   *
   * @param T The expected type of the value
   * @param index The index of the value (default is 0)
   * @return The decoded value
   */
  public inline fun <reified T> get(index: Int = 0): T =
    delegate.get(index, T::class.java)

  /**
   * Gets a value at the specified index with a generic type hint.
   *
   * Use this overload when dealing with generic types like `List<String>`.
   *
   * @param T The expected type of the value
   * @param index The index of the value
   * @param genericType The generic type for proper deserialization
   * @return The decoded value
   */
  public inline fun <reified T> get(index: Int, genericType: Type): T =
    delegate.get(index, T::class.java, genericType)

  /**
   * Gets a value at the specified index using a KClass.
   *
   * @param T The expected type of the value
   * @param index The index of the value
   * @param type The KClass of the expected type
   * @return The decoded value
   */
  public fun <T : Any> get(index: Int, type: kotlin.reflect.KClass<T>): T =
    delegate.get(index, type.java)

  // Destructuring support
  /** Destructuring component for the first value. */
  public inline operator fun <reified T> component1(): T = get(0)

  /** Destructuring component for the second value. */
  public inline operator fun <reified T> component2(): T = get(1)

  /** Destructuring component for the third value. */
  public inline operator fun <reified T> component3(): T = get(2)

  /**
   * Returns the underlying Java EncodedValues for interoperability.
   */
  public fun toEncodedValues(): EncodedValues = delegate
}
