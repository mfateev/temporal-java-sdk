package io.temporal.kotlin.common

import io.temporal.common.converter.EncodedValues
import java.lang.reflect.Type

public class KEncodedValues(@PublishedApi internal val delegate: EncodedValues) {
  public val size: Int get() = delegate.size

  public fun isEmpty(): Boolean = size == 0

  // Primary API - reified generics with default index
  public inline fun <reified T> get(index: Int = 0): T =
    delegate.get(index, T::class.java)

  public inline fun <reified T> get(index: Int, genericType: Type): T =
    delegate.get(index, T::class.java, genericType)

  // KClass-based access
  public fun <T : Any> get(index: Int, type: kotlin.reflect.KClass<T>): T =
    delegate.get(index, type.java)

  // Destructuring support
  public inline operator fun <reified T> component1(): T = get(0)
  public inline operator fun <reified T> component2(): T = get(1)
  public inline operator fun <reified T> component3(): T = get(2)

  // Java interop
  public fun toEncodedValues(): EncodedValues = delegate
}
