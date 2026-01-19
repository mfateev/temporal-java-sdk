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

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

class KArgsTest {

  @Test
  fun `kargs2 creates KArgs2 with correct values`() {
    val args = kargs("hello", 42)
    assertEquals("hello", args.a1)
    assertEquals(42, args.a2)
  }

  @Test
  fun `kargs2 toArray returns correct array`() {
    val args = kargs("hello", 42)
    assertArrayEquals(arrayOf<Any?>("hello", 42), args.toArray())
  }

  @Test
  fun `kargs3 creates KArgs3 with correct values`() {
    val args = kargs("a", "b", "c")
    assertEquals("a", args.a1)
    assertEquals("b", args.a2)
    assertEquals("c", args.a3)
  }

  @Test
  fun `kargs3 toArray returns correct array`() {
    val args = kargs(1, 2, 3)
    assertArrayEquals(arrayOf<Any?>(1, 2, 3), args.toArray())
  }

  @Test
  fun `kargs4 creates KArgs4 with correct values`() {
    val args = kargs("a", "b", "c", "d")
    assertEquals("a", args.a1)
    assertEquals("b", args.a2)
    assertEquals("c", args.a3)
    assertEquals("d", args.a4)
  }

  @Test
  fun `kargs4 toArray returns correct array`() {
    val args = kargs(1, 2, 3, 4)
    assertArrayEquals(arrayOf<Any?>(1, 2, 3, 4), args.toArray())
  }

  @Test
  fun `kargs5 creates KArgs5 with correct values`() {
    val args = kargs("a", "b", "c", "d", "e")
    assertEquals("a", args.a1)
    assertEquals("b", args.a2)
    assertEquals("c", args.a3)
    assertEquals("d", args.a4)
    assertEquals("e", args.a5)
  }

  @Test
  fun `kargs5 toArray returns correct array`() {
    val args = kargs(1, 2, 3, 4, 5)
    assertArrayEquals(arrayOf<Any?>(1, 2, 3, 4, 5), args.toArray())
  }

  @Test
  fun `kargs6 creates KArgs6 with correct values`() {
    val args = kargs("a", "b", "c", "d", "e", "f")
    assertEquals("a", args.a1)
    assertEquals("b", args.a2)
    assertEquals("c", args.a3)
    assertEquals("d", args.a4)
    assertEquals("e", args.a5)
    assertEquals("f", args.a6)
  }

  @Test
  fun `kargs6 toArray returns correct array`() {
    val args = kargs(1, 2, 3, 4, 5, 6)
    assertArrayEquals(arrayOf<Any?>(1, 2, 3, 4, 5, 6), args.toArray())
  }

  @Test
  fun `kargs handles null values`() {
    val args = kargs<String?, Int?>(null, null)
    assertEquals(null, args.a1)
    assertEquals(null, args.a2)
    assertArrayEquals(arrayOf<Any?>(null, null), args.toArray())
  }

  @Test
  fun `kargs handles mixed types`() {
    val args = kargs("string", 42, 3.14, true, listOf(1, 2, 3), mapOf("key" to "value"))
    assertEquals("string", args.a1)
    assertEquals(42, args.a2)
    assertEquals(3.14, args.a3, 0.001)
    assertEquals(true, args.a4)
    assertEquals(listOf(1, 2, 3), args.a5)
    assertEquals(mapOf("key" to "value"), args.a6)
  }

  @Test
  fun `KArgs data classes have correct equality`() {
    val args1 = kargs("a", "b")
    val args2 = kargs("a", "b")
    val args3 = kargs("a", "c")

    assertEquals(args1, args2)
    assertEquals(args1.hashCode(), args2.hashCode())
    assert(args1 != args3)
  }

  @Test
  fun `KArgs implements sealed interface`() {
    val args2: KArgs = kargs("a", "b")
    val args3: KArgs = kargs("a", "b", "c")
    val args4: KArgs = kargs("a", "b", "c", "d")
    val args5: KArgs = kargs("a", "b", "c", "d", "e")
    val args6: KArgs = kargs("a", "b", "c", "d", "e", "f")

    // All should be usable as KArgs
    assertEquals(2, args2.toArray().size)
    assertEquals(3, args3.toArray().size)
    assertEquals(4, args4.toArray().size)
    assertEquals(5, args5.toArray().size)
    assertEquals(6, args6.toArray().size)
  }
}
