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

@file:OptIn(kotlin.time.ExperimentalTime::class)

package io.temporal.kotlin

import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import java.time.Duration as JavaDuration

class DurationExtTest {

  @Test
  fun `toJava converts Kotlin Duration to Java Duration`() {
    val kotlinDuration = 30.seconds
    val javaDuration = kotlinDuration.toJava()

    assertEquals(JavaDuration.ofSeconds(30), javaDuration)
  }

  @Test
  fun `toKotlin converts Java Duration to Kotlin Duration`() {
    val javaDuration = JavaDuration.ofMinutes(5)
    val kotlinDuration = javaDuration.toKotlin()

    assertEquals(5.minutes, kotlinDuration)
  }

  @Test
  fun `round trip conversion preserves value`() {
    val original = 2.hours
    val roundTripped = original.toJava().toKotlin()

    assertEquals(original, roundTripped)
  }

  @Test
  fun `zero duration converts correctly`() {
    val kotlinZero = 0.seconds
    val javaZero = JavaDuration.ZERO

    assertEquals(javaZero, kotlinZero.toJava())
    assertEquals(kotlinZero, javaZero.toKotlin())
  }

  @Test
  fun `millisecond precision is preserved`() {
    val kotlinDuration = 1500.milliseconds
    val javaDuration = kotlinDuration.toJava()

    assertEquals(1, javaDuration.seconds)
    assertEquals(500_000_000, javaDuration.nano)
  }

  @Test
  fun `large duration converts correctly`() {
    val kotlinDuration = 24.hours
    val javaDuration = kotlinDuration.toJava()

    assertEquals(JavaDuration.ofHours(24), javaDuration)
  }
}
