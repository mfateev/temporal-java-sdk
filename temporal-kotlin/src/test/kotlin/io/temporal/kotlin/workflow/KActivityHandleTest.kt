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

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

class KActivityHandleTest {

  @Test
  fun `isCompleted returns false before completion`() {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    assertFalse(handle.isCompleted)
  }

  @Test
  fun `isCompleted returns true after completion`() {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    deferred.complete("result")

    assertTrue(handle.isCompleted)
  }

  @Test
  fun `isCompleted returns true after exceptional completion`() {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    deferred.completeExceptionally(RuntimeException("error"))

    assertTrue(handle.isCompleted)
  }

  @Test
  fun `await returns result after completion`() = runBlocking {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    deferred.complete("test-result")

    val result = handle.await()
    assertEquals("test-result", result)
  }

  @Test
  fun `await throws exception after exceptional completion`() = runBlocking {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    deferred.completeExceptionally(RuntimeException("activity failed"))

    try {
      handle.await()
      fail("Expected exception")
    } catch (e: RuntimeException) {
      assertEquals("activity failed", e.message)
    }
  }

  @Test
  fun `cancel invokes cancellation callback`() {
    val deferred = CompletableDeferred<String>()
    val cancelled = AtomicBoolean(false)
    val reasonRef = AtomicReference<String?>()

    val callback = Consumer<Exception?> { e ->
      cancelled.set(true)
      reasonRef.set(e?.message)
    }
    val handle = KActivityHandleImpl(deferred, callback)

    handle.cancel("test reason")

    assertTrue(cancelled.get())
    assertEquals("test reason", reasonRef.get())
  }

  @Test
  fun `cancel uses default message when reason is null`() {
    val deferred = CompletableDeferred<String>()
    val reasonRef = AtomicReference<String?>()

    val callback = Consumer<Exception?> { e ->
      reasonRef.set(e?.message)
    }
    val handle = KActivityHandleImpl(deferred, callback)

    handle.cancel()

    assertEquals("Activity cancelled", reasonRef.get())
  }

  @Test
  fun `cancel does nothing when already completed`() {
    val deferred = CompletableDeferred<String>()
    val cancelled = AtomicBoolean(false)

    val callback = Consumer<Exception?> { cancelled.set(true) }
    val handle = KActivityHandleImpl(deferred, callback)

    deferred.complete("result")
    handle.cancel("too late")

    assertFalse(cancelled.get())
  }

  @Test
  fun `cancel does nothing when callback is null`() {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl<String>(deferred, null)

    // Should not throw
    handle.cancel("reason")
  }

  @Test
  fun `complete sets result`() = runBlocking {
    val deferred = CompletableDeferred<Int>()
    val handle = KActivityHandleImpl(deferred, null)

    handle.complete(42)

    assertTrue(handle.isCompleted)
    assertEquals(42, handle.await())
  }

  @Test
  fun `completeExceptionally sets exception`() = runBlocking {
    val deferred = CompletableDeferred<String>()
    val handle = KActivityHandleImpl(deferred, null)

    handle.completeExceptionally(IllegalStateException("test error"))

    assertTrue(handle.isCompleted)
    try {
      handle.await()
      fail("Expected exception")
    } catch (e: IllegalStateException) {
      assertEquals("test error", e.message)
    }
  }
}
