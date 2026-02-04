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

package io.temporal.internal.common;

import static org.junit.Assert.*;

import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.internal.history.LocalActivityMarkerMetadata;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class CorePayloadConverterTest {

  @Test
  public void testNullRoundTrip() {
    Payload payload = CorePayloadConverter.toPayload(null);
    assertEquals("binary/null", payload.getMetadataOrThrow("encoding").toStringUtf8());

    String result = CorePayloadConverter.fromPayload(payload, String.class);
    assertNull(result);
  }

  @Test
  public void testStringRoundTrip() {
    String original = "hello world";
    Payload payload = CorePayloadConverter.toPayload(original);
    assertEquals("json/plain", payload.getMetadataOrThrow("encoding").toStringUtf8());
    assertEquals("\"hello world\"", payload.getData().toStringUtf8());

    String result = CorePayloadConverter.fromPayload(payload, String.class);
    assertEquals(original, result);
  }

  @Test
  public void testStringWithSpecialChars() {
    String original = "hello \"world\" with\nnewline\tand\\backslash";
    Payload payload = CorePayloadConverter.toPayload(original);
    String result = CorePayloadConverter.fromPayload(payload, String.class);
    assertEquals(original, result);
  }

  @Test
  public void testStringWithUnicode() {
    String original = "hello \u0000 control char";
    Payload payload = CorePayloadConverter.toPayload(original);
    String result = CorePayloadConverter.fromPayload(payload, String.class);
    assertEquals(original, result);
  }

  @Test
  public void testIntegerRoundTrip() {
    Integer original = 42;
    Payload payload = CorePayloadConverter.toPayload(original);
    assertEquals("42", payload.getData().toStringUtf8());

    Integer result = CorePayloadConverter.fromPayload(payload, Integer.class);
    assertEquals(original, result);
  }

  @Test
  public void testNegativeInteger() {
    Integer original = -12345;
    Payload payload = CorePayloadConverter.toPayload(original);
    Integer result = CorePayloadConverter.fromPayload(payload, Integer.class);
    assertEquals(original, result);
  }

  @Test
  public void testLongRoundTrip() {
    Long original = 9876543210L;
    Payload payload = CorePayloadConverter.toPayload(original);
    assertEquals("9876543210", payload.getData().toStringUtf8());

    Long result = CorePayloadConverter.fromPayload(payload, Long.class);
    assertEquals(original, result);
  }

  @Test
  public void testBooleanTrue() {
    Boolean original = true;
    Payload payload = CorePayloadConverter.toPayload(original);
    assertEquals("true", payload.getData().toStringUtf8());

    Boolean result = CorePayloadConverter.fromPayload(payload, Boolean.class);
    assertEquals(original, result);
  }

  @Test
  public void testBooleanFalse() {
    Boolean original = false;
    Payload payload = CorePayloadConverter.toPayload(original);
    assertEquals("false", payload.getData().toStringUtf8());

    Boolean result = CorePayloadConverter.fromPayload(payload, Boolean.class);
    assertEquals(original, result);
  }

  @Test
  public void testLocalActivityMarkerMetadataRoundTrip() {
    LocalActivityMarkerMetadata original = new LocalActivityMarkerMetadata(3, 1234567890L);
    original.setBackoff(Duration.ofMillis(5000));

    Payload payload = CorePayloadConverter.toPayload(original);
    String json = payload.getData().toStringUtf8();

    // Verify JSON format matches what Jackson would produce
    assertTrue(json.contains("\"firstSkd\":1234567890"));
    assertTrue(json.contains("\"atpt\":3"));
    assertTrue(json.contains("\"backoff\":5000"));

    LocalActivityMarkerMetadata result =
        CorePayloadConverter.fromPayload(payload, LocalActivityMarkerMetadata.class);
    assertEquals(original.getOriginalScheduledTimestamp(), result.getOriginalScheduledTimestamp());
    assertEquals(original.getAttempt(), result.getAttempt());
    assertEquals(original.getBackoff(), result.getBackoff());
  }

  @Test
  public void testLocalActivityMarkerMetadataWithoutBackoff() {
    LocalActivityMarkerMetadata original = new LocalActivityMarkerMetadata(1, 999L);
    // backoff is null

    Payload payload = CorePayloadConverter.toPayload(original);
    String json = payload.getData().toStringUtf8();

    // Should not contain backoff field when null
    assertFalse(json.contains("backoff"));

    LocalActivityMarkerMetadata result =
        CorePayloadConverter.fromPayload(payload, LocalActivityMarkerMetadata.class);
    assertEquals(original.getOriginalScheduledTimestamp(), result.getOriginalScheduledTimestamp());
    assertEquals(original.getAttempt(), result.getAttempt());
    assertNull(result.getBackoff());
  }

  @Test
  public void testToPayloads() {
    Optional<Payloads> payloads = CorePayloadConverter.toPayloads("test");
    assertTrue(payloads.isPresent());
    assertEquals(1, payloads.get().getPayloadsCount());
    assertEquals("\"test\"", payloads.get().getPayloads(0).getData().toStringUtf8());
  }

  @Test
  public void testFromPayloadsAtIndex() {
    Payloads payloads =
        Payloads.newBuilder()
            .addPayloads(CorePayloadConverter.toPayload("first"))
            .addPayloads(CorePayloadConverter.toPayload("second"))
            .build();

    assertEquals(
        "first", CorePayloadConverter.fromPayloads(0, Optional.of(payloads), String.class));
    assertEquals(
        "second", CorePayloadConverter.fromPayloads(1, Optional.of(payloads), String.class));
  }

  @Test
  public void testFromPayloadsOutOfBounds() {
    Payloads payloads =
        Payloads.newBuilder().addPayloads(CorePayloadConverter.toPayload("only")).build();

    assertNull(CorePayloadConverter.fromPayloads(1, Optional.of(payloads), String.class));
    assertNull(CorePayloadConverter.fromPayloads(5, Optional.of(payloads), String.class));
  }

  @Test
  public void testFromPayloadsEmpty() {
    assertNull(CorePayloadConverter.fromPayloads(0, Optional.empty(), String.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedTypeThrows() {
    CorePayloadConverter.toPayload(new Object());
  }

  @Test
  public void testParseMetadataWithUnknownFields() {
    // Simulate JSON with unknown fields (forward compatibility)
    String json = "{\"firstSkd\":100,\"atpt\":2,\"unknownField\":\"ignored\",\"backoff\":3000}";
    Payload payload =
        Payload.newBuilder()
            .putMetadata("encoding", com.google.protobuf.ByteString.copyFromUtf8("json/plain"))
            .setData(com.google.protobuf.ByteString.copyFromUtf8(json))
            .build();

    LocalActivityMarkerMetadata result =
        CorePayloadConverter.fromPayload(payload, LocalActivityMarkerMetadata.class);
    assertEquals(100L, result.getOriginalScheduledTimestamp());
    assertEquals(2, result.getAttempt());
    assertEquals(Duration.ofMillis(3000), result.getBackoff());
  }
}
