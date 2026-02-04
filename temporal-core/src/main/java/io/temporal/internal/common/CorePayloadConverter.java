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

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.internal.history.LocalActivityMarkerMetadata;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A minimal payload converter for internal SDK use only. Supports only the types needed for marker
 * data serialization: String, Integer, Long, Boolean, and LocalActivityMarkerMetadata.
 *
 * <p>This converter produces JSON payloads compatible with the standard SDK DataConverter, ensuring
 * backwards compatibility with existing workflow histories.
 */
public final class CorePayloadConverter {

  private static final String ENCODING_KEY = "encoding";
  private static final ByteString ENCODING_JSON = ByteString.copyFromUtf8("json/plain");
  private static final ByteString ENCODING_NULL = ByteString.copyFromUtf8("binary/null");

  private CorePayloadConverter() {}

  /**
   * Converts a value to Payloads (single element).
   *
   * @param value the value to convert (String, Integer, Long, Boolean, or
   *     LocalActivityMarkerMetadata)
   * @return Payloads containing the serialized value
   */
  public static Optional<Payloads> toPayloads(Object value) {
    return Optional.of(Payloads.newBuilder().addPayloads(toPayload(value)).build());
  }

  /**
   * Converts a value to a single Payload.
   *
   * @param value the value to convert
   * @return the serialized Payload
   */
  public static Payload toPayload(Object value) {
    if (value == null) {
      return Payload.newBuilder().putMetadata(ENCODING_KEY, ENCODING_NULL).build();
    }

    String json = toJson(value);
    return Payload.newBuilder()
        .putMetadata(ENCODING_KEY, ENCODING_JSON)
        .setData(ByteString.copyFromUtf8(json))
        .build();
  }

  /**
   * Extracts a value from Payloads at the given index.
   *
   * @param index the index in the payloads
   * @param content the Payloads to extract from
   * @param type the expected type
   * @return the deserialized value, or null if not present
   */
  @Nullable
  public static <T> T fromPayloads(int index, Optional<Payloads> content, Class<T> type) {
    if (!content.isPresent() || content.get().getPayloadsCount() <= index) {
      return null;
    }
    return fromPayload(content.get().getPayloads(index), type);
  }

  /**
   * Extracts a value from a single Payload.
   *
   * @param payload the Payload to deserialize
   * @param type the expected type
   * @return the deserialized value
   */
  @Nullable
  public static <T> T fromPayload(Payload payload, Class<T> type) {
    String encoding = payload.getMetadataOrDefault(ENCODING_KEY, ENCODING_NULL).toStringUtf8();

    if ("binary/null".equals(encoding)) {
      return null;
    }

    String json = payload.getData().toStringUtf8();
    return fromJson(json, type);
  }

  private static String toJson(Object value) {
    if (value instanceof String) {
      return escapeJsonString((String) value);
    } else if (value instanceof Integer) {
      return value.toString();
    } else if (value instanceof Long) {
      return value.toString();
    } else if (value instanceof Boolean) {
      return value.toString();
    } else if (value instanceof LocalActivityMarkerMetadata) {
      return serializeMetadata((LocalActivityMarkerMetadata) value);
    }
    throw new IllegalArgumentException(
        "CorePayloadConverter does not support type: " + value.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private static <T> T fromJson(String json, Class<T> type) {
    if (json == null || json.isEmpty()) {
      return null;
    }

    if (type == String.class) {
      return (T) parseJsonString(json);
    } else if (type == Integer.class) {
      return (T) Integer.valueOf(json.trim());
    } else if (type == Long.class) {
      return (T) Long.valueOf(json.trim());
    } else if (type == Boolean.class) {
      return (T) Boolean.valueOf(json.trim());
    } else if (type == LocalActivityMarkerMetadata.class) {
      return (T) parseMetadata(json);
    }
    throw new IllegalArgumentException(
        "CorePayloadConverter does not support type: " + type.getName());
  }

  /** Escapes a string value as a JSON string literal (with surrounding quotes). */
  private static String escapeJsonString(String value) {
    StringBuilder sb = new StringBuilder(value.length() + 2);
    sb.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
    return sb.toString();
  }

  /** Parses a JSON string literal (with surrounding quotes) to a String. */
  private static String parseJsonString(String json) {
    json = json.trim();
    if (json.length() < 2 || json.charAt(0) != '"' || json.charAt(json.length() - 1) != '"') {
      throw new IllegalArgumentException("Invalid JSON string: " + json);
    }

    StringBuilder sb = new StringBuilder(json.length() - 2);
    for (int i = 1; i < json.length() - 1; i++) {
      char c = json.charAt(i);
      if (c == '\\' && i + 1 < json.length() - 1) {
        char next = json.charAt(++i);
        switch (next) {
          case '"':
            sb.append('"');
            break;
          case '\\':
            sb.append('\\');
            break;
          case '/':
            sb.append('/');
            break;
          case 'b':
            sb.append('\b');
            break;
          case 'f':
            sb.append('\f');
            break;
          case 'n':
            sb.append('\n');
            break;
          case 'r':
            sb.append('\r');
            break;
          case 't':
            sb.append('\t');
            break;
          case 'u':
            if (i + 4 < json.length() - 1) {
              String hex = json.substring(i + 1, i + 5);
              sb.append((char) Integer.parseInt(hex, 16));
              i += 4;
            }
            break;
          default:
            sb.append(next);
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Serializes LocalActivityMarkerMetadata to JSON. Format matches Jackson output:
   * {"firstSkd":123,"atpt":1,"backoff":5000}
   */
  private static String serializeMetadata(LocalActivityMarkerMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"firstSkd\":");
    sb.append(metadata.getOriginalScheduledTimestamp());
    sb.append(",\"atpt\":");
    sb.append(metadata.getAttempt());
    Duration backoff = metadata.getBackoff();
    if (backoff != null) {
      sb.append(",\"backoff\":");
      sb.append(backoff.toMillis());
    }
    sb.append("}");
    return sb.toString();
  }

  /** Parses LocalActivityMarkerMetadata from JSON. */
  private static LocalActivityMarkerMetadata parseMetadata(String json) {
    json = json.trim();
    if (!json.startsWith("{") || !json.endsWith("}")) {
      throw new IllegalArgumentException("Invalid JSON object: " + json);
    }

    LocalActivityMarkerMetadata metadata = new LocalActivityMarkerMetadata();
    String content = json.substring(1, json.length() - 1).trim();

    if (content.isEmpty()) {
      return metadata;
    }

    // Simple JSON object parser for known fields
    int pos = 0;
    while (pos < content.length()) {
      // Skip whitespace and commas
      while (pos < content.length()
          && (content.charAt(pos) == ',' || Character.isWhitespace(content.charAt(pos)))) {
        pos++;
      }
      if (pos >= content.length()) break;

      // Parse field name
      if (content.charAt(pos) != '"') {
        throw new IllegalArgumentException("Expected field name at position " + pos);
      }
      int fieldStart = pos + 1;
      int fieldEnd = content.indexOf('"', fieldStart);
      if (fieldEnd < 0) {
        throw new IllegalArgumentException("Unterminated field name");
      }
      String fieldName = content.substring(fieldStart, fieldEnd);
      pos = fieldEnd + 1;

      // Skip colon
      while (pos < content.length() && Character.isWhitespace(content.charAt(pos))) pos++;
      if (pos >= content.length() || content.charAt(pos) != ':') {
        throw new IllegalArgumentException("Expected ':' after field name");
      }
      pos++;
      while (pos < content.length() && Character.isWhitespace(content.charAt(pos))) pos++;

      // Parse value
      int valueStart = pos;
      if (content.charAt(pos) == '"') {
        // String value - skip to end quote
        pos++;
        while (pos < content.length() && content.charAt(pos) != '"') {
          if (content.charAt(pos) == '\\') pos++; // Skip escaped char
          pos++;
        }
        pos++; // Skip closing quote
      } else if (content.charAt(pos) == 'n' && content.substring(pos).startsWith("null")) {
        pos += 4;
      } else {
        // Number or boolean - read until separator
        while (pos < content.length() && content.charAt(pos) != ',' && content.charAt(pos) != '}') {
          pos++;
        }
      }
      String valueStr = content.substring(valueStart, pos).trim();

      // Assign to field
      switch (fieldName) {
        case "firstSkd":
          metadata.setOriginalScheduledTimestamp(Long.parseLong(valueStr));
          break;
        case "atpt":
          metadata.setAttempt(Integer.parseInt(valueStr));
          break;
        case "backoff":
          if (!"null".equals(valueStr)) {
            metadata.setBackoff(Duration.ofMillis(Long.parseLong(valueStr)));
          }
          break;
          // Ignore unknown fields for forward compatibility
      }
    }

    return metadata;
  }
}
