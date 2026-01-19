package io.temporal.internal.docker;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.api.operatorservice.v1.AddSearchAttributesRequest;
import io.temporal.serviceclient.OperatorServiceStubs;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to register default test search attributes on an external Temporal service.
 *
 * <p>These search attributes are commonly used in SDK tests and match the ones created by default
 * in Temporal's Docker Compose setup.
 */
public class RegisterTestSearchAttributes {
  private static final Logger log = LoggerFactory.getLogger(RegisterTestSearchAttributes.class);

  /** Default test search attributes that are commonly used in SDK tests. */
  private static final Map<String, IndexedValueType> DEFAULT_TEST_SEARCH_ATTRIBUTES;

  static {
    DEFAULT_TEST_SEARCH_ATTRIBUTES = new HashMap<>();
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put(
        "CustomKeywordField", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put("CustomTextField", IndexedValueType.INDEXED_VALUE_TYPE_TEXT);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put(
        "CustomStringField", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put("CustomIntField", IndexedValueType.INDEXED_VALUE_TYPE_INT);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put(
        "CustomDoubleField", IndexedValueType.INDEXED_VALUE_TYPE_DOUBLE);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put("CustomBoolField", IndexedValueType.INDEXED_VALUE_TYPE_BOOL);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put(
        "CustomDatetimeField", IndexedValueType.INDEXED_VALUE_TYPE_DATETIME);
    DEFAULT_TEST_SEARCH_ATTRIBUTES.put(
        "MyKeywordListField", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD_LIST);
  }

  /**
   * Registers the default test search attributes on the given namespace.
   *
   * <p>This method is idempotent - if a search attribute already exists, it will be silently
   * ignored.
   *
   * @param operatorServiceStubs the operator service stubs to use for registration
   * @param namespace the namespace to register search attributes on
   */
  public static void registerDefaultSearchAttributes(
      OperatorServiceStubs operatorServiceStubs, String namespace) {
    registerSearchAttributes(operatorServiceStubs, namespace, DEFAULT_TEST_SEARCH_ATTRIBUTES);
  }

  /**
   * Registers the specified search attributes on the given namespace.
   *
   * <p>This method is idempotent - if a search attribute already exists, it will be silently
   * ignored.
   *
   * @param operatorServiceStubs the operator service stubs to use for registration
   * @param namespace the namespace to register search attributes on
   * @param searchAttributes map of search attribute names to their types
   */
  public static void registerSearchAttributes(
      OperatorServiceStubs operatorServiceStubs,
      String namespace,
      Map<String, IndexedValueType> searchAttributes) {
    if (searchAttributes == null || searchAttributes.isEmpty()) {
      return;
    }

    log.info(
        "Registering {} test search attributes on namespace '{}'",
        searchAttributes.size(),
        namespace);
    // Register each search attribute individually to handle partial failures
    for (Map.Entry<String, IndexedValueType> entry : searchAttributes.entrySet()) {
      registerSearchAttribute(operatorServiceStubs, namespace, entry.getKey(), entry.getValue());
    }
  }

  private static void registerSearchAttribute(
      OperatorServiceStubs operatorServiceStubs,
      String namespace,
      String name,
      IndexedValueType type) {
    try {
      AddSearchAttributesRequest request =
          AddSearchAttributesRequest.newBuilder()
              .setNamespace(namespace)
              .putSearchAttributes(name, type)
              .build();

      operatorServiceStubs.blockingStub().addSearchAttributes(request);
      log.debug(
          "Registered search attribute '{}' of type {} on namespace '{}'", name, type, namespace);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        log.debug("Search attribute '{}' already exists on namespace '{}'", name, namespace);
      } else {
        log.warn(
            "Failed to register search attribute '{}' on namespace '{}': {}",
            name,
            namespace,
            e.getMessage());
      }
    }
  }
}
