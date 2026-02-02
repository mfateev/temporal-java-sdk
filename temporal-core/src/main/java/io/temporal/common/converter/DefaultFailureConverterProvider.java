package io.temporal.common.converter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Provider for the default FailureConverter implementation. This allows temporal-sdk to register
 * the actual DefaultFailureConverter implementation that has SDK-specific dependencies.
 */
public final class DefaultFailureConverterProvider {
  private DefaultFailureConverterProvider() {}

  private static final String DEFAULT_FAILURE_CONVERTER_CLASS =
      "io.temporal.failure.DefaultFailureConverter";

  private static final AtomicReference<Supplier<FailureConverter>> provider =
      new AtomicReference<>(null);

  /** Register the default FailureConverter supplier. Called by temporal-sdk at initialization. */
  public static void register(Supplier<FailureConverter> failureConverterSupplier) {
    provider.set(failureConverterSupplier);
  }

  /** Get a new instance of the default FailureConverter. */
  public static FailureConverter get() {
    Supplier<FailureConverter> supplier = provider.get();
    if (supplier != null) {
      return supplier.get();
    }

    // Try to load and initialize DefaultFailureConverter from temporal-sdk
    // This will trigger its static initializer which registers the provider
    try {
      Class.forName(DEFAULT_FAILURE_CONVERTER_CLASS);
      supplier = provider.get();
      if (supplier != null) {
        return supplier.get();
      }
    } catch (ClassNotFoundException e) {
      // temporal-sdk not on classpath
    }

    throw new IllegalStateException(
        "Default FailureConverter not available. Ensure temporal-sdk is on the classpath.");
  }
}
