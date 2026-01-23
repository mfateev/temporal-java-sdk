package io.temporal.kotlin.client

public data class KOnConflictOptions(
  val attachRequestId: Boolean = false,
  val attachCompletionCallbacks: Boolean = false,
  val attachLinks: Boolean = false
) {
  init {
    if (attachCompletionCallbacks) {
      require(attachRequestId) {
        "attachRequestId must be true if attachCompletionCallbacks is true"
      }
    }
  }
}
