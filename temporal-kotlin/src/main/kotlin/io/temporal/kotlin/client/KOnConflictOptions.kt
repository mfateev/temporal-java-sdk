package io.temporal.kotlin.client


import io.temporal.client.OnConflictOptions

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

  public fun toJavaOptions(): OnConflictOptions = OnConflictOptions.newBuilder()
    .setAttachRequestId(attachRequestId)
    .setAttachCompletionCallbacks(attachCompletionCallbacks)
    .setAttachLinks(attachLinks)
    .build()
}
