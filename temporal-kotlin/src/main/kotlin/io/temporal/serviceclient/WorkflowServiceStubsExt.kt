
package io.temporal.serviceclient

import io.temporal.kotlin.TemporalDsl
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration
import java.time.Duration as JavaDuration

/**
 * Create WorkflowService gRPC stubs pointed on to the locally running Temporal Server.
 *
 * @see WorkflowServiceStubs.newLocalServiceStubs
 */
fun LocalWorkflowServiceStubs(): WorkflowServiceStubs {
  return WorkflowServiceStubs.newLocalServiceStubs()
}

/**
 * Create WorkflowService gRPC stubs using provided [options].
 *
 * @see WorkflowServiceStubs.newServiceStubs
 */
inline fun LazyWorkflowServiceStubs(
  options: @TemporalDsl WorkflowServiceStubsOptions.Builder.() -> Unit
): WorkflowServiceStubs {
  return WorkflowServiceStubs.newServiceStubs(WorkflowServiceStubsOptions(options))
}

/**
 * Create WorkflowService gRPC stubs using provided [options].
 *
 * @see WorkflowServiceStubs.newConnectedServiceStubs
 */
@ExperimentalTime
inline fun ConnectedWorkflowServiceStubs(
  timeout: Duration,
  options: @TemporalDsl WorkflowServiceStubsOptions.Builder.() -> Unit
): WorkflowServiceStubs {
  return ConnectedWorkflowServiceStubs(timeout.toJavaDuration(), options)
}

/**
 * Create WorkflowService gRPC stubs using provided [options].
 *
 * @see WorkflowServiceStubs.newConnectedServiceStubs
 */
inline fun ConnectedWorkflowServiceStubs(
  timeout: JavaDuration? = null,
  options: @TemporalDsl WorkflowServiceStubsOptions.Builder.() -> Unit
): WorkflowServiceStubs {
  return WorkflowServiceStubs.newConnectedServiceStubs(WorkflowServiceStubsOptions(options), timeout)
}
