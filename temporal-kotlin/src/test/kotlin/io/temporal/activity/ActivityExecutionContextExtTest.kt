
package io.temporal.activity

import io.temporal.client.newWorkflowStub
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.common.converter.KotlinObjectMapperFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Rule
import org.junit.Test
import java.time.Duration

class ActivityExecutionContextExtTest {

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    workflowTypes(TestWorkflowImpl::class)
    activityImplementations(TestActivityForHeartbeatDetails())
    workflowClientOptions {
      setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
    }
  }

  @Test
  fun `getHeartbeatDetailsOrNull should correctly deserialize generic activity heartbeat details`() {
    val workflowStub = testWorkflowRule.workflowClient.newWorkflowStub<TestWorkflow> {
      setTaskQueue(testWorkflowRule.taskQueue)
    }
    workflowStub.run()
  }

  @WorkflowInterface
  interface TestWorkflow {
    @WorkflowMethod
    fun run()
  }

  class TestWorkflowImpl : TestWorkflow {
    override fun run() {
      val activityOptions = ActivityOptions {
        setStartToCloseTimeout(Duration.ofMinutes(1))
        setRetryOptions {
          setMaximumAttempts(2)
        }
      }
      val activity = Workflow.newActivityStub(TestActivity::class.java, activityOptions)
      activity.run()
    }
  }

  @ActivityInterface
  interface TestActivity {
    fun run()
  }

  class TestActivityForHeartbeatDetails : TestActivity {

    override fun run() {
      val context = Activity.getExecutionContext()
      val heartbeatDetails = context.getHeartbeatDetailsOrNull<HeartbeatDetails<List<Long>>>()
      if (context.info.attempt == 1) {
        assertNull(heartbeatDetails)
        context.heartbeat(HEARTBEAT_DETAILS)
        throw RuntimeException()
      } else {
        assertEquals(HEARTBEAT_DETAILS, heartbeatDetails)
      }
    }

    companion object {
      private val HEARTBEAT_DETAILS = HeartbeatDetails<List<Long>>("test", listOf(1, 2))
    }
  }

  private data class HeartbeatDetails<T>(
    val id: String,
    val value: T
  )
}
