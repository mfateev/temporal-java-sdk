
package io.temporal.workflow

import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.common.converter.KotlinObjectMapperFactory
import io.temporal.internal.async.FunctionWrappingUtil
import io.temporal.internal.sync.AsyncInternal
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import org.junit.Assert
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean

class KotlinAsyncLambdaTest {

  companion object {
    private val success = AtomicBoolean(false)
  }

  @Rule
  @JvmField
  var testWorkflowRule = KSDKTestWorkflowRule {
    setWorkflowTypes(LambdaWorkflowImpl::class)
    setWorkflowClientOptions(
      WorkflowClientOptions.newBuilder()
        .setDataConverter(DefaultDataConverter(JacksonJsonPayloadConverter(KotlinObjectMapperFactory.new())))
        .build()
    )
  }

  @WorkflowInterface
  interface Workflow {
    @WorkflowMethod
    fun execute()
  }

  class LambdaWorkflowImpl : Workflow {
    override fun execute() {
      val lambda = { success.set(true) }
      Assert.assertFalse(
        "This is just a regular lambda function, it's shouldn't be recognized as a method reference to a" +
          " Temporal async stub",
        AsyncInternal.isAsync(lambda)
      )

      Assert.assertFalse(
        "This is just a regular lambda function, it's shouldn't be recognized as a method reference to a" +
          " Temporal async stub",
        AsyncInternal.isAsync(FunctionWrappingUtil.temporalJavaFunctionalWrapper(lambda))
      )

      Async.procedure(lambda).get()
    }
  }

  @Test
  fun asyncLambdaTest() {
    val client = testWorkflowRule.workflowClient
    val options = WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.taskQueue).build()
    val workflowStub = client.newWorkflowStub(Workflow::class.java, options)
    workflowStub.execute()
    assertTrue(success.get())
  }
}
