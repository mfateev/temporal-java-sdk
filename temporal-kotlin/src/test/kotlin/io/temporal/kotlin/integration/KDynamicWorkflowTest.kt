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

@file:OptIn(kotlin.time.ExperimentalTime::class, io.temporal.kotlin.internal.InternalTemporalApi::class)

package io.temporal.kotlin.integration

import io.temporal.activity.Activity
import io.temporal.client.WorkflowOptions
import io.temporal.common.converter.DataConverter
import io.temporal.kotlin.activity.KActivityOptions
import io.temporal.kotlin.activity.KDynamicActivity
import io.temporal.kotlin.common.KEncodedValues
import io.temporal.kotlin.internal.KDynamicActivityWrapper
import io.temporal.kotlin.internal.KotlinWorkflowImplementationFactory
import io.temporal.kotlin.testing.internal.KSDKTestWorkflowRule
import io.temporal.kotlin.workflow.KDynamicWorkflow
import io.temporal.kotlin.workflow.KWorkflow
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Integration tests for [KDynamicWorkflow] with Kotlin coroutine support.
 */
class KDynamicWorkflowTest {

  /**
   * Simple dynamic workflow that returns a formatted string.
   */
  class SimpleDynamicWorkflow : KDynamicWorkflow {
    override suspend fun execute(args: KEncodedValues): Any? {
      val input = args.get<String>(0)
      val workflowType = KWorkflow.info.workflowType
      return "$workflowType received: $input"
    }
  }

  @Rule
  @JvmField
  var simpleTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testSimpleDynamicWorkflow() {
    // Register dynamic workflow using KotlinWorkflowImplementationFactory
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(SimpleDynamicWorkflow::class)
    simpleTestRule.worker.registerWorkflowImplementationFactory(factory)
    simpleTestRule.testEnvironment.start()

    // Use untyped stub to start the workflow
    val stub = simpleTestRule.workflowClient.newUntypedWorkflowStub(
      "AnyWorkflowType",
      WorkflowOptions.newBuilder()
        .setTaskQueue(simpleTestRule.taskQueue)
        .build()
    )
    stub.start("test input")
    val result = stub.getResult(String::class.java)

    assertEquals("AnyWorkflowType received: test input", result)
  }

  /**
   * Dynamic workflow that uses a timer (delay).
   */
  class TimerDynamicWorkflow : KDynamicWorkflow {
    override suspend fun execute(args: KEncodedValues): Any? {
      val input = args.get<String>(0)

      // Use a very short delay to test timer integration
      delay(100)

      return "Processed: $input"
    }
  }

  @Rule
  @JvmField
  var timerTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testDynamicWorkflowWithTimer() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(TimerDynamicWorkflow::class)
    timerTestRule.worker.registerWorkflowImplementationFactory(factory)
    timerTestRule.testEnvironment.start()

    val stub = timerTestRule.workflowClient.newUntypedWorkflowStub(
      "TimerWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(timerTestRule.taskQueue)
        .build()
    )
    stub.start("timer test")
    val result = stub.getResult(String::class.java)

    assertEquals("Processed: timer test", result)
  }

  /**
   * Dynamic workflow that uses signals and activities.
   */
  class SignalDynamicWorkflow : KDynamicWorkflow {
    private var signalValue: String = ""

    override suspend fun execute(args: KEncodedValues): Any? {
      val greeting = args.get<String>(0)
      val workflowType = KWorkflow.info.workflowType

      // Register dynamic signal handler
      KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
        if (signalName == "testSignal") {
          signalValue = signalArgs.get<String>(0)
        }
      }

      // Wait for signal using coroutine
      KWorkflow.awaitCondition { signalValue.isNotEmpty() }

      // Execute activity using KWorkflow API
      val activityResult = KWorkflow.executeActivity<String>(
        "testActivity",
        listOf(greeting, signalValue),
        KActivityOptions(startToCloseTimeout = 10.seconds)
      )

      return "workflowType=$workflowType, activity=$activityResult"
    }
  }

  /**
   * Dynamic activity for testing.
   */
  class TestDynamicActivity : KDynamicActivity {
    override fun execute(args: KEncodedValues): Any? {
      val activityType = Activity.getExecutionContext().info.activityType
      val greeting = args.get<String>(0)
      val name = args.get<String>(1)
      return "$activityType: $greeting $name"
    }
  }

  @Rule
  @JvmField
  var signalTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testDynamicWorkflowWithSignalAndActivity() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(SignalDynamicWorkflow::class)
    signalTestRule.worker.registerWorkflowImplementationFactory(factory)
    signalTestRule.worker.registerActivitiesImplementations(KDynamicActivityWrapper(TestDynamicActivity()))
    signalTestRule.testEnvironment.start()

    val stub = signalTestRule.workflowClient.newUntypedWorkflowStub(
      "MyDynamicType",
      WorkflowOptions.newBuilder()
        .setTaskQueue(signalTestRule.taskQueue)
        .build()
    )

    // Start the workflow and send signal
    stub.signalWithStart("testSignal", arrayOf("World"), arrayOf("Hello"))
    val result = stub.getResult(String::class.java)

    assertEquals("workflowType=MyDynamicType, activity=testActivity: Hello World", result)
  }

  // ==================== Signal Buffering Tests ====================

  /**
   * Dynamic workflow that tests multiple signals being buffered via signalWithStart.
   * The first signal is sent atomically with start, guaranteeing it arrives before
   * the workflow has a chance to register its handler.
   */
  class MultipleSignalBufferingWorkflow : KDynamicWorkflow {
    private val receivedSignals = mutableListOf<String>()

    override suspend fun execute(args: KEncodedValues): Any? {
      val expectedCount = args.get<Int>(0)

      // Register dynamic signal handler - signals received before this should be buffered
      KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
        val value = signalArgs.get<String>(0)
        receivedSignals.add("$signalName:$value")
      }

      // Wait until we have all expected signals
      KWorkflow.awaitCondition { receivedSignals.size >= expectedCount }

      // Return the count of signals received (order is not guaranteed for signals after the first)
      return "count=${receivedSignals.size}, first=${receivedSignals.firstOrNull()}"
    }
  }

  @Rule
  @JvmField
  var multipleSignalTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testSignalBufferedViaSignalWithStart() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(MultipleSignalBufferingWorkflow::class)
    multipleSignalTestRule.worker.registerWorkflowImplementationFactory(factory)
    multipleSignalTestRule.testEnvironment.start()

    val stub = multipleSignalTestRule.workflowClient.newUntypedWorkflowStub(
      "MultiSignalWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(multipleSignalTestRule.taskQueue)
        .build()
    )

    // Start workflow expecting 1 signal, sent atomically via signalWithStart
    // This signal MUST be buffered since it arrives before workflow code runs
    stub.signalWithStart("bufferedSignal", arrayOf("buffered-value"), arrayOf(1))

    val result = stub.getResult(String::class.java)

    // Verify the signal was received (it was buffered and replayed)
    assertEquals("count=1, first=bufferedSignal:buffered-value", result)
  }

  /**
   * Dynamic workflow that tests specific handler taking precedence over buffering.
   * A specific handler registered BEFORE a signal arrives should handle the signal
   * immediately, not buffer it.
   */
  class SpecificHandlerPrecedenceWorkflow : KDynamicWorkflow {
    private var specificSignalValue: String = ""
    private val dynamicSignals = mutableListOf<String>()

    override suspend fun execute(args: KEncodedValues): Any? {
      // Register a SPECIFIC signal handler BEFORE any signals arrive
      // This handler should take precedence - signals to "specificSignal" should NOT be buffered
      KWorkflow.registerSignalHandler("specificSignal") { signalArgs ->
        specificSignalValue = signalArgs.get<String>(0)
      }

      // Now register dynamic handler for any other signals
      KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
        val value = signalArgs.get<String>(0)
        dynamicSignals.add("$signalName:$value")
      }

      // Wait for specific signal
      KWorkflow.awaitCondition { specificSignalValue.isNotEmpty() }

      return "specific=$specificSignalValue"
    }
  }

  @Rule
  @JvmField
  var specificHandlerTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testSpecificHandlerTakesPrecedence() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(SpecificHandlerPrecedenceWorkflow::class)
    specificHandlerTestRule.worker.registerWorkflowImplementationFactory(factory)
    specificHandlerTestRule.testEnvironment.start()

    val stub = specificHandlerTestRule.workflowClient.newUntypedWorkflowStub(
      "SpecificHandlerWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(specificHandlerTestRule.taskQueue)
        .build()
    )

    // Start workflow normally
    stub.start()

    // Send signal that has a specific handler
    stub.signal("specificSignal", "specific-value")

    val result = stub.getResult(String::class.java)

    // Verify specific signal was handled by specific handler
    assertEquals("specific=specific-value", result)
  }

  /**
   * Dynamic workflow that verifies signals sent AFTER dynamic handler registration
   * are handled directly (not going through buffer).
   */
  class SignalAfterRegistrationWorkflow : KDynamicWorkflow {
    private val receivedSignals = mutableListOf<String>()

    override suspend fun execute(args: KEncodedValues): Any? {
      // Register dynamic handler immediately
      KWorkflow.registerDynamicSignalHandler { signalName, signalArgs ->
        val value = signalArgs.get<String>(0)
        receivedSignals.add("$signalName:$value")
      }

      // Wait for signal
      KWorkflow.awaitCondition { receivedSignals.isNotEmpty() }

      return "received=${receivedSignals.first()}"
    }
  }

  @Rule
  @JvmField
  var afterRegistrationTestRule = KSDKTestWorkflowRule {
    doNotStart = true
  }

  @Test
  fun testSignalAfterHandlerRegistration() {
    val factory = KotlinWorkflowImplementationFactory(DataConverter.getDefaultInstance())
    factory.registerDynamicWorkflow(SignalAfterRegistrationWorkflow::class)
    afterRegistrationTestRule.worker.registerWorkflowImplementationFactory(factory)
    afterRegistrationTestRule.testEnvironment.start()

    val stub = afterRegistrationTestRule.workflowClient.newUntypedWorkflowStub(
      "SignalAfterRegWorkflow",
      WorkflowOptions.newBuilder()
        .setTaskQueue(afterRegistrationTestRule.taskQueue)
        .build()
    )

    // Start workflow normally (no signal with start)
    stub.start()

    // Send signal after workflow has started
    stub.signal("testSignal", "test-value")

    val result = stub.getResult(String::class.java)

    // Verify signal was received
    assertEquals("received=testSignal:test-value", result)
  }
}
