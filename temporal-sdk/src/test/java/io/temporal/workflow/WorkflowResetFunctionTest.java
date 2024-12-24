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

package io.temporal.workflow;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.TerminatedFailure;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.shared.TestWorkflows;
import io.temporal.workflow.shared.TestWorkflows.TestWorkflowStringArg;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;

public class WorkflowResetFunctionTest {

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setUseExternalService(true)
          .setWorkflowTypes(
              WorkflowImplementationOptions.newBuilder()
                  .setFailWorkflowExceptionTypes(Throwable.class)
                  .build(),
              WorkflowToReset.class)
          .setUseTimeskipping(false)
          // Forcing a replay. Full history arrived from a normal queue causing a replay.
          .setWorkerOptions(
              WorkerOptions.newBuilder()
                  .setStickyQueueScheduleToStartTimeout(Duration.ZERO)
                  .build())
          .build();

  private static final CompletableFuture<Void> goingToSleep = new CompletableFuture<>();

  @Test
  public void testWorkflowResetFunction() throws ExecutionException, InterruptedException {
    WorkflowOptions options =
        WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build();
    TestWorkflowStringArg workflowStub =
        testWorkflowRule.getWorkflowClient().newWorkflowStub(TestWorkflowStringArg.class, options);

    // start workflow
    WorkflowExecution we =
        WorkflowClient.start(workflowStub::execute, testWorkflowRule.getTaskQueue());
    goingToSleep.get();
    WorkflowStub untyped = WorkflowStub.fromTyped(workflowStub);
    untyped.signal("unexpectedSignal", "test");
    WorkflowFailedException e =
        assertThrows(WorkflowFailedException.class, () -> untyped.getResult(Void.class));
    assertTrue(e.getCause() instanceof TerminatedFailure);
    WorkflowStub resetWorkflow =
        testWorkflowRule.getWorkflowClient().newUntypedWorkflowStub(we.getWorkflowId());
    resetWorkflow.getResult(Void.class);
  }

  public static class WorkflowToReset implements TestWorkflows.TestWorkflowStringArg {

    @Override
    public void execute(String taskQueue) {
      // Hack to test code changes in the middle of a workflow execution
      if (goingToSleep.isDone()) {
        Workflow.reset("timerUpdate", "test");
        Workflow.sleep(100);
      } else {
        goingToSleep.complete(null);
        Workflow.sleep(Duration.ofHours(100));
      }
    }
  }
}
