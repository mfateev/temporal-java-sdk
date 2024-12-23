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

import io.temporal.activity.ActivityInterface;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.internal.SDKTestOptions;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

public class BundleTest {

  private static final Logger log = Workflow.getLogger(BundleTest.class);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestBundleWorkflowImpl.class)
          .setActivityImplementations(new TestActivitiesImpl())
          .setUseExternalService(true)
          .setNamespace("default")
          .build();

  @Test
  public void testBundleWorkflow() {
    WorkflowStub sagaWorkflow = testWorkflowRule.newUntypedWorkflowStub("TestBundleWorkflow");
    sagaWorkflow.startBundle(1, 2, 3, 4, 5, 6, 7, 9, 10);
    Integer result = sagaWorkflow.getResult(Integer.class, Integer.class);
    //    Assert.assertEquals(Integer.valueOf(11), result);
    //    String trace = testWorkflowRule.getInterceptor(TracingWorkerInterceptor.class).getTrace();
    //    Assert.assertTrue(trace, trace.contains("executeChildWorkflow TestCompensationWorkflow"));
    //    Assert.assertTrue(trace, trace.contains("executeActivity Activity2"));
  }

  @ActivityInterface
  public interface TestActivities {
    int activity1(int input);

    int activity2(int input);

    int activity3(int input);
  }

  public class TestActivitiesImpl implements TestActivities {
    @Override
    public int activity1(int input) {
      return input;
    }

    @Override
    public int activity2(int input) {
      return input + 100;
    }

    @Override
    public int activity3(int input) {
      return input + 1000;
    }
  }

  @WorkflowInterface
  public interface TestBundleWorkflow {
    @WorkflowMethod
    int execute(int arg);
  }

  public static class TestBundleWorkflowImpl implements TestBundleWorkflow {

    @Override
    public int execute(int arg) {
      String taskQueue = Workflow.getInfo().getTaskQueue();
      TestActivities testActivities =
          Workflow.newActivityStub(
              TestActivities.class, SDKTestOptions.newActivityOptionsForTaskQueue(taskQueue));

      int result = testActivities.activity1(arg);
      if (result > 3) {
        result = testActivities.activity3(result);
      } else {
        result = testActivities.activity2(result);
      }
      log.info("activity1 completed, result: {}", result);
      return result;
    }
  }
}
