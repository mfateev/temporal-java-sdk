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

package io.temporal.internal.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.internal.replay.ReplayWorkflowFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.junit.Test;

/**
 * Tests for {@link CompositeReplayWorkflowFactory}.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Delegation to custom factories in registration order
 *   <li>Fallback to default factory when no custom factory handles the type
 *   <li>Aggregation of workflow types from all factories
 *   <li>isAnyTypeSupported behavior across multiple factories
 * </ul>
 */
public class CompositeReplayWorkflowFactoryTest {

  private static final String WORKFLOW_ID = "test-workflow-id";
  private static final String RUN_ID = "test-run-id";

  @Test
  public void testDelegateToFirstFactory() throws Exception {
    // Setup: First factory handles "Type1"
    ReplayWorkflow mockWorkflow = mock(ReplayWorkflow.class);
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(mockWorkflow, "Type1");
    TestWorkflowImplementationFactory factory2 =
        new TestWorkflowImplementationFactory(null, "Type2");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);
    factories.add(factory2);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("Type1").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify
    assertSame(mockWorkflow, result);
    verifyNoInteractions(defaultFactory);
  }

  @Test
  public void testDelegateToSecondFactory() throws Exception {
    // Setup: Second factory handles "Type2"
    ReplayWorkflow mockWorkflow = mock(ReplayWorkflow.class);
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(null, "Type1");
    TestWorkflowImplementationFactory factory2 =
        new TestWorkflowImplementationFactory(mockWorkflow, "Type2");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);
    factories.add(factory2);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("Type2").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify
    assertSame(mockWorkflow, result);
    verifyNoInteractions(defaultFactory);
  }

  @Test
  public void testFallbackToDefaultFactory() throws Exception {
    // Setup: No custom factory handles "UnknownType"
    ReplayWorkflow defaultWorkflow = mock(ReplayWorkflow.class);
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(null, "Type1");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);
    when(defaultFactory.getWorkflow(any(), any())).thenReturn(defaultWorkflow);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("UnknownType").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify
    assertSame(defaultWorkflow, result);
    verify(defaultFactory).getWorkflow(workflowType, execution);
  }

  @Test
  public void testEmptyFactoriesFallsBackToDefault() throws Exception {
    // Setup: No custom factories
    ReplayWorkflow defaultWorkflow = mock(ReplayWorkflow.class);
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);
    when(defaultFactory.getWorkflow(any(), any())).thenReturn(defaultWorkflow);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("AnyType").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify
    assertSame(defaultWorkflow, result);
    verify(defaultFactory).getWorkflow(workflowType, execution);
  }

  @Test
  public void testIsAnyTypeSupportedWithCustomFactories() {
    // Setup: Custom factory has types
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(null, "Type1");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);
    when(defaultFactory.isAnyTypeSupported()).thenReturn(false);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Verify
    assertTrue(composite.isAnyTypeSupported());
  }

  @Test
  public void testIsAnyTypeSupportedWithOnlyDefaultFactory() {
    // Setup: No custom factories, default has types
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);
    when(defaultFactory.isAnyTypeSupported()).thenReturn(true);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Verify
    assertTrue(composite.isAnyTypeSupported());
  }

  @Test
  public void testIsAnyTypeSupportedReturnsFalseWhenNoTypes() {
    // Setup: No factories have types
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);
    when(defaultFactory.isAnyTypeSupported()).thenReturn(false);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Verify
    assertFalse(composite.isAnyTypeSupported());
  }

  @Test
  public void testGetRegisteredWorkflowTypesAggregatesAllFactories() {
    // Setup
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(null, "Type1", "Type2");
    TestWorkflowImplementationFactory factory2 =
        new TestWorkflowImplementationFactory(null, "Type3");
    TestWorkflowImplementationFactory defaultFactory =
        new TestWorkflowImplementationFactory(null, "Type4", "Type5");

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);
    factories.add(factory2);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    Set<String> registeredTypes = composite.getRegisteredWorkflowTypes();

    // Verify
    assertEquals(5, registeredTypes.size());
    assertTrue(registeredTypes.contains("Type1"));
    assertTrue(registeredTypes.contains("Type2"));
    assertTrue(registeredTypes.contains("Type3"));
    assertTrue(registeredTypes.contains("Type4"));
    assertTrue(registeredTypes.contains("Type5"));
  }

  @Test
  public void testRegistrationOrderIsPreserved() throws Exception {
    // Setup: Both factories can handle "SharedType", first should win
    ReplayWorkflow workflow1 = mock(ReplayWorkflow.class);
    ReplayWorkflow workflow2 = mock(ReplayWorkflow.class);

    WorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(workflow1, "SharedType");
    WorkflowImplementationFactory factory2 =
        new TestWorkflowImplementationFactory(workflow2, "SharedType");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);
    factories.add(factory2);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("SharedType").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify: First factory wins
    assertSame(workflow1, result);
  }

  @Test
  public void testLateRegistrationOfFactories() throws Exception {
    // Setup: List is populated after CompositeReplayWorkflowFactory is created
    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    // Add factory after construction (simulating Worker.registerWorkflowImplementationFactory)
    ReplayWorkflow mockWorkflow = mock(ReplayWorkflow.class);
    factories.add(new TestWorkflowImplementationFactory(mockWorkflow, "LateType"));

    // Execute
    WorkflowType workflowType = WorkflowType.newBuilder().setName("LateType").build();
    WorkflowExecution execution = createExecution();
    ReplayWorkflow result = composite.getWorkflow(workflowType, execution);

    // Verify: Late-registered factory is consulted
    assertSame(mockWorkflow, result);
    verifyNoInteractions(defaultFactory);
  }

  @Test
  public void testToStringIncludesFactoryInfo() {
    TestWorkflowImplementationFactory factory1 =
        new TestWorkflowImplementationFactory(null, "Type1");
    ReplayWorkflowFactory defaultFactory = mock(ReplayWorkflowFactory.class);

    List<WorkflowImplementationFactory> factories = new ArrayList<>();
    factories.add(factory1);

    CompositeReplayWorkflowFactory composite =
        new CompositeReplayWorkflowFactory(factories, defaultFactory);

    String str = composite.toString();
    assertTrue(str.contains("CompositeReplayWorkflowFactory"));
    assertTrue(str.contains("customFactories=1"));
  }

  private WorkflowExecution createExecution() {
    return WorkflowExecution.newBuilder().setWorkflowId(WORKFLOW_ID).setRunId(RUN_ID).build();
  }

  /** Test implementation of WorkflowImplementationFactory for unit testing. */
  private static class TestWorkflowImplementationFactory implements WorkflowImplementationFactory {
    private final ReplayWorkflow workflowToReturn;
    private final Set<String> registeredTypes;

    TestWorkflowImplementationFactory(ReplayWorkflow workflowToReturn, String... types) {
      this.workflowToReturn = workflowToReturn;
      this.registeredTypes = new HashSet<>();
      Collections.addAll(this.registeredTypes, types);
    }

    @Nullable
    @Override
    public ReplayWorkflow getWorkflow(
        @Nonnull WorkflowType workflowType, @Nonnull WorkflowExecution workflowExecution) {
      // Return the workflow only if this factory handles the type
      if (registeredTypes.contains(workflowType.getName())) {
        return workflowToReturn;
      }
      return null;
    }

    @Nonnull
    @Override
    public Set<String> getRegisteredWorkflowTypes() {
      return Collections.unmodifiableSet(registeredTypes);
    }
  }
}
