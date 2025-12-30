package io.temporal.worker;

import static org.junit.Assert.*;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.internal.worker.WorkflowImplementationFactory;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for the WorkflowImplementationFactory registry in Worker.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Custom factories can be registered with the worker
 *   <li>Multiple factories can be registered (ordering preserved)
 *   <li>Null factories are rejected
 *   <li>Registration after worker start is rejected
 * </ul>
 */
public class WorkflowImplementationFactoryRegistryTest {

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder().setDoNotStart(true).build();

  @Test
  public void testRegisterCustomFactory() {
    Worker worker = testWorkflowRule.getWorker();
    TestWorkflowImplementationFactory factory = new TestWorkflowImplementationFactory("TestType");

    worker.registerWorkflowImplementationFactory(factory);

    List<WorkflowImplementationFactory> factories = worker.getWorkflowImplementationFactories();
    assertEquals(1, factories.size());
    assertSame(factory, factories.get(0));
  }

  @Test
  public void testRegisterMultipleFactories() {
    Worker worker = testWorkflowRule.getWorker();
    TestWorkflowImplementationFactory factory1 = new TestWorkflowImplementationFactory("Type1");
    TestWorkflowImplementationFactory factory2 = new TestWorkflowImplementationFactory("Type2");
    TestWorkflowImplementationFactory factory3 = new TestWorkflowImplementationFactory("Type3");

    worker.registerWorkflowImplementationFactory(factory1);
    worker.registerWorkflowImplementationFactory(factory2);
    worker.registerWorkflowImplementationFactory(factory3);

    List<WorkflowImplementationFactory> factories = worker.getWorkflowImplementationFactories();
    assertEquals(3, factories.size());
    // Verify registration order is preserved
    assertSame(factory1, factories.get(0));
    assertSame(factory2, factories.get(1));
    assertSame(factory3, factories.get(2));
  }

  @Test
  public void testFactoryListIsUnmodifiable() {
    Worker worker = testWorkflowRule.getWorker();
    TestWorkflowImplementationFactory factory = new TestWorkflowImplementationFactory("TestType");
    worker.registerWorkflowImplementationFactory(factory);

    List<WorkflowImplementationFactory> factories = worker.getWorkflowImplementationFactories();

    assertThrows(
        UnsupportedOperationException.class,
        () -> factories.add(new TestWorkflowImplementationFactory("Another")));
  }

  @Test
  public void testNullFactoryRejected() {
    Worker worker = testWorkflowRule.getWorker();

    assertThrows(
        NullPointerException.class, () -> worker.registerWorkflowImplementationFactory(null));
  }

  @Test
  public void testRegistrationAfterStartRejected() {
    Worker worker = testWorkflowRule.getWorker();
    worker.start();

    TestWorkflowImplementationFactory factory = new TestWorkflowImplementationFactory("TestType");

    assertThrows(
        IllegalStateException.class, () -> worker.registerWorkflowImplementationFactory(factory));
  }

  @Test
  public void testEmptyFactoryListByDefault() {
    Worker worker = testWorkflowRule.getWorker();

    List<WorkflowImplementationFactory> factories = worker.getWorkflowImplementationFactories();

    assertNotNull(factories);
    assertTrue(factories.isEmpty());
  }

  /** A simple test implementation of WorkflowImplementationFactory for testing purposes. */
  private static class TestWorkflowImplementationFactory implements WorkflowImplementationFactory {
    private final Set<String> registeredTypes;

    TestWorkflowImplementationFactory(String... types) {
      this.registeredTypes = new HashSet<>();
      Collections.addAll(this.registeredTypes, types);
    }

    @Nullable
    @Override
    public ReplayWorkflow getWorkflow(
        @Nonnull WorkflowType workflowType, @Nonnull WorkflowExecution workflowExecution) {
      // Return null to indicate this factory doesn't handle the type
      // (actual implementation would return a real workflow)
      return null;
    }

    @Nonnull
    @Override
    public Set<String> getRegisteredWorkflowTypes() {
      return Collections.unmodifiableSet(registeredTypes);
    }
  }
}
