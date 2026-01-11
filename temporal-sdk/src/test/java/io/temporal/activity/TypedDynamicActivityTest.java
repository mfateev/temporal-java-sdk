package io.temporal.activity;

import static org.junit.Assert.assertEquals;

import io.temporal.common.converter.EncodedValues;
import io.temporal.testing.internal.SDKTestOptions;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import io.temporal.worker.TypeAlreadyRegisteredException;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class TypedDynamicActivityTest {

  private static final String TYPED_DYNAMIC_ACTIVITY_TYPE = "TypedDynamicActivityType";

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setActivityImplementations(new TestTypedDynamicActivity())
          .build();

  /**
   * Test that TypedDynamicActivity can be registered and executed successfully. The activity
   * receives arguments via EncodedValues, accesses context via Activity.getExecutionContext(), and
   * returns a result.
   */
  @Test
  public void testTypedDynamicActivityExecution() {
    TestWorkflow workflow = testWorkflowRule.newWorkflowStub(TestWorkflow.class);
    String result = workflow.executeTypedDynamicActivity("Hello", false);
    assertEquals("TypedDynamic-Hello-TypedDynamicActivityType", result);
  }

  /**
   * Test that TypedDynamicActivity can be executed as a local activity. The activity should work
   * the same way as regular activities but execute locally.
   */
  @Test
  public void testTypedDynamicActivityAsLocalActivity() {
    TestWorkflow workflow = testWorkflowRule.newWorkflowStub(TestWorkflow.class);
    String result = workflow.executeTypedDynamicActivity("World", true);
    assertEquals("TypedDynamic-World-TypedDynamicActivityType", result);
  }

  /**
   * Test that TypedDynamicActivity can coexist with regular POJO activities on the same worker.
   * Both activity types should be callable from the same workflow.
   */
  @Rule
  public SDKTestWorkflowRule testWorkflowRuleWithMixedActivities =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(MixedActivitiesWorkflowImpl.class)
          .setActivityImplementations(new TestTypedDynamicActivity(), new RegularActivityImpl())
          .setDoNotStart(true)
          .build();

  @Test
  public void testTypedDynamicActivityWithRegularActivity() {
    testWorkflowRuleWithMixedActivities.getTestEnvironment().start();
    MixedActivitiesWorkflow workflow =
        testWorkflowRuleWithMixedActivities.newWorkflowStub(MixedActivitiesWorkflow.class);
    String result = workflow.executeBoth("Test");
    assertEquals(
        "TypedDynamic:TypedDynamic-Test-TypedDynamicActivityType|Regular:Regular-Test", result);
  }

  /**
   * Test that registering a TypedDynamicActivity with an activity type that is already registered
   * throws TypeAlreadyRegisteredException.
   */
  @Rule
  public SDKTestWorkflowRule testWorkflowRuleForDuplicateType =
      SDKTestWorkflowRule.newBuilder().setDoNotStart(true).build();

  @Test
  public void testDuplicateTypeRegistration() {
    Assert.assertThrows(
        TypeAlreadyRegisteredException.class,
        () ->
            testWorkflowRuleForDuplicateType
                .getWorker()
                .registerActivitiesImplementations(
                    new TestTypedDynamicActivity(), new DuplicateTypedDynamicActivity()));
  }

  /**
   * Test that registering a TypedDynamicActivity with the same type as a POJO activity throws
   * TypeAlreadyRegisteredException.
   */
  @Rule
  public SDKTestWorkflowRule testWorkflowRuleForDuplicateTypeWithPojo =
      SDKTestWorkflowRule.newBuilder().setDoNotStart(true).build();

  @Test
  public void testDuplicateTypeWithPojoActivity() {
    // Register POJO activity first
    testWorkflowRuleForDuplicateTypeWithPojo
        .getWorker()
        .registerActivitiesImplementations(new ConflictingPojoActivityImpl());
    // Try to register TypedDynamicActivity with same type
    Assert.assertThrows(
        TypeAlreadyRegisteredException.class,
        () ->
            testWorkflowRuleForDuplicateTypeWithPojo
                .getWorker()
                .registerActivitiesImplementations(new ConflictingTypedDynamicActivity()));
  }

  // Test TypedDynamicActivity implementation
  public static class TestTypedDynamicActivity implements TypedDynamicActivity {
    @Override
    public Object execute(EncodedValues args) {
      String input = args.get(0, String.class);
      String activityType = Activity.getExecutionContext().getInfo().getActivityType();
      return "TypedDynamic-" + input + "-" + activityType;
    }

    @Override
    public String getActivityType() {
      return TYPED_DYNAMIC_ACTIVITY_TYPE;
    }
  }

  // Another TypedDynamicActivity with the same type (for testing duplicate detection)
  public static class DuplicateTypedDynamicActivity implements TypedDynamicActivity {
    @Override
    public Object execute(EncodedValues args) {
      return "Duplicate";
    }

    @Override
    public String getActivityType() {
      return TYPED_DYNAMIC_ACTIVITY_TYPE; // Same type as TestTypedDynamicActivity
    }
  }

  // TypedDynamicActivity that conflicts with a POJO activity name
  public static class ConflictingTypedDynamicActivity implements TypedDynamicActivity {
    @Override
    public Object execute(EncodedValues args) {
      return "Conflicting";
    }

    @Override
    public String getActivityType() {
      return "ConflictingActivity"; // Same as ConflictingPojoActivity method name
    }
  }

  // Regular POJO activity interface and implementation
  @ActivityInterface
  public interface RegularActivity {
    String regularActivityMethod(String input);
  }

  public static class RegularActivityImpl implements RegularActivity {
    @Override
    public String regularActivityMethod(String input) {
      return "Regular-" + input;
    }
  }

  // POJO activity with a method name that conflicts with TypedDynamicActivity
  @ActivityInterface
  public interface ConflictingPojoActivity {
    @ActivityMethod(name = "ConflictingActivity")
    String conflictingMethod(String input);
  }

  public static class ConflictingPojoActivityImpl implements ConflictingPojoActivity {
    @Override
    public String conflictingMethod(String input) {
      return "Pojo-" + input;
    }
  }

  // Workflow interfaces and implementations
  @WorkflowInterface
  public interface TestWorkflow {
    @WorkflowMethod
    String executeTypedDynamicActivity(String input, boolean useLocal);
  }

  public static class TestWorkflowImpl implements TestWorkflow {
    @Override
    public String executeTypedDynamicActivity(String input, boolean useLocal) {
      if (useLocal) {
        ActivityStub localActivity =
            Workflow.newUntypedLocalActivityStub(
                LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .build());
        return localActivity.execute(TYPED_DYNAMIC_ACTIVITY_TYPE, String.class, input);
      } else {
        ActivityStub activity =
            Workflow.newUntypedActivityStub(
                ActivityOptions.newBuilder(SDKTestOptions.newActivityOptions()).build());
        return activity.execute(TYPED_DYNAMIC_ACTIVITY_TYPE, String.class, input);
      }
    }
  }

  @WorkflowInterface
  public interface MixedActivitiesWorkflow {
    @WorkflowMethod
    String executeBoth(String input);
  }

  public static class MixedActivitiesWorkflowImpl implements MixedActivitiesWorkflow {
    private final RegularActivity regularActivity =
        Workflow.newActivityStub(RegularActivity.class, SDKTestOptions.newActivityOptions());

    @Override
    public String executeBoth(String input) {
      // Call TypedDynamicActivity
      ActivityStub untypedActivity =
          Workflow.newUntypedActivityStub(
              ActivityOptions.newBuilder(SDKTestOptions.newActivityOptions()).build());
      String typedResult =
          untypedActivity.execute(TYPED_DYNAMIC_ACTIVITY_TYPE, String.class, input);

      // Call regular POJO activity
      String regularResult = regularActivity.regularActivityMethod(input);

      return "TypedDynamic:" + typedResult + "|Regular:" + regularResult;
    }
  }
}
