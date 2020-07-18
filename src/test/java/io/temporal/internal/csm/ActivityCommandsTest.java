package io.temporal.internal.csm;

import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import org.junit.Test;

public class ActivityCommandsTest {

  ActivityCommands commands =
      new ActivityCommands(
          ScheduleActivityTaskCommandAttributes.newBuilder().build(), (a, b, c) -> {}, (c) -> {});

  @Test
  public void plantUML() {
    System.out.println(commands.toPlantUML());
  }
}
