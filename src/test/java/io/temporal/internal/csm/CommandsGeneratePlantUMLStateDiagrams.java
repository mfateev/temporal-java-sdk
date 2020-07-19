/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.csm;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import io.temporal.workflow.Functions;
import io.temporal.workflow.WorkflowTest;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandsGeneratePlantUMLStateDiagrams {

  private static final Logger log = LoggerFactory.getLogger(WorkflowTest.class);

  @Test
  public void plantUML() {
    generate(ActivityCommands.class, ActivityCommands::asPlantUMLStateDiagram);
    generate(TimerCommands.class, TimerCommands::asPlantUMLStateDiagram);
    generate(SignalExternalCommands.class, SignalExternalCommands::asPlantUMLStateDiagram);
  }

  private void generate(
      Class<? extends CommandsBase> commandClass, Functions.Func<String> generator) {
    String projectPath = System.getProperty("user.dir");
    String relativePath = commandClass.getName().replace(".", File.separator);
    String fullRelativePath = ("src/main/java/" + relativePath).replace("/", File.separator);
    String diagramFile = (projectPath + "/" + fullRelativePath).replace("/", File.separator);
    File file = new File(diagramFile + ".puml");
    CharSink sink = Files.asCharSink(file, Charsets.UTF_8);
    File licenseFile = new File(projectPath + File.separator + "license-header.txt");
    StringBuilder content = new StringBuilder();
    try {
      List<String> license = Files.readLines(licenseFile, Charsets.UTF_8);
      for (String licenseLine : license) {
        content.append("`" + licenseLine + "\n");
      }
      content.append("\n");
      content.append(
          "` Generated from " + fullRelativePath + ".java\n` by " + this.getClass().getName());
      content.append("\n\n");
      content.append(generator.apply());
      if (file.exists()) {
        String existingContent = Files.asCharSource(file, Charsets.UTF_8).read();
        if (existingContent.equals(content.toString())) {
          return;
        }
      }
      sink.write(content);
    } catch (IOException e) {
      Throwables.propagateIfPossible(e, RuntimeException.class);
    }
    log.info("Regenerated state diagram file: " + diagramFile);
  }
}
