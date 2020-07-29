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

package io.temporal.internal.replay;

import io.temporal.api.command.v1.Command;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.api.query.v1.WorkflowQueryResult;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.worker.ActivityTaskHandler;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface WorkflowExecutor {

  void handleWorkflowTask(PollWorkflowTaskQueueResponseOrBuilder workflowTask);

  WorkflowTaskResult getResult();

  Optional<Payloads> handleQueryWorkflowTask(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask, WorkflowQuery query);

  List<ExecuteLocalActivityParameters> getLocalActivityRequests();

  void close();

  void handleLocalActivityCompletion(ActivityTaskHandler.Result laCompletion);

  class WorkflowTaskResult {

    private final List<Command> commands;
    private final boolean finalCommand;
    private final Map<String, WorkflowQueryResult> queryResults;

    public WorkflowTaskResult(
        List<Command> commands,
        Map<String, WorkflowQueryResult> queryResults,
        boolean finalCommand) {
      this.commands = commands;
      this.queryResults = queryResults;
      this.finalCommand = finalCommand;
    }

    public List<Command> getCommands() {
      return commands;
    }

    public Map<String, WorkflowQueryResult> getQueryResults() {
      return queryResults;
    }

    /** Is this result contain a workflow completion command */
    public boolean isFinalCommand() {
      return finalCommand;
    }
  }
}
