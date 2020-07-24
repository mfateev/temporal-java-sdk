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

import io.temporal.api.command.v1.Command;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.workflow.Functions;
import java.util.Objects;
import java.util.Optional;

public class NewCommand {

  private final Command command;
  private final CommandsBase commands;
  private final Functions.Proc1<HistoryEvent> matchingEventCallback;
  private boolean canceled;
  private Optional<Long> initialCommandEventId;

  public NewCommand(
      Command command, CommandsBase commands, Functions.Proc1<HistoryEvent> matchingEventCallback) {
    this.command = Objects.requireNonNull(command);
    this.commands = Objects.requireNonNull(commands);
    this.initialCommandEventId = Optional.empty();
    this.matchingEventCallback = matchingEventCallback;
  }

  public Optional<Command> getCommand() {
    if (canceled) {
      return Optional.empty();
    }
    return Optional.of(command);
  }

  public void cancel() {
    canceled = true;
  }

  public Optional<Long> getInitialCommandEventId() {
    return initialCommandEventId;
  }

  public void setInitialCommandEventId(long initialCommandEventId) {
    this.initialCommandEventId = Optional.of(initialCommandEventId);
  }

  /**
   * Called with matching command event. During non replay phase is called with null event to
   * indicate that all other events were already processed. This is used to unblock side effect and
   * similar marker based commands.
   */
  public void setMatchingEvent(HistoryEvent event) {
    if (event != null) {
      setInitialCommandEventId(event.getEventId());
    }
    if (this.matchingEventCallback != null) {
      this.matchingEventCallback.apply(event);
    }
  }

  public CommandsBase getCommands() {
    return commands;
  }
}