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
import io.temporal.api.enums.v1.CommandType;
import io.temporal.api.history.v1.HistoryEvent;
import java.util.Objects;
import java.util.Optional;

public class NewCommand {

  private final Command command;
  private final CommandsBase commands;
  private boolean canceled;
  private Optional<Long> initialCommandEventId;

  public NewCommand(Command command, CommandsBase commands) {
    this.command = Objects.requireNonNull(command);
    this.commands = Objects.requireNonNull(commands);
    this.initialCommandEventId = Optional.empty();
  }

  public Command getCommand() {
    if (canceled) {
      throw new IllegalStateException("canceled");
    }
    return command;
  }

  public boolean isCanceled() {
    return canceled;
  }

  public void cancel() {
    canceled = true;
  }

  public Optional<Long> getInitialCommandEventId() {
    return initialCommandEventId;
  }

  /** Notifies about eventId that command event will have. */
  public void setInitialCommandEventId(long initialCommandEventId) {
    this.initialCommandEventId = Optional.of(initialCommandEventId);
  }

  public CommandsBase getCommands() {
    return commands;
  }

  public CommandType getCommandType() {
    return command.getCommandType();
  }

  public void handleEvent(HistoryEvent event) {
    if (canceled) {
      return;
    }
    commands.handleEvent(event);
    if (!canceled) {
      initialCommandEventId = Optional.of(event.getEventId());
    }
  }
}
