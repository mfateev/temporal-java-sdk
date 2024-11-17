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

package io.temporal.internal.replay;

import io.grpc.Deadline;
import io.temporal.api.history.v1.HistoryEvent;
import java.util.*;

public class MultiIterator {
  private final List<HistoryEvent> buffer = new ArrayList<>();
  private final List<SplitHistoryIterator> startedIdIterators = new ArrayList<>();
  private final List<SplitHistoryIterator> iterators = new ArrayList<>();
  private WorkflowHistoryIterator sourceIterator;

  public MultiIterator(int splitCount) {
    for (int i = 0; i < splitCount; i++) {
      iterators.add(new SplitHistoryIterator(i, new SplitIterator()));
      startedIdIterators.add(new SplitHistoryIterator(i, new SplitIterator()));
    }
  }

  public void setSourceIterator(
      WorkflowHistoryIterator sourceIterator, long previousStartedEventId) {
    this.sourceIterator = sourceIterator;
    buffer.clear();
    for (int i = 0; i < iterators.size(); i++) {
      iterators.get(i).reset(previousStartedEventId);
      startedIdIterators.get(i).reset(previousStartedEventId);
    }
  }

  public SplitHistoryIterator getIterator(int splitIndex) {
    return iterators.get(splitIndex);
  }

  public SplitHistoryIterator getStartedIdIterator(int splitIndex) {
    return startedIdIterators.get(splitIndex);
  }

  class SplitIterator implements WorkflowHistoryIterator {

    private int position;

    @Override
    public boolean hasNext() {
      // Check if position is within the buffer or if there are elements in the source iterator
      return position < buffer.size() || sourceIterator.hasNext();
    }

    @Override
    public HistoryEvent next() {
      if (position < buffer.size()) {
        // Return the buffered element
        return buffer.get(position++);
      }
      if (sourceIterator.hasNext()) {
        // Fetch and buffer the next element from the source
        HistoryEvent nextValue = sourceIterator.next();
        buffer.add(nextValue);
        position++;
        return nextValue;
      }
      throw new NoSuchElementException("No more events");
    }

    @Override
    public void initDeadline(Deadline deadline) {
      // TODO(maxim): ???

    }

    public void reset() {
      position = 0;
    }

    @Override
    public String toString() {
      return String.valueOf(buffer);
    }
  }
}
