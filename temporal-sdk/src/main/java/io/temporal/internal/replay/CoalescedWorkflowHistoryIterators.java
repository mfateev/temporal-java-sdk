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
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import java.nio.charset.StandardCharsets;
import java.util.*;

class CoalescedWorkflowHistoryIterators {

  private class SplitIterator implements WorkflowHistoryIterator {

    private long eventId = 1;
    private Iterator<HistoryEvent> iterator;

    public SplitIterator(List<HistoryEvent> events) {
      for (int i = 0; i < events.size(); i++) {
        HistoryEvent event = events.get(i).toBuilder().setEventId(i + 1).build();
        events.add(event);
      }
      iterator = events.iterator();
    }

    @Override
    public void initDeadline(Deadline deadline) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public HistoryEvent next() {
      return iterator.next();
    }
  }

  List<SplitIterator> splitIterators;

  public CoalescedWorkflowHistoryIterators(int size, WorkflowHistoryIterator historyIterator) {
    List<List<HistoryEvent>> splits = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      splits.add(new ArrayList<>());
    }
    while (historyIterator.hasNext()) {
      HistoryEvent event = historyIterator.next();
      switch (event.getEventType()) {
        case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
          WorkflowExecutionStartedEventAttributes attr =
              event.getWorkflowExecutionStartedEventAttributes();
          Payloads payloads = attr.getInput();
          for (int i = 0; i < splits.size(); i++) {
            Payload payload = payloads.getPayloads(i);
            WorkflowExecutionStartedEventAttributes.Builder updatedAttr =
                attr.toBuilder().setInput(Payloads.newBuilder().addPayloads(payload).build());
            HistoryEvent updated =
                event.toBuilder().setWorkflowExecutionStartedEventAttributes(updatedAttr).build();
            splits.get(i).add(updated);
          }
          break;
        default:
          Payload summary = event.getUserMetadata().getSummary();
          if (summary.containsMetadata("split")) {
            int index =
                Integer.parseInt(
                    summary.getMetadataOrThrow("split").toString(StandardCharsets.UTF_8));
            splits.get(index).add(event);
          } else {
            for (int i = 0; i < splits.size(); i++) {
              splits.get(i).add(event);
            }
          }
      }
    }
    splitIterators = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      splitIterators.add(new SplitIterator(splits.get(i)));
    }
  }

  public WorkflowHistoryIterator get(int i) {
    return splitIterators.get(i);
  }
}
