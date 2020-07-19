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

import io.grpc.Status;
import io.temporal.api.enums.v1.EventType;
import io.temporal.workflow.Functions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * State machine of a single server side entity like activity, workflow task or the whole workflow.
 *
 * <p>Based on the idea that each entity goes through state transitions and the same operation like
 * timeout is applicable to some states only and can lead to different actions in each state. Each
 * valid state transition should be registered through add methods. The associated callback is
 * invoked when the state transition is requested.
 */
final class StateMachine<State, Action, Data> {

  private final List<Transition<State, ActionOrEventType<Action>>> transitionHistory =
      new ArrayList<>();
  private final Map<Transition<State, ActionOrEventType<Action>>, TransitionTarget<State, Data>>
      transitions =
          new LinkedHashMap<>(); // linked to maintain the same order for diagram generation

  private final State initialState;
  private final List<State> finalStates;

  private State state;

  public static <State, Action, Data> StateMachine<State, Action, Data> newInstance(
      State initialState, State... finalStates) {
    return new StateMachine<>(initialState, finalStates);
  }

  public StateMachine(State initialState, State[] finalStates) {
    this.initialState = initialState;
    if (finalStates.length == 0) {
      throw new IllegalArgumentException("At least one final state is required");
    }
    this.finalStates = Arrays.asList(finalStates);
    this.state = initialState;
  }

  public State getState() {
    return state;
  }

  public boolean isFinalState() {
    return finalStates.contains(state);
  }

  /**
   * Registers a transition between states.
   *
   * @param from initial state that transition applies to
   * @param to destination state of a transition.
   * @param callback callback to invoke upon transition
   * @return the current StateMachine instance for the fluid pattern.
   */
  StateMachine<State, Action, Data> add(
      State from, Action action, State to, Functions.Proc1<Data> callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType<>(action)),
        new FixedTransitionTarget<>(to, callback));
    return this;
  }

  StateMachine<State, Action, Data> add(State from, Action action, State to) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType<>(action)),
        new FixedTransitionTarget<>(to, (data) -> {}));
    return this;
  }

  StateMachine<State, Action, Data> add(
      State from, EventType eventType, State to, Functions.Proc1<Data> callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType<>(eventType)),
        new FixedTransitionTarget<>(to, callback));
    return this;
  }

  StateMachine<State, Action, Data> add(State from, EventType eventType, State to) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType<>(eventType)),
        new FixedTransitionTarget<>(to, (data) -> {}));
    return this;
  }

  /**
   * Registers a dynamic transition between states. Used when the same action can transition to more
   * than one state depending on data.
   *
   * @param from initial state that transition applies to
   * @param toStates allowed destination states of a transition.
   * @param callback callback to invoke upon transition
   * @return the current StateMachine instance for the fluid pattern.
   */
  StateMachine<State, Action, Data> add(
      State from, Action action, State[] toStates, DynamicCallback<State, Data> callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType<>(action)),
        new DynamicTransitionTarget<>(toStates, callback));
    return this;
  }

  void action(Action action, Data data) {
    action(new ActionOrEventType<>(action), data);
  }

  void handleEvent(EventType eventType, Data data) {
    action(new ActionOrEventType<>(eventType), data);
  }

  private void action(ActionOrEventType<Action> actionOrEventType, Data data) {
    Transition<State, ActionOrEventType<Action>> transition =
        new Transition<>(state, actionOrEventType);
    TransitionTarget<State, Data> destination = transitions.get(transition);
    if (destination == null) {
      throw Status.INTERNAL
          .withDescription("Invalid " + transition + ", history: " + transitionHistory)
          .asRuntimeException();
    }
    state = destination.apply(data);
    transitionHistory.add(transition);
  }

  public String asPlantUMLStateDiagram() {
    StringBuilder result = new StringBuilder();
    result.append("@startuml\n" + "scale 350 width\n");
    result.append("[*] --> ");
    result.append(initialState);
    result.append('\n');
    for (Map.Entry<Transition<State, ActionOrEventType<Action>>, TransitionTarget<State, Data>>
        entry : transitions.entrySet()) {
      List<State> targets = entry.getValue().getAllowedStates();
      for (State target : targets) {
        result.append(entry.getKey().getFrom());
        result.append(" --> ");
        result.append(target);
        result.append(": ");
        result.append(entry.getKey().getAction());
        result.append('\n');
      }
    }
    for (State finalState : finalStates) {
      result.append(finalState);
      result.append(" --> [*]\n");
    }
    result.append("@enduml\n");
    return result.toString();
  }
}
