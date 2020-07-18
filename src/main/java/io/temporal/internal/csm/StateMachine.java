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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * State machine of a single server side entity like activity, workflow task or the whole workflow.
 *
 * <p>Based on the idea that each entity goes through state transitions and the same operation like
 * timeout is applicable to some states only and can lead to different actions in each state. Each
 * valid state transition should be registered through {@link #add(Object, Object, Object,
 * Functions.Proc)}. The associated callback is invoked when the state transition is requested.
 */
final class StateMachine<State, Action, Data> {

  /**
   * Function invoked when an action happens in a given state. Returns the next state. Used when the
   * next state depends not only on the current state and action, but also on the data.
   */
  @FunctionalInterface
  interface DynamicCallback<State> {

    /** @return state after the action */
    State apply();
  }

  private static class ActionOrEventType<Action> {
    final Action action;
    final EventType eventType;

    private ActionOrEventType(Action action) {
      this.action = action;
      this.eventType = null;
    }

    public ActionOrEventType(EventType eventType) {
      this.eventType = eventType;
      this.action = null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ActionOrEventType<?> that = (ActionOrEventType<?>) o;
      return com.google.common.base.Objects.equal(action, that.action)
          && eventType == that.eventType;
    }

    @Override
    public int hashCode() {
      return com.google.common.base.Objects.hashCode(action, eventType);
    }

    @Override
    public String toString() {
      if (action == null) {
        return eventType.toString();
      }
      return action.toString();
    }
  }

  private static class Transition<State, ActionOrEventType> {

    final State from;
    final ActionOrEventType action;

    public Transition(State from, ActionOrEventType action) {
      this.from = Objects.requireNonNull(from);
      this.action = Objects.requireNonNull(action);
    }

    public State getFrom() {
      return from;
    }

    public ActionOrEventType getAction() {
      return action;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Transition<?, ?> that = (Transition<?, ?>) o;
      return com.google.common.base.Objects.equal(from, that.from)
          && com.google.common.base.Objects.equal(action, that.action);
    }

    @Override
    public int hashCode() {
      return com.google.common.base.Objects.hashCode(from, action);
    }

    @Override
    public String toString() {
      return "Transition{" + "from='" + from + '\'' + ", action=" + action + '}';
    }
  }

  private interface TransitionTarget<State> {
    State apply();

    List<State> getAllowedStates();
  }

  private static class FixedTransitionTarget<State> implements TransitionTarget<State> {

    final State state;

    final Functions.Proc callback;

    private FixedTransitionTarget(State state, Functions.Proc callback) {
      this.state = state;
      this.callback = callback;
    }

    @Override
    public String toString() {
      return "TransitionDestination{" + "state=" + state + ", callback=" + callback + '}';
    }

    @Override
    public State apply() {
      callback.apply();
      return state;
    }

    @Override
    public List<State> getAllowedStates() {
      return Collections.singletonList(state);
    }
  }

  private static class DynamicTransitionTarget<State> implements TransitionTarget<State> {

    final DynamicCallback<State> callback;
    State[] expectedStates;
    State state;

    private DynamicTransitionTarget(State[] expectedStates, DynamicCallback<State> callback) {
      this.expectedStates = expectedStates;
      this.callback = callback;
    }

    @Override
    public String toString() {
      return "DynamicTransitionDestination{" + "state=" + state + ", callback=" + callback + '}';
    }

    @Override
    public State apply() {
      state = callback.apply();
      for (State s : expectedStates) {
        if (s.equals(state)) {
          return state;
        }
      }
      throw new IllegalStateException(
          state + " is not expected. Expected states are: " + Arrays.toString(expectedStates));
    }

    @Override
    public List<State> getAllowedStates() {
      return Arrays.asList(expectedStates);
    }
  }

  private final List<Transition<State, ActionOrEventType>> transitionHistory = new ArrayList<>();
  private final Map<Transition<State, ActionOrEventType>, TransitionTarget<State>> transitions =
      new HashMap<>();

  private final State initialState;
  private final List<State> finalStates;

  private State state;

  public static <State> StateMachine newInstance(State initialState, State... finalStates) {
    return new StateMachine(initialState, finalStates);
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
  StateMachine add(State from, Action action, State to, Functions.Proc callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType(action)),
        new FixedTransitionTarget<>(to, callback));
    return this;
  }

  StateMachine add(State from, Action action, State to) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType(action)),
        new FixedTransitionTarget<>(to, () -> {}));
    return this;
  }

  StateMachine add(State from, EventType eventType, State to, Functions.Proc callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType(eventType)),
        new FixedTransitionTarget<>(to, callback));
    return this;
  }

  StateMachine add(State from, EventType eventType, State to) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType(eventType)),
        new FixedTransitionTarget<>(to, () -> {}));
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
  StateMachine add(State from, Action action, State[] toStates, DynamicCallback<State> callback) {
    transitions.put(
        new Transition<>(from, new ActionOrEventType(action)),
        new DynamicTransitionTarget<>(toStates, callback));
    return this;
  }

  void action(Action action) {
    action(new ActionOrEventType(action));
  }

  void handleEvent(EventType eventType) {
    action(new ActionOrEventType(eventType));
  }

  private void action(ActionOrEventType actionOrEventType) {
    Transition<State, ActionOrEventType> transition = new Transition<>(state, actionOrEventType);
    TransitionTarget<State> destination = transitions.get(transition);
    if (destination == null) {
      throw Status.INTERNAL
          .withDescription("Invalid " + transition + ", history: " + transitionHistory)
          .asRuntimeException();
    }
    state = destination.apply();
    transitionHistory.add(transition);
  }

  public String toPlantUML() {
    StringBuilder result = new StringBuilder();
    result.append("@startuml\n" + "scale 350 width\n");
    result.append("[*] --> ");
    result.append(initialState);
    result.append('\n');
    for (Map.Entry<Transition<State, ActionOrEventType>, TransitionTarget<State>> entry :
        transitions.entrySet()) {
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
