package io.temporal.internal.statemachines;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/** Action that can transition to exactly one state. */
class FixedTransitionAction<State, Data> implements TransitionAction<State, Data> {

  final State state;

  final Consumer<Data> action;

  FixedTransitionAction(State state, Consumer<Data> action) {
    this.state = state;
    this.action = action;
  }

  @Override
  public String toString() {
    return "FixedTransitionAction{" + "toState=" + state + "}";
  }

  @Override
  public State apply(Data data) {
    action.accept(data);
    return state;
  }

  @Override
  public List<State> getAllowedStates() {
    return Collections.singletonList(state);
  }
}
