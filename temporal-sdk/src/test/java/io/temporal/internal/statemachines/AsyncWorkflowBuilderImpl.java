package io.temporal.internal.statemachines;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class AsyncWorkflowBuilderImpl<T> implements AsyncWorkflowBuilder<T> {

  private final Queue<Runnable> scheduled;

  private final List<Consumer<T>> callbacks = new ArrayList<>();

  private final Consumer<T> callback =
      (result) -> {
        for (Consumer<T> callback : callbacks) {
          schedule(() -> callback.accept(result));
        }
      };

  void apply(T value) {
    callback.accept(value);
  }

  private void schedule(Runnable proc) {
    scheduled.add(proc);
  }

  AsyncWorkflowBuilderImpl(Queue<Runnable> scheduled) {
    this.scheduled = scheduled;
  }

  @Override
  public <R> AsyncWorkflowBuilder<R> add1(BiConsumer<T, Consumer<R>> proc) {
    AsyncWorkflowBuilderImpl<R> scheduler = new AsyncWorkflowBuilderImpl<>(scheduled);
    callbacks.add((value) -> schedule(() -> proc.accept(value, scheduler.callback)));
    return scheduler;
  }

  @Override
  public <R1, R2> AsyncWorkflowBuilder<Pair<R1, R2>> add2(BiConsumer<T, BiConsumer<R1, R2>> proc) {
    AsyncWorkflowBuilderImpl<Pair<R1, R2>> scheduler = new AsyncWorkflowBuilderImpl<>(scheduled);
    callbacks.add(
        (value) ->
            schedule(
                () ->
                    proc.accept(value, (t1, t2) -> scheduler.callback.accept(new Pair<>(t1, t2)))));
    return scheduler;
  }

  @Override
  public AsyncWorkflowBuilder<T> add(Consumer<T> proc) {
    callbacks.add((result) -> schedule(() -> proc.accept(result)));
    return this;
  }
}
