package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class collects {@link Consumer}s via {@link #push(Consumer)}, or {@link #pushUnique(Consumer, Supplier)}
 * <p> The class will cache the Consumers until an {@link #accept(Object)} or {@link #accepted(Object)}
 * <p> will flush all Consumers on a FIFO manner ({@link Deque#pollLast()})
 * <p> Adding {@link Consumer}s directly to the {@link Deque} may produce undesirable effects.
 * */
public final class FlushableConsumers<X> implements Consumer<X> {
    private final Deque<Consumer<? super X>> consumers;

    public FlushableConsumers(Deque<Consumer<? super X>> consumers) { this.consumers = consumers; }

    private volatile Process<X> process = Process.init();

    @SuppressWarnings("unchecked")
    public boolean isProcessing() { return ((Process<X>)PROCESS.getOpaque(this)).busy(); }
    private static final VarHandle PROCESS;
    static {
        try {
            PROCESS = MethodHandles.lookup().findVarHandle(FlushableConsumers.class, "process", Process.class);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    static class Process<X> {
        final X value;
        private static final Process<?> idle = new Process<>(null), init = new Process<>(null) {
            @Override
            boolean busy() { return false; }
        };

        Process(X value) { this.value = value; }

        @SuppressWarnings("unchecked")
        static<S> Process<S> idle() { return (Process<S>) idle; }
        @SuppressWarnings("unchecked")
        static<S> Process<S> init() { return (Process<S>) init; }
        boolean busy() { return this != idle; }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void accept(X x) {
        Process<?> prev;
        while (
                ((prev = (Process<?>)PROCESS.getOpaque(this))).busy()
        ) {}
        Consumer<? super X> next;
        if (PROCESS.compareAndSet(this, prev, new Process<>(x))) {
            while (
                    (next = consumers.pollLast()) != null
            ) {
                next.accept(x);
            }
            PROCESS.setOpaque(this, Process.idle());
        } else accept(x);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public boolean accepted(X x) {
        Process<?> prev;
        while (
                ((prev = (Process<?>)PROCESS.getOpaque(this))).busy()
        ) {}
        if (PROCESS.compareAndSet(this, prev, new Process<>(x))) {
            Consumer<? super X> next;
            boolean loopDone = false;
            if ((next = consumers.pollLast()) != null) {
                next.accept(x);
                while (
                        (next = consumers.pollLast()) != null
                ) {
                    next.accept(x);
                }
                loopDone = true;
            }
            PROCESS.setOpaque(this, Process.idle());
            return loopDone;
        } else return accepted(x);
    }

    /**
     * If the {@link #accept(Object)} dispatch is currently processing,
     * The consumer will consume the value && will NOT be added to the  Dequeue.
     * */
    @SuppressWarnings("unchecked")
    public void push(Consumer<? super X> consumer) {
        Process<X> prev;
        if ((prev = (Process<X>)PROCESS.getOpaque(this)).busy()) {
            consumer.accept(prev.value);
        } else {
            consumers.push(consumer);
        }
    }

    /**
     * Removes the {@link Consumer} waiting to be flushed
     * */
    public boolean remove(Consumer<? super X> consumer) { return consumers.remove(consumer); }

    /**
     * throws if already contained in Dequeue
     * */
    public <T extends Throwable> void pushUnique(Consumer<? super X> consumer, Supplier<T> t) throws T {
        synchronized (consumers) {
            if (consumers.contains(consumer)) {
                throw t.get();
            }
            push(consumer);
        }
    }

    public void clear() { consumers.clear(); }

    /**
     * This class will store a {@link T} 'value', and evaluate if a {@link Predicate} 'test' is satisfied({@code true}).
     * The {@link #acquire(Consumer)} method will return the value if the current {@link T} 'value'
     * has satisfied the 'when' {@link Predicate} OR enqueue the {@link Consumer} if the test has not been satisfied.
     * When the test has returned {@code true} an assertion error may(depending on VM options) verify the consistency
     * of the current state evaluation against the 'when' {@link Predicate} and throw if the state has been changed via side-effect.
     * When the test returns {@code false} upon {@link #acquire(Consumer)} execution no assertion will be applied,
     * instead it will be directly pushed to a {@link Deque}, UNTIL a {@link #set(Object)}, {@link #accept(Object)} OR {@link #accepted(Object)}
     * re-evaluates the {@link Predicate} 'test' and triggers a {@link #flush()}
     * If the value has been altered via side-effects, the method {@link #tryFlush()}
     * will re-apply the 'when' valuation, and flush when it returns {@code true}.
     * */
    public static final class Acquire<T>
            implements Supplier<T>
    {
        private volatile ValTest<T> value;
        private final Predicate<T> when;
        private final FlushableConsumers<T> consumers;
        static final VarHandle VALUE;

        public boolean isNull() { return null == value.val; }

        public void clearConsumers() { consumers.clear(); }

        @Override
        @SuppressWarnings("unchecked")
        public T get() { return ((ValTest<T>)VALUE.getOpaque(this)).val; }

        private static final String stateChangedErr = "The state appears to have been changed before 'flush' " +
                "\n in a side-effect fashion, use 'tryFlush' instead.";

        /**
         * Checks if the inner state has not had any side-effect
         * actions performed on it's state before dispatch.
         * The dispatch will remove all enqueued {@link Consumer}s in a FIFO manner.
         * */
        public boolean flush() {
            ValTest<T> current;
            if ((current = value).test) {
                boolean dispatched = consumers.accepted(current.val);
                if (!when.test(current.val)) throw new IllegalStateException(stateChangedErr);
                return dispatched;
            }
            else if (!when.test(current.val)) throw new IllegalStateException(stateChangedErr);
            return false;
        }

        public boolean set(T value) {
            if (value == null) throw new IllegalStateException("Use setNull() instead.");
            final ValTest<T> newVal;
            if ((this.value = (newVal = new ValTest<>(value, when.test(value))))
                    .test
                    && compare(newVal)
            ) {
                consumers.accept(newVal.val);
                return true;
            } else return false;
        }

        public boolean compareAndSet(T expect, T set) {
            if (set == null) throw new IllegalStateException("Use setNull() instead.");
            final ValTest<T> prev = value, newVal;
            if (
                    Objects.equals(expect, prev.val)
                            && VALUE.compareAndSet(this, prev,
                            newVal = new ValTest<>(set, when.test(set))
                    )
                            && newVal.test
            ) {
                consumers.accept(newVal.val);
                return true;
            } else return false;
        }

        public boolean setNull() { return VALUE.compareAndSet(this, VALUE.getOpaque(this), ValTest.getDefault()); }

        @SuppressWarnings("unchecked")
        public T getAndClear() { return ((ValTest<T>)VALUE.getAndSet(this, ValTest.getDefault())).val; }

        public boolean setIfNull(Supplier<T> value) {
            final ValTest<T> curr, newVal;
            final T newTVal;
            if ((curr = this.value).isDefault() &&
                    VALUE.compareAndSet(
                            this, curr,
                            newVal = new ValTest<>(newTVal = value.get(), when.test(newTVal))
                    )
                    && newVal.test
            ) {
                consumers.accept(newVal.val);
                return true;
            } else return false;
        }

        private synchronized boolean compare(ValTest<T> newVal) { return newVal == value; }

        @SuppressWarnings("unchecked")
        public boolean tryFlush() {
            ValTest<T> prev;
            boolean prevYes, equal;
            if ((equal = (prevYes =
                    (prev = (ValTest<T>) VALUE.getOpaque(this))
                            .test) == when.test(prev.val)) && prevYes) {
                return consumers.accepted(prev.val);
            } else if (!equal) {
                boolean set;
                ValTest<T> next;
                if ((set = VALUE.compareAndSet(this, prev,
                        next = new ValTest<>(prev.val, !prevYes)
                )) && prevYes) {
                    return false;
                } else return set && consumers.accepted(next.val);
            }
            return false;
        }

        record ValTest<T>(T val, boolean test) {
            static ValTest<?> DEF = new ValTest<>(null, false);
            @SuppressWarnings("unchecked")
            static<S> ValTest<S> getDefault() { return (ValTest<S>) DEF; }

            public boolean isDefault() { return this == DEF; }
        }

        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(Acquire.class,
                        "value", ValTest.class);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public static<S> Acquire<S> nonNull(boolean concurrent) {
            return new Acquire<>(concurrent, null, Objects::nonNull);
        }
        public Acquire(boolean concurrent, T value, Predicate<T> when) {
            final Deque<Consumer<? super T>>
                    dequeue = concurrent ?
                    new ConcurrentLinkedDeque<>() :
                    new ArrayDeque<>();
            this.when = when;
            this.value = value == null ?
                    ValTest.getDefault() :
                    new ValTest<>(value, when.test(value));
            this.consumers = new FlushableConsumers<>(dequeue);
        }

        /**
         * Update the current state if the value's inner state has changed via side-effects
         * */
        @SuppressWarnings("unchecked")
        public boolean updateState() {
            ValTest<T> prev;
            boolean prevYes, equal;
            if ((equal = (prevYes =
                    (prev = (ValTest<T>) VALUE.getOpaque(this))
                            .test) == when.test(prev.val)) && prevYes) {
                return true;
            }
            else if (!equal) {
                boolean set;
                if ((set = VALUE.compareAndSet(this, prev,
                        new ValTest<>(prev.val, !prevYes)
                )) && prevYes) {
                    return false;
                } else return set;
            }
            return false;
        }

        @SuppressWarnings("unchecked")
        private BooleanSupplier updateState(Supplier<T> supplier) {
            return supplier != null ?
                    () -> {
                        T nextT;
                        boolean test;
                        return VALUE.compareAndSet(this,
                                (ValTest<T>) VALUE.getOpaque(this),
                                new ValTest<>(nextT = supplier.get(), test = when.test(nextT))
                        ) && test;
                    }
                    : () -> {
                ValTest<T> prev;
                boolean prevYes, equal;
                if ((equal = (prevYes =
                        (prev = (ValTest<T>) VALUE.getOpaque(this))
                                .test) == when.test(prev.val)) && prevYes) {
                    return true;
                }
                else if (!equal) {
                    boolean set;
                    if ((set = VALUE.compareAndSet(this, prev,
                            new ValTest<>(prev.val, !prevYes)
                    )) && prevYes) {
                        return false;
                    } else return set;
                }
                return false;
            };
        }

        @SuppressWarnings("unchecked")
        public boolean acquire(Consumer<? super T> waiter) {
            ValTest<T> current;
            if ((current = (ValTest<T>) VALUE.getOpaque(this)).test) {
                waiter.accept(current.val);
                assert when.test(current.val) == current.test : "The value was proactively altered with side-effects";
                return true;
            } else {
                consumers.push(waiter);
                return false;
            }
        }
        public boolean remove(Consumer<? super T> waiter) { return consumers.remove(waiter); }
    }
}