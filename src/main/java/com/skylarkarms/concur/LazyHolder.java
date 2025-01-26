package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * Lock-free lazy initialization holder.
 * <p> Lazy concurrent holder that will spinlock concurrent calls until the inner value is considered
 * {@link #CREATED}.
 * <p> Different spin-lock strategies can be defined with a {@link Locks.Config} object via {@link Consumer}&lt;{@link Locks.Config.Builder}&gt; or {@link Locks.ExceptionConfig}&lt;{@link RuntimeException}&gt;.
 * */
public class LazyHolder<T> implements Lazy<T>{
    /**
     * Set to {@code true} for more detailed {@link Exception}s
     * <p> Setting this to {@code true} will hamper performance.
     * */
    private static volatile boolean debug = false;

    static volatile boolean debug_grabbed;
    public static synchronized void setDebug(boolean debug) {
        if (debug_grabbed) throw new IllegalStateException("A LazyHolder instance has already being called." +
                "This setting should be set before any instance has been initialized.");
        LazyHolder.debug = debug;
    }
    record DEBUG() {
        static {debug_grabbed = true;}
        static final boolean ref = debug;
    }

    static volatile Locks.ExceptionConfig<RuntimeException> holderConfig = Locks.ExceptionConfig.runtime_5();
    static volatile boolean config_grabbed;

    public static synchronized void setHolderConfig(Locks.ExceptionConfig<RuntimeException> holderConfig) {
        if (config_grabbed) throw new IllegalStateException("Configuration already in use, state must be set before any one instance of LazyHolder is initialized.");
        LazyHolder.holderConfig = holderConfig;
    }

    public static void setUnbridled() {
        setHolderConfig(Locks.ExceptionConfig.unbridled());
    }

    record FINAL_CONFIG() {
        static {config_grabbed = true;}
        final static Locks.ExceptionConfig<RuntimeException> ref = holderConfig;
    }

    private static final int
            NULL_PHASE = -1,
            CREATING_PHASE = 0,
    /**
     * Once the holder reaches this phase, the value is assumed fully assigned to this atomic reference.
     * */
    CREATED = 1;

    private static final Versioned<?>
            NULL = new Versioned<>(NULL_PHASE, null),
    /**
     * Process signaling the processing of the inner state
     * */
    CREATING = new Versioned<>(CREATING_PHASE, null);

    final Spinner<T> spinner;

    @FunctionalInterface
    private interface Spinner<T> {
        Versioned<T> spin();
    }

    /**
     * This spinner strategy consists on indefinitely busy-wait until the reference
     * has been assigned with a newly `CREATED` Object T.
     * */
    private final class Unbridled implements Spinner<T> {

        @Override
        public Versioned<T> spin() { // non dupe_2 is better common
            Versioned<T> prev = ref;
            while (prev == CREATING) { prev = ref; }
            return prev;
        }

        @Override
        public String toString() { return ">>> Spinner[unbridled]"; }
    }

    /**
     * A partial busy-wait lock that will park the {@link Thread} upon reaching the
     * threshold parameters specified by the user.
     * Or throw an Exception when the time comes.
     * */
    private final class Timeout implements Spinner<T> {

        @Override
        public Versioned<T> spin() {
            Versioned<T> load = ref;
            Locks.Config config = timeoutParams.config;

            long currentNanoTime = System.nanoTime();
            long totalTimeNanos = config.totalNanos;

            final long end = currentNanoTime + totalTimeNanos;
            final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

            final double backOffFactor = config.backOffFactor;

            long waitTime = config.initialWaitNanos; // Start with a small initial wait time

            boolean limitReached = false;

            while (
                    currentNanoTime < (end - waitTime)
                            &&
                            load == CREATING
            ) {
                long currentNano = System.nanoTime();
                final long curr_end = currentNano + waitTime;
                while (currentNano < curr_end) {
                    LockSupport.parkNanos(curr_end - currentNano);
                    currentNano = System.nanoTime();
                }

                waitTime = (long)(waitTime * backOffFactor);
                if (waitTime >= maxWaitNanos) {
                    limitReached = true;
                    break;
                }

                currentNanoTime = System.nanoTime();
                load = ref;
            }

            if (limitReached) {
                final long steadyEnd = end - maxWaitNanos;
                while (
                        currentNanoTime < steadyEnd
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + maxWaitNanos;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }
            }

            // Final precise waiting phase
            if (load == CREATING) {
                long currentNano = System.nanoTime();
                while (currentNano < end) {
                    LockSupport.parkNanos(end - currentNano);
                    currentNano = System.nanoTime();
                }

                load = ref;
                if (load != CREATING) return load;
                else {
                    throw throwRuntimeException(totalTimeNanos);
                }
            } else return load;
        }

        @Override
        public String toString() {
            return ">>> TimeoutSpinner{" +
                    "\n   >>> params= " + timeoutParams
                    + "\n}";
        }
    }

    final <E extends Exception> Versioned<T> timeoutSpin(Locks.ExceptionConfig<E> timeoutConfig) throws E {
        Versioned<T> load = ref;

        if (timeoutConfig.config.isUnbridled()) {
            while (load == CREATING) { load = ref; }
            return load;
        } else {
            Locks.Config config = timeoutConfig.config;

            long currentNanoTime = System.nanoTime();
            long totalTimeNanos = config.totalNanos;

            final long end = currentNanoTime + totalTimeNanos;
            final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

            final double backOffFactor = config.backOffFactor;

            long waitTime = config.initialWaitNanos; // Start with a small initial wait time

            boolean limitReached = false;

            while (
                    currentNanoTime < (end - waitTime)
                            &&
                            load == CREATING
            ) {
                long currentNano = System.nanoTime();
                final long curr_end = currentNano + waitTime;
                while (currentNano < curr_end) {
                    LockSupport.parkNanos(curr_end - currentNano);
                    currentNano = System.nanoTime();
                }

                waitTime = (long)(waitTime * backOffFactor);
                if (waitTime >= maxWaitNanos) {
                    limitReached = true;
                    break;
                }

                currentNanoTime = System.nanoTime();
                load = ref;
            }

            if (limitReached) {
                final long steadyEnd = end - maxWaitNanos;
                while (
                        currentNanoTime < steadyEnd
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + maxWaitNanos;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }
            }

            // Final precise waiting phase
            if (load == CREATING) {
                long currentNano = System.nanoTime();
                while (currentNano < end) {
                    LockSupport.parkNanos(end - currentNano);
                    currentNano = System.nanoTime();
                }

                load = ref;
                if (load != CREATING) return load;
                else {
                    throw throwRuntimeException(timeoutConfig, totalTimeNanos);
                }
            } else return load;
        }
    }

    /**
     * version = {@link #NULL_PHASE}
     * */

    @SuppressWarnings("unchecked")
    volatile Versioned<T> ref = (Versioned<T>) NULL;

    private static final VarHandle VALUE;
    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(LazyHolder.class, "ref", Versioned.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public final boolean isNull() { return NULL == VALUE.getOpaque(this); }

    final StackTraceElement[] es;

    LazyHolder(
            Consumer<Locks.Config.Builder> configBuilder
    ) {
        if (DEBUG.ref) {
            es = Thread.currentThread().getStackTrace();
        } else es = null;
        final Locks.Config.Builder b = new Locks.Config.Builder(LazyHolder.holderConfig.config);
        configBuilder.accept(b);
        this.timeoutParams = Locks.ExceptionConfig.runtime(b);
        this.spinner = timeoutParams.config.isUnbridled() ? new Unbridled() : new Timeout();
    }

    LazyHolder(
            Locks.ExceptionConfig<RuntimeException> config
    ) {
        if (DEBUG.ref) {
            es = Thread.currentThread().getStackTrace();
        } else es = null;
        this.timeoutParams = config;
        this.spinner = config.config.isUnbridled() ? new Unbridled() : new Timeout();
    }

    private final AtomicInteger clearingVer = new AtomicInteger();

    final Locks.ExceptionConfig<RuntimeException> timeoutParams;

    /**
     * @return null if the inner value was null or was already cleared
     * */
    @Override
    public final T getAndClear() {
        int c_ver = clearingVer.incrementAndGet();
        final Versioned<T> prev = ref;
        int ver = prev.version();

        return switch (ver) {
            case CREATED -> (c_ver == clearingVer.getOpaque() && VALUE.compareAndSet(this, prev, NULL)) ? prev.value() : null;
            case NULL_PHASE -> null;
            case CREATING_PHASE -> {
                // doing an adaptive erasing is less computationally expensive than creating a
                // "notificating"-like mechanic on the `get` side.
                // We would be adding a flag check on every `get` application... just to reduce the destruction
                // waiting phase by ~17 millis (on an advanced waiting time).
                // Anyway this phase will be a rare occurrence.... only on REAL contention
                // scenarios where the store has not been performed yet.
                Versioned<T> load = ref;

                if (load != prev) { // may have been (finally) created... or destroyed
                    if (load.version() == CREATED) {
                        if (VALUE.compareAndSet(this, load, NULL)) { // destroy.
                            yield load.value();
                        } else yield null;
                    }
                    else {
                        assert load == NULL;
                        yield null;
                    }
                }

                Locks.Config config = timeoutParams.config;

                long totalTimeNanos = config.totalNanos;
                long currentNanoTime = System.nanoTime();

                final long end = currentNanoTime + totalTimeNanos;
                final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                final double backOffFactor = config.backOffFactor;
                long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                boolean limitReached = false;

                while (
                        currentNanoTime < (end - waitTime)
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + waitTime;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    waitTime = (long)(waitTime * backOffFactor);
                    if (waitTime >= maxWaitNanos) {
                        limitReached = true;
                        break;
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }

                if (limitReached) {
                    final long steadyEnd = end - maxWaitNanos;
                    while (
                            currentNanoTime < steadyEnd
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + maxWaitNanos;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }
                }

                // Final precise waiting phase
                if (load == CREATING) {
                    long currentNano = System.nanoTime();

                    while (currentNano < end) {
                        LockSupport.parkNanos(end - currentNano);
                        currentNano = System.nanoTime();
                    }
                    load = ref;
                    if (load != CREATING) {
                        if (
                                load.version() == CREATED
                                        && c_ver == clearingVer.getOpaque()
                                        && VALUE.compareAndSet(this, load, NULL)
                        ) yield load.value();
                        else yield null;
                    }
                    else throw throwRuntimeException(totalTimeNanos);

                } else {
                    if (
                            load.version() == CREATED // was created!
                            && c_ver == clearingVer.getOpaque()
                            && VALUE.compareAndSet(this, load, NULL)
                    ) {
                        yield load.value();

                    } else yield null;
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + ver);
        };
    }

    @Override
    public final <E extends Exception> T getAndClear(final Locks.ExceptionConfig<E> timeoutConfig) throws E {
        int c_ver = clearingVer.incrementAndGet();
        final Versioned<T> prev = ref;
        int ver = prev.version();

        return switch (ver) {
            case CREATED -> (c_ver == clearingVer.getOpaque() && VALUE.compareAndSet(this, prev, NULL)) ? prev.value() : null;
            case NULL_PHASE -> null;
            case CREATING_PHASE -> {
                // doing an adaptive erasing is less computationally expensive than creating a
                // "notificating"-like mechanic on the `get` side.
                // We would be adding a flag check on every `get` application... just to reduce the destruction
                // waiting phase by ~17 millis (on an advanced waiting time).
                // Anyway this phase will be a rare occurrence.... only on REAL contention
                // scenarios where the store has not been performed yet.
                Versioned<T> load = ref;

                if (load != prev) { // may have been (finally) created... or destroyed
                    if (load.version() == CREATED) {
                        if (VALUE.compareAndSet(this, load, NULL)) { // destroy.
                            yield load.value();
                        } else yield null;
                    }
                    else {
                        assert load == NULL;
                        yield null;
                    }
                }

                Locks.Config config = timeoutConfig.config;

                long totalTimeNanos = config.totalNanos;
                long currentNanoTime = System.nanoTime();

                final long end = currentNanoTime + totalTimeNanos;
                final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                final double backOffFactor = config.backOffFactor;
                long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                boolean limitReached = false;

                while (
                        currentNanoTime < (end - waitTime)
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + waitTime;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    waitTime = (long)(waitTime * backOffFactor);
                    if (waitTime >= maxWaitNanos) {
                        limitReached = true;
                        break;
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }

                if (limitReached) {
                    final long steadyEnd = end - maxWaitNanos;
                    while (
                            currentNanoTime < steadyEnd
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + maxWaitNanos;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }
                }

                // Final precise waiting phase
                if (load == CREATING) {
                    long currentNano = System.nanoTime();

                    while (currentNano < end) {
                        LockSupport.parkNanos(end - currentNano);
                        currentNano = System.nanoTime();
                    }
                    load = ref;
                    if (load != CREATING) {
                        if (
                                load.version() == CREATED
                                        && c_ver == clearingVer.getOpaque()
                                        && VALUE.compareAndSet(this, load, NULL)
                        ) yield load.value();
                        else yield null;
                    }
                    else throw throwRuntimeException(timeoutConfig, totalTimeNanos);

                } else {
                    if (
                            load.version() == CREATED // was created!
                            && c_ver == clearingVer.getOpaque()
                            && VALUE.compareAndSet(this, load, NULL)
                    ) {
                        yield load.value();

                    } else yield null;
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + ver);
        };
    }

    private RuntimeException throwRuntimeException(long totalTimeNanos) {
        return throwRuntimeException(es, holderConfig, totalTimeNanos);
    }

    private<E extends Exception> E throwRuntimeException(Locks.ExceptionConfig<E> config, long totalTimeNanos) {
        return throwRuntimeException(es, config, totalTimeNanos);
    }

    static <E extends Exception> E throwRuntimeException(StackTraceElement[] es, Locks.ExceptionConfig<E> config, long totalTimeNanos) {
        String m = es == null ?
                "TimeOutException:"
                        + "\n Timeout parameters = " + config.config
                        + "\n Waiting = " + Locks.formatNanos(totalTimeNanos) + " nanos, "
                        + "\n This Holder is waiting too much on this Supplier."
                        + "\n One option is to broaden the values of the ExceptionConfig<Runtime> timeout parameter of the problematic Holder"
                        + "\n If this Exception keeps appearing, the cause may be a cyclic referencing"
                        + "\n  to find the possible source of the error, set 'com.skylarkarms.concurrents.LazyHolder.debug = true'."
                :
                "TimeOutException:"
                        + "\n Timeout parameters = " + config.config
                        + "\n Waiting = " + Locks.formatNanos(totalTimeNanos) + " nanos, "
                        + "\n This Holder is waiting too much on this Supplier."
                        + "\n One option is to broaden the values of the ExceptionConfig<Runtime> timeout parameter of the problematic Holder"
                        + "\n If this Exception keeps appearing, the cause may be a cyclic referencing"
                        + "\n at = " + formatStack(es);
        E e = config.exception.get();
        e.initCause(
                new Throwable(m)
        );
        return e;
    }

    /**
     * @return true if the {@code expect}-ed value matched the inner value.
     * @param expect the expected value that will allow the reference clearing.
     * */
    @Override
    public final boolean clear(T expect) {
        assert expect != null : "We'd rather expect that 'expect' was not null... thanks...";
        Versioned<T> prev = ref;
        return
                ((prev.value() == expect) || (prev.value() != null && prev.value().equals(expect)))
                        && VALUE.compareAndSet(this, prev, NULL);
    }

    /**
     * Will not trigger a {@link #CREATING} process
     * @return The current value.
     * */
    @SuppressWarnings("unchecked")
    @Override
    public final T getOpaque() { return ((Versioned<T>) VALUE.getOpaque(this)).value(); }

    /**
     * Lazy and stateful {@link java.util.function.Supplier}.
     * Concurrent calls to {@link #get()} will spin-lock until the
     * {@link #builder} has finished.
     * @see LazyHolder
     * */
    public static final class Supplier<T> extends LazyHolder<T> implements LazySupplier<T> {
        private final java.util.function.Supplier<T> builder;

        /**
         * Main {@link Supplier} constructor
         * @see com.skylarkarms.concur.Locks.ExceptionConfig
         * */
        public Supplier(
                Locks.ExceptionConfig<RuntimeException> config
                , java.util.function.Supplier<T> builder
        ) {
            super(
                    config
            );
            builderCheck(builder);
            this.builder = builder;
        }

        public Supplier(Consumer<Locks.Config.Builder> configBuilder
                , java.util.function.Supplier<T> builder
        ) {
            super(configBuilder);
            builderCheck(builder);
            this.builder = builder;
        }

        private void builderCheck(java.util.function.Supplier<T> builder) {
            if (builder == null) throw new IllegalStateException("'builder' Supplier cannot be null");
            if (builder instanceof LazyHolder.Supplier<T>) throw new IllegalStateException("'builder' is instance of LazyHolder.Supplier<?>");
        }

        Supplier(java.util.function.Supplier<T> builder) {
            this(FINAL_CONFIG.ref, builder);
        }

        public static <S> Supplier<S> getNew(java.util.function.Supplier<S> supplier) {
            return supplier instanceof LazyHolder.Supplier<S> s ? s : new Supplier<>(supplier);
        }

        /**
         * Will force a re-application of {@link #get()} if the value
         * was lost during creation phase to any one call of
         * {@link #clear(Object)} or {@link #getAndClear()}
         * One waiting period will consume one fresh Timeout parameter.
         * No assurances are given if a waiting period misses a destruction,
         * in which case both creating phases will be subjected to the same Timeout parameter.
         * The Timeout parameter will be refreshed each retry.
         * */
        @Override
        public T reviveGet(int maxTries) throws TimeoutException {
            maxTriesException(maxTries);
            Versioned<T> val = getVersioned();
            if (val.version() != CREATED) {
                int i = 0;
                while (val == NULL) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getVersioned();
                }
            }
            return val.value();
        }

        @Override
        public T reviveGet() {
            Versioned<T> val = getVersioned();
            if (val.version() != CREATED) {
                while (val == NULL) { val = getVersioned(); }
            }
            return val.value();
        }

        @Override
        public T get() {
            final Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev.value();
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    T res = builder.get();
                    ref = new Versioned<>(CREATED, res);
                    return res;
                } else return spinner.spin().value();
            }
        }

        @Override
        public <E extends Exception> T reviveGet(Locks.ExceptionConfig<E> config, final int maxTries) throws TimeoutException, E {
            maxTriesException(maxTries);
            Versioned<T> val = getVersioned(config);
            if (val.version() != CREATED) {
                int i = 0;
                while (val == NULL) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getVersioned(config);
                }
            }
            return val.value();
        }

        private <E extends Exception> Versioned<T> getVersioned(Locks.ExceptionConfig<E> config) throws E {
            Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev;
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    T res = builder.get();
                    prev = new Versioned<>(CREATED, res);
                    ref = prev;
                    return prev;
                } else return timeoutSpin(config);
            }
        }

        private Versioned<T> getVersioned() {
            Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev;
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    T res = builder.get();
                    prev = new Versioned<>(CREATED, res);
                    ref = prev;
                    return prev;
                } else return spinner.spin();
            }
        }

        @Override
        public <E extends Exception> T get(Locks.ExceptionConfig<E> config) throws E {
            return getVersioned(config).value();
        }

    }

    private static void maxTriesException(int maxTries) {
        if (maxTries == 0 || maxTries == Integer.MAX_VALUE) throw new IllegalStateException("Invalid retries");
    }

    public static final class OfInt implements OfIntSupplier {

        static final ValState NULL = new ValState(0, NULL_PHASE);
        static final ValState CREATING = new ValState(0, CREATING_PHASE);

        @FunctionalInterface
        private interface IntSpinner {
            ValState spin();
        }

        private <E extends Exception> ValState timeoutSpin(Locks.ExceptionConfig<E> timeoutConfig) throws E {
            ValState load = ref;

            if (timeoutConfig.config.isUnbridled()) {
                while (load == CREATING) { load = ref; }
                return load;
            } else {
                Locks.Config config = timeoutConfig.config;

                long currentNanoTime = System.nanoTime();
                long totalTimeNanos = config.totalNanos;

                final long end = currentNanoTime + totalTimeNanos;
                final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                final double backOffFactor = config.backOffFactor;

                long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                boolean limitReached = false;

                while (
                        currentNanoTime < (end - waitTime)
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + waitTime;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    waitTime = (long)(waitTime * backOffFactor);
                    if (waitTime >= maxWaitNanos) {
                        limitReached = true;
                        break;
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }

                if (limitReached) {
                    final long steadyEnd = end - maxWaitNanos;
                    while (
                            currentNanoTime < steadyEnd
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + maxWaitNanos;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }
                }

                // Final precise waiting phase
                if (load == CREATING) {
                    long currentNano = System.nanoTime();
                    while (currentNano < end) {
                        LockSupport.parkNanos(end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    load = ref;
                    if (load != CREATING) return load;
                    else {
                        throw throwRuntimeException(timeoutConfig, totalTimeNanos);
                    }
                } else return load;
            }
        }

        final Locks.ExceptionConfig<RuntimeException> timeoutParams;
        final IntSpinner spinner;

        private RuntimeException throwRuntimeException(long totalTimeNanos) {
            return LazyHolder.throwRuntimeException(es, holderConfig, totalTimeNanos);
        }

        private<E extends Exception> E throwRuntimeException(Locks.ExceptionConfig<E> config, long totalTimeNanos) {
            return LazyHolder.throwRuntimeException(es, config, totalTimeNanos);
        }

        final class TimeOut implements IntSpinner {

            @Override
            public ValState spin() {
                ValState load = ref;
                Locks.Config config = timeoutParams.config;

                long currentNanoTime = System.nanoTime();
                long totalTimeNanos = config.totalNanos;

                final long end = currentNanoTime + totalTimeNanos;
                final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                final double backOffFactor = config.backOffFactor;

                long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                boolean limitReached = false;

                while (
                        currentNanoTime < (end - waitTime)
                                &&
                                load == CREATING
                ) {
                    long currentNano = System.nanoTime();
                    final long curr_end = currentNano + waitTime;
                    while (currentNano < curr_end) {
                        LockSupport.parkNanos(curr_end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    waitTime = (long)(waitTime * backOffFactor);
                    if (waitTime >= maxWaitNanos) {
                        limitReached = true;
                        break;
                    }

                    currentNanoTime = System.nanoTime();
                    load = ref;
                }

                if (limitReached) {
                    final long steadyEnd = end - maxWaitNanos;
                    while (
                            currentNanoTime < steadyEnd
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + maxWaitNanos;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }
                }

                // Final precise waiting phase
                if (load == CREATING) {
                    long currentNano = System.nanoTime();
                    while (currentNano < end) {
                        LockSupport.parkNanos(end - currentNano);
                        currentNano = System.nanoTime();
                    }

                    load = ref;
                    if (load != CREATING) return load;
                    else {
                        throw throwRuntimeException(timeoutParams, totalTimeNanos);
                    }
                } else return load;
            }
        }

        final class Unbridled implements IntSpinner {

            @Override
            public ValState spin() {
                ValState prev;
                prev = ref;
                while (prev == CREATING) { prev = ref; }
                return prev;
            }
        }

        private final IntSupplier builder;

        volatile ValState ref = NULL;
        private static final VarHandle REF;
        final StackTraceElement[] es;

        static {
            try {
                REF = MethodHandles.lookup().findVarHandle(OfInt.class, "ref", ValState.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public static OfInt getNew(IntSupplier supplier) {
            return supplier instanceof OfInt oi ? oi : new OfInt(supplier);
        }

        OfInt(IntSupplier builder) { this(FINAL_CONFIG.ref, builder); }

        public OfInt(Locks.ExceptionConfig<RuntimeException> timeoutParams, IntSupplier builder) {
            this.builder = builder;
            if (DEBUG.ref) {
                es = Thread.currentThread().getStackTrace();
            } else es = null;
            this.timeoutParams = timeoutParams;
            this.spinner = timeoutParams.config.isUnbridled() ? new Unbridled() : new TimeOut();
        }

        public OfInt(Consumer<Locks.Config.Builder> paramsBuilder, IntSupplier builder) {
            this.builder = builder;
            if (DEBUG.ref) {
                es = Thread.currentThread().getStackTrace();
            } else es = null;
            final Locks.Config.Builder b = new Locks.Config.Builder(LazyHolder.holderConfig.config);
            paramsBuilder.accept(b);
            this.timeoutParams = Locks.ExceptionConfig.runtime(b);
            this.spinner = timeoutParams.config.isUnbridled() ? new Unbridled() : new TimeOut();
        }

        record ValState(int val, int state){}

        @Override
        public int getAsInt() {
            final ValState prev = ref;
            if (prev.state == CREATED) return prev.val;
            else {
                if (prev == NULL && REF.compareAndSet(this, NULL, CREATING)) {
                    int res = builder.getAsInt();
                    ref = new ValState(res, CREATED);
                    return res;
                } else return spinner.spin().val;
            }
        }

        @Override
        public <E extends Exception> int getAsInt(Locks.ExceptionConfig<E> config) throws E {
            return getState(config).val;
        }

        private final AtomicInteger clearingVer = new AtomicInteger();

        /**
         * @return null if the inner value was null or was already cleared
         * */
        @Override
        public int getAndClear() {
            int c_ver = clearingVer.incrementAndGet();
            final ValState prev = ref;
            int ver = prev.state;

            return switch (ver) {
                case CREATED -> (c_ver == clearingVer.getOpaque() && REF.compareAndSet(this, prev, NULL)) ? prev.val : 0;
                case NULL_PHASE -> 0;
                case CREATING_PHASE -> {
                    // doing an adaptive erasing is less computationally expensive than creating a
                    // "notificating"-like mechanic on the `get` side.
                    // We would be adding a flag check on every `get` application... just to reduce the destruction
                    // waiting phase by ~17 millis (on an advanced waiting time).
                    // Anyway this phase will be a rare occurrence.... only on REAL contention
                    // scenarios where the store has not been performed yet.
                    ValState load = ref;

                    if (load != prev) { // may have been (finally) created... or destroyed
                        if (load.state == CREATED) {
                            if (REF.compareAndSet(this, load, NULL)) { // destroy.
                                yield load.val;
                            } else yield 0;
                        }
                        else {
                            assert load == NULL;
                            yield 0;
                        }
                    }

                    Locks.Config config = timeoutParams.config;

                    long totalTimeNanos = config.totalNanos;
                    long currentNanoTime = System.nanoTime();

                    final long end = currentNanoTime + totalTimeNanos;
                    final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                    final double backOffFactor = config.backOffFactor;
                    long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                    boolean limitReached = false;

                    while (
                            currentNanoTime < (end - waitTime)
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + waitTime;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        waitTime = (long)(waitTime * backOffFactor);
                        if (waitTime >= maxWaitNanos) {
                            limitReached = true;
                            break;
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }

                    if (limitReached) {
                        final long steadyEnd = end - maxWaitNanos;
                        while (
                                currentNanoTime < steadyEnd
                                        &&
                                        load == CREATING
                        ) {
                            long currentNano = System.nanoTime();
                            final long curr_end = currentNano + maxWaitNanos;
                            while (currentNano < curr_end) {
                                LockSupport.parkNanos(curr_end - currentNano);
                                currentNano = System.nanoTime();
                            }

                            currentNanoTime = System.nanoTime();
                            load = ref;
                        }
                    }

                    // Final precise waiting phase
                    if (load == CREATING) {
                        long currentNano = System.nanoTime();

                        while (currentNano < end) {
                            LockSupport.parkNanos(end - currentNano);
                            currentNano = System.nanoTime();
                        }
                        load = ref;
                        if (load != CREATING) {
                            if (
                                    load.state == CREATED
                                            && c_ver == clearingVer.getOpaque()
                                            && REF.compareAndSet(this, load, NULL)
                            ) yield load.val;
                            else yield 0;
                        }
                        else throw throwRuntimeException(totalTimeNanos);

                    } else {
                        if (
                                load.state == CREATED // was created!
                                        && c_ver == clearingVer.getOpaque()
                                        && REF.compareAndSet(this, load, NULL)
                        ) {
                            yield load.val;

                        } else yield 0;
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + ver);
            };
        }

        @Override
        public <E extends Exception> int getAndClear(final Locks.ExceptionConfig<E> timeoutConfig) throws E {
            int c_ver = clearingVer.incrementAndGet();
            final ValState prev = ref;
            int ver = prev.state;

            return switch (ver) {
                case CREATED -> (c_ver == clearingVer.getOpaque() && REF.compareAndSet(this, prev, NULL)) ? prev.val : 0;
                case NULL_PHASE -> 0;
                case CREATING_PHASE -> {
                    // doing an adaptive erasing is less computationally expensive than creating a
                    // "notificating"-like mechanic on the `get` side.
                    // We would be adding a flag check on every `get` application... just to reduce the destruction
                    // waiting phase by ~17 millis (on an advanced waiting time).
                    // Anyway this phase will be a rare occurrence.... only on REAL contention
                    // scenarios where the store has not been performed yet.
                    ValState load = ref;

                    if (load != prev) { // may have been (finally) created... or destroyed
                        if (load.state == CREATED) {
                            if (REF.compareAndSet(this, load, NULL)) { // destroy.
                                yield load.val;
                            } else yield 0;
                        }
                        else {
                            assert load == NULL;
                            yield 0;
                        }
                    }

                    Locks.Config config = timeoutConfig.config;

                    long totalTimeNanos = config.totalNanos;
                    long currentNanoTime = System.nanoTime();

                    final long end = currentNanoTime + totalTimeNanos;
                    final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time

                    final double backOffFactor = config.backOffFactor;
                    long waitTime = config.initialWaitNanos; // Start with a small initial wait time

                    boolean limitReached = false;

                    while (
                            currentNanoTime < (end - waitTime)
                                    &&
                                    load == CREATING
                    ) {
                        long currentNano = System.nanoTime();
                        final long curr_end = currentNano + waitTime;
                        while (currentNano < curr_end) {
                            LockSupport.parkNanos(curr_end - currentNano);
                            currentNano = System.nanoTime();
                        }

                        waitTime = (long)(waitTime * backOffFactor);
                        if (waitTime >= maxWaitNanos) {
                            limitReached = true;
                            break;
                        }

                        currentNanoTime = System.nanoTime();
                        load = ref;
                    }

                    if (limitReached) {
                        final long steadyEnd = end - maxWaitNanos;
                        while (
                                currentNanoTime < steadyEnd
                                        &&
                                        load == CREATING
                        ) {
                            long currentNano = System.nanoTime();
                            final long curr_end = currentNano + maxWaitNanos;
                            while (currentNano < curr_end) {
                                LockSupport.parkNanos(curr_end - currentNano);
                                currentNano = System.nanoTime();
                            }

                            currentNanoTime = System.nanoTime();
                            load = ref;
                        }
                    }

                    // Final precise waiting phase
                    if (load == CREATING) {
                        long currentNano = System.nanoTime();

                        while (currentNano < end) {
                            LockSupport.parkNanos(end - currentNano);
                            currentNano = System.nanoTime();
                        }
                        load = ref;
                        if (load != CREATING) {
                            if (
                                    load.state == CREATED
                                            && c_ver == clearingVer.getOpaque()
                                            && REF.compareAndSet(this, load, NULL)
                            ) yield load.val;
                            else yield 0;
                        }
                        else throw throwRuntimeException(timeoutConfig, totalTimeNanos);
                    } else {
                        if (
                                load.state == CREATED // was created!
                                        && c_ver == clearingVer.getOpaque()
                                        && REF.compareAndSet(this, load, NULL)
                        ) {
                            yield load.val;

                        } else yield 0;
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + ver);
            };
        }

        /**
         * @return true if the {@code expect}-ed value matched the inner value.
         * @param expect the expected value that will allow the reference clearing.
         * */
        @Override
        public boolean clear(int expect) {
            ValState prev = ref;
            return
                    (prev.val == expect)
                            && REF.compareAndSet(this, prev, NULL);
        }

        private <E extends Exception> ValState getState(Locks.ExceptionConfig<E> config) throws E {
            ValState prev = ref;
            if (prev.state == CREATED) return prev;
            else {
                if (prev == NULL && REF.compareAndSet(this, NULL, CREATING)) {
                    prev = new ValState(builder.getAsInt(), CREATED);
                    ref = prev;
                    return prev;
                } else return timeoutSpin(config);
            }
        }

        private <E extends Exception> ValState getState() throws E {
            ValState prev = ref;
            if (prev.state == CREATED) return prev;
            else {
                if (prev == NULL && REF.compareAndSet(this, NULL, CREATING)) {
                    prev = new ValState(builder.getAsInt(), CREATED);
                    ref = prev;
                    return prev;
                } else return spinner.spin();
            }
        }

        /**
         * Will force a re-application of {@link #getAsInt()} if the value
         * was lost during creation phase to any one call of
         * {@link #clear(Object)} or {@link #getAndClear()}
         * One waiting period will consume one fresh Timeout parameter.
         * No assurances are given if a waiting period misses a destruction,
         * in which case both creating phases will be subjected to the same Timeout parameter.
         * The Timeout parameter will be refreshed each retry.
         * */
        @Override
        public int reviveGet(int maxTries) throws TimeoutException {
            if (maxTries == 0) throw new IllegalStateException("Invalid retries");
            ValState val = getState();
            if (maxTries == Integer.MAX_VALUE) {
                while (val == NULL) { val = getState(); }
            } else {
                int i = 0;
                while (val == null) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getState();
                }
            }
            return val.val;
        }

        @Override
        public <E extends Exception> int reviveGet(Locks.ExceptionConfig<E> config, int maxTries) throws TimeoutException, E {
            if (maxTries == 0) throw new IllegalStateException("Invalid retries");
            ValState val = getState(config);
            if (maxTries == Integer.MAX_VALUE) {
                while (val == NULL) { val = getState(config); }
            } else {
                int i = 0;
                while (val == null) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getState(config);
                }
            }
            return val.val;
        }
    }


    /**
     * Will apply and store the result.
     * <p> The {@link #function} function will apply ONCE, any concurrent calls to {@link #apply} will spinlock until the result has been resolved.
     * <p> The stored result will capture the scope of builder function in the constructor,
     * unless every value captured is properly de-referenced (deep copy).
     *
     * <p> Lambdas are prone to false-positive memory-leaks by APIs like Leak-Cannary.
     * Anonymous classes prevent them.
     * */
    public static final class Function<S, T> extends LazyHolder<T>
            implements LazyFunction<S, T> {

        private final java.util.function.Function<S, T> function;

        public Function(
                Locks.ExceptionConfig<RuntimeException> config
                , java.util.function.Function<S, T> function
        ) {
            super(config);
            functionCheck(function);
            this.function = function;
        }

        public Function(
                Consumer<Locks.Config.Builder> builder
                , java.util.function.Function<S, T> function
        ) {
            super(builder);
            functionCheck(function);
            this.function = function;
        }

        private void functionCheck(java.util.function.Function<S, T> function) {
            if (function == null) throw new IllegalArgumentException("'function' cannot be null");
            if (function instanceof LazyHolder.Function<S, T>) throw new IllegalArgumentException("'function' cannot be instance of this class.");
        }

        public static <T, U> Function<T, U> getNew(java.util.function.Function<T, U> function) {
            return function instanceof LazyHolder.Function<T, U> lf ? lf : new Function<>(function);
        }

        Function(java.util.function.Function<S, T> function) { this(FINAL_CONFIG.ref, function); }

        @Override
        public T reviveApply(S s) {
            Versioned<T> val = getVersioned(s);
            if (val.version() != CREATED) {
                while (val == NULL) { val = getVersioned(s); }
            }
            return val.value();
        }

        /**
         * Will force a re-application of {@link #apply(Object)} if the value
         * was lost during creation phase to any one call of
         * {@link #clear(Object)} or {@link #getAndClear()}
         * One waiting period will consume one fresh Timeout parameter.
         * No assurances are given if a waiting period misses a destruction,
         * in which case both creating phases will be subjected to the same Timeout parameter.
         * The Timeout parameter will be refreshed each retry.
         * */
        @Override
        public T reviveApply(S s, final int maxTries) throws TimeoutException {
            maxTriesException(maxTries);
            Versioned<T> val = getVersioned(s);
            if (val.version() != CREATED) {
                int i = 0;
                while (val == NULL) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getVersioned(s);
                }
            }
            return val.value();
        }

        @Override
        public <E extends Exception> T reviveApply(S s, final int maxTries, Locks.ExceptionConfig<E> config) throws TimeoutException, E {
            maxTriesException(maxTries);
            Versioned<T> val = getVersioned(s, config);
            if (val.version() != CREATED) {
                int i = 0;
                while (val == NULL) {
                    if (i++ > maxTries) throw new TimeoutException("Max tries [" + maxTries + "] reached");
                    val = getVersioned(s, config);
                }
            }
            return val.value();
        }

        @Override
        public T apply(S s) {
            final Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev.value();
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    T res = function.apply(s);
                    ref = new Versioned<>(CREATED, res);
                    return res;
                } else return spinner.spin().value();
            }
        }

        @Override
        public <E extends Exception> T apply(S s, Locks.ExceptionConfig<E> config) throws E {
            return getVersioned(s, config).value();
        }

        private <E extends Exception> Versioned<T> getVersioned(S s, Locks.ExceptionConfig<E> config) throws E {
            Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev;
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    prev = new Versioned<>(CREATED, function.apply(s));
                    ref = prev;
                    return prev;
                } else return timeoutSpin(config);
            }
        }

        private Versioned<T> getVersioned(S s) {
            Versioned<T> prev = ref;
            if (prev.version() == CREATED) return prev;
            else {
                if (prev == NULL && VALUE.compareAndSet(this, NULL, CREATING)) {
                    prev = new Versioned<>(CREATED, function.apply(s));
                    ref = prev;
                    return prev;
                } else return spinner.spin();
            }
        }
    }

    /**
     * Collection of generic classes ({@link V}) that extend {@link Supplier}s that can be stored and retrieved via keys of common type {@link K} .
     * */
    public static class KeyedCollection<K, V extends Supplier<?>> extends AbstractMap<K, V> {
        /**
         * Default implementation of {@link KeyedCollection} where {@code V} = {@link Supplier} of type {@code ?}
         * */
        public static class Default<K> extends KeyedCollection<K, Supplier<?>> {

            @SafeVarargs
            public
            <E extends SupplierEntry<?, K, Supplier<?>>>
            Default(
                    boolean unmodifiable
                    , E... entries
            ) {
                super(unmodifiable, entries);
            }

            @SafeVarargs
            public Default(boolean unmodifiable, SupplierEntry.Default<K, ?>... entries) {
                super(unmodifiable, entries);
            }

            public Default() { super(); }
        }
        /**The reason behind the map NOT being of at least of type AbstractEntry...
         * is because there should be a flexible way to populate it from the start.*/
        final Map<K, V> map;

        /**
         * Key-value pair entry for the {@link KeyedCollection} class
         * */
        public static class SupplierEntry<T, K, V extends Supplier<T>>{
            final K key;
            final V value;

            public SupplierEntry(K key, V value) {
                this.key = key;
                this.value = value;
            }

            protected V valueTemplate(Object... varArgs) {return null;}

            public SupplierEntry(K key, Object... varArgs) {
                this.key = key;
                this.value = valueTemplate(varArgs);
                if (value == null) throw new IllegalStateException("Must override valueTemplate(Object... varArgs)");
            }

            public static class Default<K, T> extends SupplierEntry<T, K, Supplier<T>> {

                public Default(K key, java.util.function.Supplier<T> value) {
                    super(key,
                            new Supplier<>(value)
                    );
                }

                private Default(K key, Supplier<T> value) { super(key, value); }
            }
        }

        @SafeVarargs
        public<E extends SupplierEntry<?, K, V>> KeyedCollection(
                boolean unmodifiable,
                final E... entries
        ) {
            if (unmodifiable) {
                this.map = getUnmodifiableAbs(entries);
            } else {
                final Map<K, V> map = new ConcurrentHashMap<>();
                if (entries != null) {
                    for (E e:entries) {
                        exceptPut(map, e);
                    }
                }
                this.map = map;
            }
        }

        @SafeVarargs
        public KeyedCollection(
                boolean unmodifiable,
                final SupplierEntry.Default<K, ?>... entries
        ) {
            if (unmodifiable) {
                this.map = getUnmodifiableAbs(entries);
            } else {
                final Map<K, V> map = new ConcurrentHashMap<>();
                if (entries != null) {
                    for (SupplierEntry.Default<K, ?> e:entries) {
                        exceptTypedPut(map, e);
                    }
                }
                this.map = map;
            }
        }

        <E extends SupplierEntry<?, K, V>> void exceptPut(Map<K, V> map, E e) {
            final V e_val = e.value;
            V val = map.putIfAbsent(e.key, e_val);
            if (val == null) {
                onAdded(e_val);
            } else throw new IllegalStateException("Key " + e.key + " already present in map = " + map);
        }

        @SuppressWarnings("unchecked")
        <T, E extends SupplierEntry<T, K, ? extends Supplier<T>>> void exceptTypedPut(Map<K, V> map, E e) {
            final V e_val = (V) e.value;
            V val = map.putIfAbsent(e.key, e_val);
            if (val == null) {
                onAdded(e_val);
            } else throw new IllegalStateException("Key " + e.key + " already present in map = " + map);
        }

        /**
         * Will get called once a key-value pair ({@link SupplierEntry}) has been successfully added.
         * @see Map#putIfAbsent(Object, Object)
         * */
        protected void onAdded(V value) {}

        public KeyedCollection() { this.map = new ConcurrentHashMap<>(); }

        /**
         * This method will only accept new keys.
         * @throws IllegalStateException if the key was already contained in the {@link Map}.
         * @return the value inserted.
         * */
        @Override
        public V put(K key, V value) {
            V prev = map.put(key, value);
            if (prev != null) throw new IllegalStateException("Key [" + key + "] already present in map = " + map);
            return value;
        }

        public<T, S extends Supplier<T>, E extends SupplierEntry<T, K, S>> E put(E value) {
            exceptTypedPut(map, value);
            return value;
        }

        @Override
        public void clear() { map.clear(); }


        @Override
        public V get(Object key) { return map.get(key); }

        @Override
        public V remove(Object key) { return map.remove(key); }

        @Override
        public Set<Entry<K, V>> entrySet() { return map.entrySet(); }

        @SafeVarargs
        private <E extends SupplierEntry<?, K, V>>
        Map<K, V>
        getUnmodifiableAbs(final E... entries) {
            Map<K, V> map = new HashMap<>();
            if (entries == null) throw new IllegalStateException("Entries must not be null");
            for (E e:entries) {
                exceptPut(map, e);
            }
            return Collections.unmodifiableMap(map);
        }

        @SafeVarargs
        private
        Map<K, V>
        getUnmodifiableAbs(final SupplierEntry.Default<K, ?>... entries) {
            Map<K, V> map = new HashMap<>();
            if (entries == null) throw new IllegalStateException("Entries must not be null");
            for (SupplierEntry.Default<K, ?> e:entries) {
                exceptTypedPut(map, e);
            }
            return Collections.unmodifiableMap(map);
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) { map.forEach(action); }
    }

    /**
     * Collection of {@link LazyHolder}s that allows for the storage of a <p>
     * single instance of any given class inserted.
     * <p> The collection cannot be changed once the Collection is created. <p>
     * */
    public static
    class SingletonCollection<T, S extends Supplier<T>> extends KeyedCollection<Class<T>, S> {
        public static class Default extends KeyedCollection.Default<Class<?>> {

            @SafeVarargs
            public
            <E extends SupplierEntry<?, Class<?>, Supplier<?>>>
            Default(
                    final E... entries
            ) {
                super(true, entries);
            }

            @SafeVarargs
            public Default(SupplierEntry.Default<Class<?>, ?>... entries) { super(true, entries); }

            @SafeVarargs
            public <E extends SupplierEntry<?, Class<?>, Supplier<?>>> Default(boolean unmodifiable, E... entries) {
                super(unmodifiable, entries);
            }

            public<C> C get(Class<C> tClass) {
                Supplier<?> supplier = Objects.requireNonNull(map.get(tClass), () -> "Type: " + tClass + " not present in map " + map);
                return tClass.cast(supplier.get());
            }

            @Override
            public Supplier<?> get(Object key) {
                throw new UnsupportedOperationException("SingletonCollection cannot accept anything other than tClass keys.");
            }
        }

        public static class SingletonEntry<T, S extends Supplier<T>>
                extends SupplierEntry<T, Class<T>, S> {

            public SingletonEntry(Class<T> key, S value) { super(key, value); }
        }

        @SafeVarargs
        public<E extends SingletonEntry<T, S>> SingletonCollection(
                boolean unmodifiable
                , E... entries) {
            super(unmodifiable, entries);
        }

        @SafeVarargs
        public <E extends SupplierEntry<T, Class<T>, S>> SingletonCollection(
                E... entries) {
            super(true, entries);
        }

        private SingletonCollection() { }

        public<C> C get(Class<C> tClass) {
            Supplier<?> supplier = Objects.requireNonNull(map.get(tClass), () -> "Type: " + tClass + " not present in map " + map);
            return tClass.cast(supplier.get());
        }

        @Override
        public S get(Object key) {
            throw new UnsupportedOperationException("SingletonCollection cannot accept anything other than tClass keys.");
        }
    }

    static String toStateString(int state) {
        return switch (state) {
            case NULL_PHASE -> "NULL";
            case CREATING_PHASE -> "CREATING";
            default -> "CREATED";
        };
    }

    private static final String formatStack(StackTraceElement[] es) throws AssertionError {
        int le = es.length;
        assert le > 0 : "`startAt` index [" + 0 + "] greater than or equal to StackTraceElement arrays length [" + le  +"].";
        StringBuilder sb = new StringBuilder((le * 2) + 2);
        sb.append("\n >> stack {");
        for (int i = 0; i < le; i++) {
            StackTraceElement e = es[i];
            sb.append("\n   - at [").append(i).append("] ").append(e);
        }
        return sb.append("\n } << stack. [total length = ").append(le).append("]").toString();
    }

    @Override
    public String toString() {
        String hash = Integer.toString(hashCode());
        Versioned<T> current = ref;
        return "LazyHolder@".concat(hash).concat("{"
                + "\n >>> status=[" + toStateString(current.version()) + "]"
                + ",\n >>> ref=\n" + current.toString().concat(",").indent(3)
                + spinner.toString().indent(1)
                + " }@").concat(hash);
    }

    public String toStringDetailed() {
        String hash = Integer.toString(hashCode());
        Versioned<T> current = ref;
        return es != null ?
                "LazyHolder@".concat(hash).concat("{" +
                        "\n >>> status=[" + toStateString(current.version()) + "]" +
                        ",\n >>> ref=\n" + current.toStringDetailed().concat(",").indent(3)
                        + spinner.toString().concat(",").indent(1) +
                        " >>> provenance=" + formatStack(es).indent(3) +
                        "}@").concat(hash)
                :
                "LazyHolder@".concat(hash).concat("{" +
                        "\n >>> status=[" + toStateString(current.version()) + "]" +
                        ",\n >>> ref=\n" + current.toStringDetailed().concat(",").indent(3)
                        + spinner.toString().indent(1) +
                        "}@").concat(hash);
    }
}