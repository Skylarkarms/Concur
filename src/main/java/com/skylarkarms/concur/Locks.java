package com.skylarkarms.concur;

import com.skylarkarms.lambdas.Suppliers;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class Locks<E extends Exception> {

    /**
     * This performs better than boolean flag semaphores.
     * Valet will `park` a Thread and then be considered "busy" by which stage it can either:
     * <ul>
     *     <li>{@link #unpark()} it</li>
     *     <li>{@link #discharge()} it</li>
     * </ul>
     * {@link #discharge()} can be called BEFORE a call to park.
     * <p> Calling {@link #unpark()} BEFORE a call to {@link #park()} will throw an {@link IllegalStateException}.
     * */
    public static final class Valet {

        private final Locks.ExceptionConfig<TimeoutException> exc;

        public Valet() {
            exc = ExceptionConfig.timeout();
        }

        public Valet(Consumer<Config.Builder> builder) {
            this.exc = ExceptionConfig.timeout(builder);
        }

        volatile Thread t;

        private static final VarHandle T;

        static {
            try {
                T = MethodHandles.lookup().findVarHandle(Valet.class, "t", Thread.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public boolean isBusy() { return T.getOpaque(this) != null; }

        final BooleanSupplier waitIf = () -> T.getOpaque(this) != null;

        public boolean park(
                long duration
                , TimeUnit unit
                , Suppliers.OfString cause
        ) throws TimeoutException {
            if (wasLetGo.getOpaque()) return false;
            if (T.compareAndSet(this, null, Thread.currentThread())) {
                ExceptionConfig<TimeoutException> tc = exc.copyWith(duration, unit);
                waitIf(
                        tc,
                        this.waitIf,
                        cause
                );
                return true;
            } else return false;
        }

        public boolean park(
                long seconds, Suppliers.OfString cause
        ) throws TimeoutException {
            if (wasLetGo.getOpaque()) return false;
            if (T.compareAndSet(this, null, Thread.currentThread())) {
                ExceptionConfig<TimeoutException> tc = exc.copyWith(seconds, TimeUnit.SECONDS);
                waitIf(
                        tc,
                        this.waitIf,
                        cause
                );
                return true;
            } else return false;
        }

        /**
         * @param cause: cause of a possible failure if the time (default 10 seconds) has expired.
         * @return {@code `true`} if the code performed a park,
         * <p> {@code `false`} if not.
         * @throws TimeoutException if 10 seconds have passed without response.
         * */
        public boolean park(Suppliers.OfString cause) throws TimeoutException {
            if (wasLetGo.getOpaque()) return false;
            if (T.compareAndSet(this, null, Thread.currentThread())) {
                waitIf(
                        exc,
                        this.waitIf,
                        cause
                );
                return true;
            } else return false;
        }

        /**
         * @return {@code `true`} if the code performed a park,
         * <p> {@code `false`} if not.
         * @throws TimeoutException if 10 seconds have passed without response.
         * */
        public boolean park() throws TimeoutException {
            if (wasLetGo.getOpaque()) return false;
            if (T.compareAndSet(this, null, Thread.currentThread())) {
                waitIf(
                        exc,
                        this.waitIf
                );
                return true;
            } else return false;
        }

        public<E extends Exception> boolean park(ExceptionConfig<E> config) throws E {
            if (wasLetGo.getOpaque()) return false;
            if (T.compareAndSet(this, null, Thread.currentThread())) {
                waitIf(
                        config,
                        this.waitIf
                );
                return true;
            } else return false;
        }

        public boolean park(
                Consumer<Config.Builder> builder,
                Suppliers.OfString cause
        ) throws TimeoutException {
            if (wasLetGo.getOpaque()) return false;
            Thread curr = Thread.currentThread();
            if (T.compareAndSet(this, null, curr)) {
                ExceptionConfig<TimeoutException> unbr = exc.copyWith(builder);
                waitIf(
                        unbr,
                        this.waitIf,
                        cause
                );
                return true;
            } else return false;
        }

        public void unpark() throws IllegalStateException {
            Object t;
            if ((t = T.getAndSet(this, null)) != null) {
                LockSupport.unpark((Thread) t);
            }
            throw new IllegalStateException("No Thread has been parked yet.");
        }

        private final AtomicBoolean wasLetGo = new AtomicBoolean();

        public void discharge() {
            if (wasLetGo.compareAndSet(false, true)) {
                Object t;
                if ((t = T.getAndSet(this, null)) != null) {
                    LockSupport.unpark((Thread) t);
                }
            }
        }
        public boolean recruit() {
            return wasLetGo.compareAndSet(true, false);
        }
    }

    private static void durationExcep(long duration) {
        if (duration == 0 || duration == Long.MAX_VALUE) throw new IllegalStateException("Not a valid duration");
    }

    private static void unitExcept(TimeUnit unit) {
        if (unit == null) throw new NullPointerException("Unit cannot be null");
    }

    public static void robustPark(TimeUnit unit, long duration) {
        durationExcep(duration);
        long currentNano = System.nanoTime();
        final long end = currentNano + unit.toNanos(duration);
        while (currentNano < end) {
            LockSupport.parkNanos(end - currentNano);
            currentNano = System.nanoTime();
        }
    }

    public static void robustPark(long nanos) {
        durationExcep(nanos);
        long currentNano = System.nanoTime();
        final long end = currentNano + nanos;
        while (currentNano < end) {
            LockSupport.parkNanos(end - currentNano);
            currentNano = System.nanoTime();
        }
    }

    private static Config globalConfig = Config.DEFAULT_CONFIG;
    private static final AtomicBoolean grabbed = new AtomicBoolean();

    private static Config getConfig() {
        grabbed.setOpaque(true);
        return globalConfig;
    }

    public static synchronized void setGlobalConfig(Consumer<Config.Builder> builder) {
        if (builder == null) throw new NullPointerException("`builder` was null");
        if (globalConfig != Config.DEFAULT_CONFIG) throw new IllegalStateException("Can only set once");
        if (grabbed.getOpaque())
            throw new IllegalStateException("A Lock has already been instantiated with a `globalConfig` Config instance.");

        Config.Builder builder1 = new Config.Builder(globalConfig);
        builder.accept(builder1);
        globalConfig = new Config(builder1);
    }

    static final class Config {
        private final int initialWaitFraction, maxWaitFraction;
        final long totalNanos, initialWaitNanos, maxWaitNanos;
        final double backOffFactor;

        /**
         * Default implementation of {@link Config} that uses these values as default:
         * <ul>
         *     <li>
         *         {@link #totalNanos} = 50000000 (50 millis)
         *     </li>
         *     <li>
         *         {@link #initialWaitFraction} = 1000
         *     </li>
         *     <li>
         *         {@link #maxWaitFraction} = 100
         *     </li>
         *     <li>
         *         {@link #backOffFactor} = 1.3
         *     </li>
         * </ul>
         * */
        public static final Config DEFAULT_CONFIG = new Config(
                50000000, //50 millis
                1000,
                100,
                1.3
        );

        Config(
                long totalNanos, int initialWaitFraction, int maxWaitFraction, double backOffFactor) {
            this.totalNanos = totalNanos;
            this.initialWaitFraction = initialWaitFraction;
            this.maxWaitFraction = maxWaitFraction;
            this.initialWaitNanos = totalNanos / initialWaitFraction;
            this.maxWaitNanos = totalNanos / maxWaitFraction;
            this.backOffFactor = backOffFactor;
        }
        Config(
                Builder builder) {
            this.totalNanos = builder.totalNanos;
            this.initialWaitFraction = builder.initialWaitFraction;
            this.maxWaitFraction = builder.maxWaitFraction;
            this.initialWaitNanos = builder.initialWaitNanos;
            this.maxWaitNanos = builder.maxWaitNanos;
            this.backOffFactor = builder.backOffFactor;
        }

        Config(long duration, TimeUnit unit, Config parent) {
            durationExcep(duration);
            unitExcept(unit);
            this.totalNanos = unit.toNanos(duration);
            this.initialWaitFraction = parent.initialWaitFraction;
            this.maxWaitFraction = parent.maxWaitFraction;
            this.initialWaitNanos = totalNanos / initialWaitFraction;
            this.maxWaitNanos = totalNanos / maxWaitFraction;
            this.backOffFactor = parent.backOffFactor;
        }

        public static final class Builder {
            private TimeUnit unit;
            private long totalDuration;

            int initialWaitFraction;
            int maxWaitFraction;
            double backOffFactor;

            long totalNanos;
            long initialWaitNanos;
            long maxWaitNanos;

            Builder(Config defaultConfig) {
                this.unit = TimeUnit.NANOSECONDS;
                this.totalDuration = defaultConfig.totalNanos;
                this.initialWaitFraction = defaultConfig.initialWaitFraction;
                this.maxWaitFraction = defaultConfig.maxWaitFraction;
                this.backOffFactor = defaultConfig.backOffFactor;
                this.totalNanos = defaultConfig.totalNanos;
                this.initialWaitNanos = defaultConfig.initialWaitNanos;
                this.maxWaitNanos = defaultConfig.maxWaitNanos;
            }

            void waitFractionException() {
                if (initialWaitFraction < maxWaitFraction)
                    throw new IllegalStateException("`initialWaitFraction` [" + initialWaitFraction + "] cannot be LESSER THAN `maxWaitFraction` [" + maxWaitFraction + "]");
            }

            public Builder setInitialWaitFraction(int initialWaitFraction) {
                if (this.initialWaitFraction != initialWaitFraction) {
                    this.initialWaitFraction = initialWaitFraction;
                    waitFractionException();
                    setInitialWaitNanos();
                }
                return this;
            }

            public void setBackOffFactor(double backOffFactor) {
                this.backOffFactor = backOffFactor;
            }

            public Builder setMaxWaitFraction(int maxWaitFraction) {
                if (this.maxWaitFraction != maxWaitFraction) {
                    this.maxWaitFraction = maxWaitFraction;
                    waitFractionException();
                    setMaxWaitNanos();
                }
                return this;
            }

            public Builder setDurationUnit(long duration, TimeUnit unit) {
                unitExcept(unit);
                durationExcep(duration);

                final boolean firstDiff;

                if (
                        (firstDiff = duration != this.totalDuration)
                                ||
                                this.unit != unit
                ) {
                    if (firstDiff) {
                        if (this.unit != unit) {
                            this.unit = unit;
                        }
                        this.totalDuration = duration;
                    } else {
                        this.unit = unit;
                    }
                    setTotalNanos();
                }

                return this;
            }

            public Builder setTotalDuration(long totalDuration) {
                durationExcep(totalDuration);
                if (this.totalDuration != totalDuration) {
                    this.totalDuration = totalDuration;
                    setTotalNanos();
                }
                return this;
            }

            public Builder setUnit(TimeUnit unit) {
                unitExcept(unit);
                if (this.unit != unit) {
                    this.unit = unit;
                    setTotalNanos();
                }
                return this;
            }

            private void setTotalNanos() {
                this.totalNanos = unit.toNanos(totalDuration);
                setInitialWaitNanos();
                setMaxWaitNanos();
            }

            private void setInitialWaitNanos() {
                this.initialWaitNanos = totalNanos / initialWaitFraction;
            }

            private void setMaxWaitNanos() {
                this.maxWaitNanos = totalNanos / maxWaitFraction;
            }
        }
    }

    public static final class ExceptionConfig<E extends Exception> {
        final Supplier<E> exception; final Config config;

        static final class DefaultExc {
            record runtime() {
                static final Supplier<RuntimeException> ref = RuntimeException::new;
            }
            record timeout() {
                static final Supplier<TimeoutException> ref = TimeoutException::new;
            }
        }

        ExceptionConfig(Supplier<E> exception, Config config) {
            this.exception = exception;
            this.config = config;
        }

        record runtime() {
            static final ExceptionConfig<RuntimeException>
                    ref = new ExceptionConfig<>(DefaultExc.runtime.ref, getConfig());
        }

        public static ExceptionConfig<RuntimeException> runtime() {
            return runtime.ref;
        }

        record timeout() {
            static final ExceptionConfig<TimeoutException>
                    ref = new ExceptionConfig<>(DefaultExc.timeout.ref, getConfig());
        }

        /**
         * Uses the default configuration defined at {@link #globalConfig} via {@link #setGlobalConfig(Consumer)}
         * */
        public static ExceptionConfig<TimeoutException> timeout() {
            return timeout.ref;
        }

        public static ExceptionConfig<TimeoutException> timeout(long duration, TimeUnit unit) {
            return new ExceptionConfig<>(
                    DefaultExc.timeout.ref, new Config(duration, unit, getConfig()));
        }

        public static ExceptionConfig<TimeoutException> timeout(long millis) {
            return new ExceptionConfig<>(
                    DefaultExc.timeout.ref, new Config(millis, TimeUnit.MILLISECONDS, getConfig()));
        }

        public static ExceptionConfig<TimeoutException> timeout(Consumer<Config.Builder> builder) {
            Config.Builder builder1 = new Config.Builder(getConfig());
            builder.accept(builder1);
            return new ExceptionConfig<>(
                    DefaultExc.timeout.ref, new Config(builder1));
        }

        public static ExceptionConfig<RuntimeException> runtime(Consumer<Config.Builder> builder) {
            Config.Builder builder1 = new Config.Builder(getConfig());
            builder.accept(builder1);
            return new ExceptionConfig<>(
                    DefaultExc.runtime.ref, new Config(builder1));
        }

        public static<E extends Exception> ExceptionConfig<E> custom(Supplier<E> exception, Consumer<Config.Builder> builder) {
            Config.Builder builder1 = new Config.Builder(getConfig());
            builder.accept(builder1);
            return new ExceptionConfig<>(
                    exception, new Config(builder1));
        }

        public static<E extends Exception> ExceptionConfig<E> custom(Supplier<E> exception, long millis) {
            return new ExceptionConfig<>(
                    exception, new Config(millis, TimeUnit.MILLISECONDS, getConfig()));
        }

        public static<E extends Exception> ExceptionConfig<E> custom(Supplier<E> exception) {
            return new ExceptionConfig<>(
                    exception, getConfig());
        }

        /**
         * Will wait 10 seconds until {@link TimeoutException}
         * */
        public static ExceptionConfig<TimeoutException> largeTimeout() {
            return timeout(10, TimeUnit.SECONDS);
        }

        ExceptionConfig<E> copyWith(long duration, TimeUnit unit) {
            return new ExceptionConfig<>(exception, new Config(duration, unit, config));
        }
        ExceptionConfig<E> copyWith(Consumer<Config.Builder> builder) {
            Config.Builder builder1 = new Config.Builder(config);
            builder.accept(builder1);
            return new ExceptionConfig<>(exception, new Config(builder1));
        }
    }

    public static<E extends Exception> void waitIf(
            Supplier<E> exception,
            BooleanSupplier condition,
            Suppliers.OfString cause
    ) throws E {
        waitIf(
                exception,
                getConfig(),
                condition,
                cause
        );
    }

    public static<E extends Exception> void waitIf(
            Supplier<E> exception,
            Config config,
            BooleanSupplier condition,
            Suppliers.OfString cause
    ) throws E {
        if (!condition.getAsBoolean()) return;
        long totalTimeNanos = config.totalNanos;
        long currentNanoTime = System.nanoTime();

        final long end = currentNanoTime + totalTimeNanos;

        final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time
        final double backOffFactor = config.backOffFactor;  // Reasonable growth factor for backoff

        long waitTime = config.initialWaitNanos; // Start with a small initial wait time


        boolean maxReached = false;
        while (
                currentNanoTime < end
        ) {
            if (!condition.getAsBoolean()) return;

            // Calculate remaining time using the currentNanoTime variable
            long remainingTime = end - currentNanoTime;

            // If the wait time exceeds the remaining time, set waitTime to remaining time
            waitTime = Math.min(waitTime, remainingTime);

            LockSupport.parkNanos(waitTime);

            // Increase wait time by backoff factor, but cap it at maxWaitNanos and remaining time
            if (!maxReached) {
                waitTime = Math.min((long)(waitTime * backOffFactor), maxWaitNanos);
                if (waitTime == maxWaitNanos) {
                    maxReached = true;
                }
            }
            currentNanoTime = System.nanoTime();
        }
        if (condition.getAsBoolean()) {
            E e = exception.get();
            String initialCause = "Expired: " + totalTimeNanos + "[nanos]";
            if (cause != null) {
                initialCause = initialCause.concat("\n Cause: " + cause.get());
            }
            e.initCause(
                    new Throwable(initialCause)
            );
            throw e;
        }
    }

    public static<E extends Exception, T> T getUnless(
            Supplier<E> exception,
            Config config,
            Supplier<T> supplier,
            Predicate<T> unless,
            Suppliers.OfString cause
    ) throws E {
        T res;
        if (!unless.test(res = supplier.get())) return res;
        long totalTimeNanos = config.totalNanos;
        long currentNanoTime = System.nanoTime();

        final long end = currentNanoTime + totalTimeNanos;

        final long maxWaitNanos = config.maxWaitNanos;      // Cap max wait to a fraction of total time
        final double backOffFactor = config.backOffFactor;  // Reasonable growth factor for backoff

        long waitTime = config.initialWaitNanos; // Start with a small initial wait time


        boolean maxReached = false;
        while (
                currentNanoTime < end
        ) {
            if (!unless.test(res = supplier.get())) return res;

            // Calculate remaining time using the currentNanoTime variable
            long remainingTime = end - currentNanoTime;

            // If the wait time exceeds the remaining time, set waitTime to remaining time
            waitTime = Math.min(waitTime, remainingTime);

            LockSupport.parkNanos(waitTime);

            // Increase wait time by backoff factor, but cap it at maxWaitNanos and remaining time
            if (!maxReached) {
                waitTime = Math.min((long)(waitTime * backOffFactor), maxWaitNanos);
                if (waitTime == maxWaitNanos) {
                    maxReached = true;
                }
            }
            currentNanoTime = System.nanoTime();
        }
        if (unless.test(res = supplier.get())) {
            E e = exception.get();
            String initialCause = "Expired: " + totalTimeNanos + "[nanos]";
            if (cause != null) {
                initialCause = initialCause.concat("\n Cause: " + cause.get());
            }
            e.initCause(
                    new Throwable(initialCause)
            );
            throw e;
        } else return res;
    }

    public static<E extends Exception> void waitIf(
            ExceptionConfig<E> config,
            BooleanSupplier condition,
            Suppliers.OfString cause
    ) throws E {
        waitIf(
                config.exception, config.config,
                condition, cause
        );
    }

    public static<E extends Exception> void waitIf(
            ExceptionConfig<E> config,
            BooleanSupplier condition
    ) throws E {
        waitIf(
                config.exception, config.config,
                condition, null
        );
    }

    public static<E extends Exception, T> T getUnless(
            ExceptionConfig<E> config,
            Supplier<T> supplier,
            Predicate<T> unless
    ) throws E {
        return getUnless(
                config.exception, config.config, supplier,
                unless, null
        );
    }

}
