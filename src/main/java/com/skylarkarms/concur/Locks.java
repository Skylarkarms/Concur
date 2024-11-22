package com.skylarkarms.concur;

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

    public static final class Valet {
        volatile Thread parking;
        private final AtomicBoolean isShutdown = new AtomicBoolean();

        public Valet() {
        }

        public boolean isShutdown() { return isShutdown.getOpaque(); }

        public boolean isBusy() { return PARKING.getOpaque(this) != null; }
        static final VarHandle PARKING;
        static {
            try {
                PARKING = MethodHandles.lookup().findVarHandle(Valet.class, "parking", Thread.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        /**
         * @see #parkUnpark(long)
         * */
        public final Boolean parkUnpark(long duration, TimeUnit unit) {
            return parkUnpark(unit.toNanos(duration));
        }
        /**
         * @return
         * <ul>
         *     <li>{@code `null`} if this class is {@code `busy`}</li>
         *     <li>{@code `true`} if the park was successful</li>
         *     <li>{@code `false`} if the park was interrupted OR was shutdown()</li>
         * </ul>
         * */
        public final Boolean parkUnpark(long nanos) {
            if (isShutdown.getOpaque()) return false;
            if (nanos < 1) return true;
            Thread curr = Thread.currentThread();
            if (PARKING.compareAndSet(this, null, curr)) {
                long currentNano = System.nanoTime();
                final long end = currentNano + nanos;
                while (currentNano < end) {
                    if (curr == PARKING.getOpaque(this)) {
                        if (!isShutdown.getOpaque()) {
                            LockSupport.parkNanos(end - currentNano);
                            if (curr == PARKING.getOpaque(this)) {
                                currentNano = System.nanoTime();
                            } else return false;
                        } else {
                            PARKING.compareAndSet(this, curr, null);
                            return false;
                        }


                    } else return false;
                }
                return !isShutdown.getOpaque() && PARKING.compareAndSet(this, curr, null);
            }
            return null;
        }

        public final Boolean parkShutdown(long duration, TimeUnit unit) {
            return parkShutdown(unit.toNanos(duration));
        }
        public final Boolean parkShutdown(long nanos) {
            if (isShutdown.getOpaque()) return false;
            if (nanos < 1) return true;
            Thread curr = Thread.currentThread();
            if (PARKING.compareAndSet(this, null, curr)) {
                long currentNano = System.nanoTime();
                final long end = currentNano + nanos;
                while (currentNano < end) {
                    if (curr == PARKING.getOpaque(this)) {
                        if (!isShutdown.getOpaque()) {
                            LockSupport.parkNanos(end - currentNano);
                            if (curr == PARKING.getOpaque(this)) {
                                currentNano = System.nanoTime();

                            } else return false;
                        } else {
                            PARKING.compareAndSet(this, curr, null);
                            return false;
                        }
                    } else return false;
                }
                return isShutdown.compareAndSet(false, true) && PARKING.compareAndSet(this, curr, null);
            }
            return null;
        }

        /**
         * @return {@code `null`} if this class was NOT {@code `busy`}.
         * */
        public final Thread interrupt() {
            Object cur;
            if ((cur = PARKING.getAndSet(this, null)) != null) {
                Thread t = (Thread) cur;
                LockSupport.unpark(t);
                return t;
            }
            return null;
        }

        /**
         * @return
         * <ul>
         *     <li>{@code `null`} If the shutdown was successful, but the class was NOT {@code `busy`}</li>
         *     <li>{@code `true`} If the shutdown was successful, and the class WAS {@code `busy`}</li>
         *     <li>{@code `false`} If the shutdown was NOT successful, because the class was ALREADY shutdown</li>
         * </ul>
         * */
        public final Boolean shutdown() {
            if (isShutdown.compareAndSet(false, true)) {
                Object cur;
                if (
                        (cur = PARKING.getAndSet(this, null)) != null
                ) {
                    Thread t = (Thread) cur;
                    LockSupport.unpark(t);
                    return true;
                } else return null;
            } else return false;
        }
    }

    private static void durationExcep(long duration) {
        if (duration == 0 || duration == Long.MAX_VALUE) throw new IllegalStateException("Not a valid duration");
    }

    private static void unitExcept(TimeUnit unit) {
        if (unit == null) throw new NullPointerException("Unit cannot be null");
    }

    public static void robustPark(long duration, TimeUnit unit) {
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

    public static final class Config {
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

        @Override
        public String toString() {
            return "Config{" +
                    "\n   >> initialWaitFraction=" + initialWaitFraction +
                    ",\n   >> maxWaitFraction=" + maxWaitFraction +
                    ",\n   >> totalNanos=" + totalNanos +
                    ",\n         >> totalNanos=\n" + formatNanos(totalNanos).indent(10) +
                    "   >> initialWaitNanos=" + initialWaitNanos +
                    ",\n   >> maxWaitNanos=" + maxWaitNanos +
                    ",\n   >> backOffFactor=" + backOffFactor
                    + '}';
        }
    }

    static String formatNanos(long nanos) {
        // Break down nanos into seconds, milliseconds, and remaining nanoseconds
        long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos) % 1000;
        long remainingNanos = nanos % 1_000_000;

        // Build the formatted string
        return String.format("%d[seconds]: %03d[millis]: %03d[nanos]", seconds, millis, remainingNanos);
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

        public static ExceptionConfig<RuntimeException> runtime() { return runtime.ref; }

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

        record large_timeout() {
            static final ExceptionConfig<TimeoutException> ref = timeout(10, TimeUnit.SECONDS);
        }

        record large_runtime() {
            static final ExceptionConfig<RuntimeException> ref = runtime(builder -> builder.setDurationUnit(10, TimeUnit.SECONDS));
        }

        /**
         * Will wait 10 seconds until {@link TimeoutException}
         * */
        public static ExceptionConfig<TimeoutException> largeTimeout() {
            return large_timeout.ref;
        }

        /**
         * Will wait 10 seconds until {@link RuntimeException}
         * */
        public static ExceptionConfig<RuntimeException> largeRuntime() {
            return large_runtime.ref;
        }

        ExceptionConfig<E> copyWith(long duration, TimeUnit unit) {
            return new ExceptionConfig<>(exception, new Config(duration, unit, config));
        }
        ExceptionConfig<E> copyWith(Consumer<Config.Builder> builder) {
            Config.Builder builder1 = new Config.Builder(config);
            builder.accept(builder1);
            return new ExceptionConfig<>(exception, new Config(builder1));
        }

        @Override
        public String toString() {
            return "ExceptionConfig{" +
                    "\n    >> exception=" + exception +
                    ",\n    >> config=\n" + config.toString().indent(3) +
                    '}';
        }
    }

    public static<E extends Exception> void waitIf(
            Supplier<E> exception,
            BooleanSupplier condition,
            Supplier<String> cause
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
            Supplier<String> cause
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
            Supplier<String> cause
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
            Supplier<String> cause
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
