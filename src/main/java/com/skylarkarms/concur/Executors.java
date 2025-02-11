package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class Executors {

    private static final String handler_tag = "concurrents.Executor.AUTO_EXIT_HANDLER";
    public static Thread.UncaughtExceptionHandler AUTO_EXIT_HANDLER(Consumer<String> printer) {
        return printer != null ?
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread thread, Throwable throwable) {
                        throwable.printStackTrace(System.err);
                        printer.accept(throwable.getMessage());
                        thread.interrupt();
                        System.exit(0);
                    }

                    @Override
                    public String toString() { return handler_tag;
                    }
                } :
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace(System.err);
                        t.interrupt();
                        System.exit(0);
                    }

                    @Override
                    public String toString() { return handler_tag
                            .concat("@").concat(Integer.toString(hashCode()));
                    }
                };
    }

    /**
     * @return An {@link java.lang.Thread.UncaughtExceptionHandler} instance of logic:
     * <pre>{@code
     *   public void uncaughtException(Thread t, Throwable e) {
     *      e.printStackTrace(System.err);
     *      t.interrupt();
     *      System.exit(0);
     *   }
     * }</pre>

     * */
    public static Thread.UncaughtExceptionHandler auto_exit_handler() {return auto_exit_handler.ref; }
    private record auto_exit_handler() {
        static final Thread.UncaughtExceptionHandler ref
                = AUTO_EXIT_HANDLER(null);
    }

    private static final String
            max_hand_tag = "skylarkarms.concurrents.Executors.SystemThreadFactory#"
            ;
    public static final ThreadFactory cleanFactory(int priority, Thread.UncaughtExceptionHandler handler) {
        return cleanFactory(null, priority, handler);
    }
    public static final ThreadFactory cleanFactory(Thread.UncaughtExceptionHandler handler) {
        return cleanFactory(null, Thread.NORM_PRIORITY, handler);
    }
    private static ThreadFactoryBuilder mFactory = Executors::cleanFactory;
    private static volatile boolean fact_init;

    public static void setSystemFactory(ThreadFactoryBuilder systemFactory) {
        if (fact_init) throw new IllegalStateException("ThreadFactory already initialized.");
        if (systemFactory == null) throw new IllegalStateException("Parameter cannot be null.");
        mFactory = systemFactory;
    }

    @FunctionalInterface
    public interface ThreadFactoryBuilder {
        ThreadFactory build(ThreadGroup group, int priority, Thread.UncaughtExceptionHandler handler);
    }

    public static final ThreadFactory cleanFactory(ThreadGroup group, int priority, Thread.UncaughtExceptionHandler handler) {
        return new ThreadFactory() {
            final String tag = max_hand_tag.concat(prio(priority));
            final ThreadFactory finalFact =
                    group == null ?
                            r -> new Thread(r, tag) {
                                @Override
                                public String toString() {
                                    return super.toString().concat("@").concat(Integer.toString(hashCode()));
                                }
                            }
                            :
                            r -> new Thread(group, r, tag) {
                                @Override
                                public String toString() {
                                    return super.toString().concat("@").concat(Integer.toString(hashCode()));
                                }
                            };

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = finalFact.newThread(r);
                t.setPriority(priority);
                t.setUncaughtExceptionHandler(handler);
                return t;
            }

            static String prio(int priority) {
                return switch (priority) {
                    case Thread.MAX_PRIORITY -> "MAX_PRIO";
                    case Thread.NORM_PRIORITY -> "NORM_PRIO";
                    case Thread.MIN_PRIORITY -> "MIN_PRIO";
                    default -> "Priority: ".concat(Integer.toString(priority));
                };
            }

            @Override
            public String toString() {
                return tag +
                        ",\n Handler = " + handler
                        + "\n }";
            }
        };
    }

    private static final ThreadFactory sysFactory(ThreadGroup group, int priority, Thread.UncaughtExceptionHandler handler) {
        fact_init = true;
        return mFactory.build(group, priority, handler);
    }

    private static final ThreadFactory sysFactory(int priority, Thread.UncaughtExceptionHandler handler) {
        return sysFactory(null, priority, handler);
    }

    public static ThreadFactory factory() {return NORM_PRIOR.ref;}
    public static ThreadFactory factory(int priority) {
        return switch (priority) {
            case Thread.NORM_PRIORITY -> NORM_PRIOR.ref;
            case Thread.MAX_PRIORITY -> MAX_PRIOR.ref;
            case Thread.MIN_PRIORITY -> MIN_PRIOR.ref;
            default -> sysFactory(priority, auto_exit_handler());
        };
    }
    /**
     * @return {@link ThreadFactory} instance with {@link #auto_exit_handler()} as {@link java.lang.Thread.UncaughtExceptionHandler}
     * */
    public static ThreadFactory cleanFactory(int priority) {
        return cleanFactory(priority, auto_exit_handler());
    }
    private record MAX_PRIOR() {static final ThreadFactory ref
            = sysFactory(Thread.MAX_PRIORITY, auto_exit_handler.ref);
    }

    private record NORM_PRIOR() {static final ThreadFactory ref
            = sysFactory(Thread.NORM_PRIORITY, auto_exit_handler.ref);
    }

    private record MIN_PRIOR() {static final ThreadFactory ref
            = sysFactory(Thread.MIN_PRIORITY, auto_exit_handler.ref);
    }

    private static final String
            executor_tag = "skylarkarms.concurrents.Executors.Executor#UNBRIDLED."
            + "\n [ThreadFactory = ";
    public static Executor UNBRIDLED(
            ThreadFactory factory
    ) {
        return new Executor() {
            @Override
            public void execute(Runnable command) { factory.newThread(command).start(); }

            @Override
            public String toString() {
                return executor_tag.concat(factory.toString())
                        .concat("]");
            }
        };
    }

    /**
     * A limitless/pool-less Executor that delivers unbridled {@link Thread}'s
     * from {@link UNBRIDLED_NORM#ref} factory
     * with an {@link Executors#auto_exit_handler} exception handler
     * */
    public static Executor UNBRIDLED() {return UNBRIDLED_NORM.ref;}

    public static Executor UNBRIDLED(int priority) {
        return switch (priority) {
            case Thread.MAX_PRIORITY -> UNBRIDLED_MAX.ref;
            case Thread.NORM_PRIORITY -> UNBRIDLED_NORM.ref;
            case Thread.MIN_PRIORITY -> UNBRIDLED_MIN.ref;
            default -> UNBRIDLED(sysFactory(priority, auto_exit_handler()));
        };
    }

    private record UNBRIDLED_MAX() {
        static final Executor ref = UNBRIDLED(MAX_PRIOR.ref);
    }

    private record UNBRIDLED_NORM() {
        static final Executor ref = UNBRIDLED(NORM_PRIOR.ref);
    }

    private record UNBRIDLED_MIN() {
        static final Executor ref = UNBRIDLED(MIN_PRIOR.ref);
    }

    /**
     * @return a {@link ContentiousExecutor}.
     * */
    public static ContentiousExecutor getContentious(Executor executor) { return new ContentiousExecutor(executor); }

    public static Delayer getDelayer(Executor executor, long duration, TimeUnit unit) {
        return new Delayer(executor, duration, unit);
    }

    /**
     * Delivers a {@link Delayer} with a single Thread pool,
     * <p> with {@link ThreadFactory} defined at {@link java.util.concurrent.Executors#defaultThreadFactory()}
     * */
    public static Delayer getSingleExecDelayer(long duration, TimeUnit unit) {
        return new Delayer(
                java.util.concurrent.Executors.newSingleThreadExecutor()
                , duration, unit);
    }

    @FunctionalInterface
    public interface BaseExecutor extends Executor {
        boolean onExecute(Runnable command);

        @Override
        default void execute(Runnable command) { onExecute(command); }
    }


    /**
     * This class is designed to schedule commands after a specified time frame.
     * <p> It operates by setting a command via the {@link #execute(Runnable)} method.
     * <p> The class then attempts to execute this command until either a new command is set or the time limit, defined in the constructor, is reached.
     * <p> Every command is subject to a waiting period that lasts for the duration specified in the constructor.
     * <p> If a new command is set via the {@link #execute(Runnable)} method while the current Runnable is still processing,
     * <p> and if the executor has only a single core defined in its thread pool, both commands will share the same thread context.
     * <p> This implies that if one command uses the {@link Thread#sleep(long)} method, the execution of the subsequent command may be influenced by
     * <p> the waiting time of the previous command.
     * */
    public static class Delayer
            implements BaseExecutor
    {
        final long nano;
        final Executor executor;
        final Locks.Valet valet = new Locks.Valet();
        final Runnable lockable;

        volatile Runnable core;

        public boolean isWaiting(){
            return valet.isBusy();
        }

        public Delayer(Executor executor, long duration, TimeUnit unit) {
            this(executor, unit.toNanos(duration));
        }

        public Delayer(long duration, TimeUnit unit) {
            this(UNBRIDLED_NORM.ref, unit.toNanos(duration));
        }

        public Delayer(long nanos) {
            this(UNBRIDLED_NORM.ref, nanos);
        }

        public Delayer(Executor executor, long nano) {
            this.executor = executor;
            this.nano = nano;
            this.lockable = () -> {
                Boolean res =  valet.parkUnpark(nano);
                if (res != null) {
                    Runnable r = core;
                    if (res) {
                        if (r != null) r.run();
                        //else may have been interrupted...
                    } else {
                        // interruption SUCCESSFUL
                        // core may have been swapped.
                        res =  valet.parkUnpark(nano);
                        while (
                                res != null
                                        && !res
                        ) {
                            res = valet.parkUnpark(nano);
                        }
                        r = core;
                        if (res != null && r != null) r.run();
                        // else valte was busy... on a different Thread...

                    }
                }
            };
        }

        public boolean interrupt() {
            core = null;
            Thread cur = valet.interrupt();
            if (cur != null) {
                cur.interrupt();
                return true;
            } else return false;
        }

        @Override
        public boolean onExecute(Runnable command) {
            core = command;
            Thread cur = valet.interrupt();
            // successful interruption is NOT NULL
            // if NOT NULL... DO NOT EXECUTE
            if (cur == null) { // if NOT BUSY
                executor.execute(
                        lockable
                );
                return true;
            }
            // else the interruption WAS successful
            cur.interrupt();
            return false;
        }

        @Override
        public String toString() {
            String hash = Integer.toString(hashCode());
            return "Delayer@".concat(hash).concat("{" +
                    ("\n >>> nanoTime=" + nano +
                            "\n    - (millis)=" + Duration.ofNanos(nano).toMillis() +
                            ",\n >>> core runnable (Runnable) =" + core
                            + ",\n >>> executor =\n" + executor.toString().indent(3)).indent(3)
                    + "}@").concat(hash);
        }

        public static void oneShot(
                Executor executor,
                long duration, TimeUnit unit, Runnable runnable) {
            Locks.durationExcep(duration);
            executor.execute(
                    () -> {
                        long currentNano = System.nanoTime();
                        final long end = currentNano + unit.toNanos(duration);
                        while (currentNano < end) {
                            LockSupport.parkNanos(end - currentNano);
                            currentNano = System.nanoTime();
                        }
                        runnable.run();
                    }
            );
        }

        record ONE_SHOT() {
            static final ThreadFactory ref = cleanFactory(Thread.NORM_PRIORITY);
        }

        /**
         * Uses a {@link ThreadFactory} instance of characteristics:
         * <ul>
         *     <li>{@link Thread#getPriority()} = {@link Thread#NORM_PRIORITY}</li>
         *     <li>{@link java.lang.Thread.UncaughtExceptionHandler} = {@link #auto_exit_handler()}</li>
         * </ul>
         * */
        public static void oneShot(
                long duration, TimeUnit unit, Runnable runnable) {
            Locks.durationExcep(duration);
            final long nanos = unit.toNanos(duration);
            ONE_SHOT.ref.newThread(
                    () -> {
                        long currentNano = System.nanoTime();
                        final long end = currentNano + nanos;
                        while (currentNano < end) {
                            LockSupport.parkNanos(end - currentNano);
                            currentNano = System.nanoTime();
                        }
                        runnable.run();
                    }
            ).start();
        }
    }

    /**
     * An Executor wrapper that will relieve back-pressure, by executing the last command consumed by this class.
     * <p> The period of relief will be the timeframe between a call
     * <p> to {@link Executor#execute(Runnable)} and a call to {@link Runnable#run()} within the executor.
     * <p> If more commands are received while the Executor is processing, a re-evaluation will infer whether a
     * <p> new processing needs to be performed in the same Thread, or if the Thread has been abandoned already...
     * <p> then a new execution needs to be performed.
     * */
    public static class ContentiousExecutor
            implements BaseExecutor {
        private final Executor executor;
        private final AtomicBoolean isActive = new AtomicBoolean();
        private volatile Runnable commandRef;
        private static final VarHandle COMMAND_REF;
        private final Runnable executable = () -> {
            Runnable r = commandRef;
            while (isActive.get()) {
                r.run();
                //Is active must be false, IF runned was same as get.
                isActive.set(r != (r = (Runnable) COMMAND_REF.getOpaque(this)));
            }
        };
        static {
            try {
                COMMAND_REF = MethodHandles.lookup().findVarHandle(
                        ContentiousExecutor.class, "commandRef",
                        Runnable.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        public ContentiousExecutor(Executor executor) {
            this.executor = executor;
        }
        public ContentiousExecutor() {
            this(UNBRIDLED());
        }
        /**
         * @return true, if this command triggers <p>
         * the creation of a new thread.
         * */
        @Override
        public boolean onExecute(Runnable command) {
            commandRef = command;
            if (isActive.compareAndSet(false, true)) {
                executor.execute(executable);
                return true;
            }
            //Here a command may be lost... so we retry setting it to true.
            // If succeeded, the inner do while will catch up on it.
            return false;
        }

        @Override
        public String toString() {
            String hash = Integer.toString(hashCode());
            return "ContentiousExecutor@".concat(hash).concat("{" +
                    ("\n >>>isActive=" + isActive.getOpaque() +
                    ",\n >>> commandRef (mutable)=" + commandRef +
                    ",\n >>> (main) executable=" + executable +
                    "\n >>> executor =\n" + executor.toString().indent(3)).indent(3)
                    + "}@").concat(hash);
        }
    }

    public static class ThrowableExecutor
            extends ThreadPoolExecutor {

        public ThrowableExecutor(
                int corePoolSize,
                boolean preestartCore,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue,
                ThreadFactory threadFactory
        ) {
            super(
                    corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory
            );
            if (preestartCore) {
                prestartAllCoreThreads();
            }
        }

        public ThrowableExecutor(
                int corePoolSize, boolean preestartCores, int maxPoolSize, long keepAliveTime, TimeUnit unit) {
            this(
                    corePoolSize, preestartCores, maxPoolSize, keepAliveTime, unit,
                    factory(Thread.NORM_PRIORITY)
            );
        }

        public ThrowableExecutor(
                int corePoolSize, boolean preestartCores, int maxPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory factory) {
            this(
                    corePoolSize, preestartCores, maxPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>(), factory);
        }
        public ThrowableExecutor(
                int nThreads) {
            this(
                    nThreads, false,
                    nThreads,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), factory());
        }
    }

    /**
     * This {@link BooleanSupplier} action is designed for preemptively escaping a process (backpressure drop).
     * The most common type of confirmation is the double-checking strategy done by
     * opaque-like or volatile-like loads from/to memory.
     *
     * <p> A double check result can either return:
     * <ul>
     *     <li>
     *         {@code `true`} Execution escapes. Ths is the faster option,
     *         but more memory expensive as it ends Thread utilization prematurely.
     *     </li>
     *     <li>
     *         {@code `false`} Execution retries. This option will try to reuse the Thread once again.
     *         <p> {@code `true`} should be the de-facto choice of returning on the very last line of the {@link BooleanSupplier} action, this allows us to simplify the code on the {@link #execute()} side
     *         <p> since it does not need to spin-wait for potential missing calls.
     *     </li>
     * </ul>
     * <p> The class constructor will throw a {@link RuntimeException} if deadlocked on a {@code `false`} result
     * */
    public static class ScopedExecutor
    {
        private final Executor executor;

        private final Runnable sysAction;

        private final AtomicInteger ticket = new AtomicInteger();

        volatile Exec execution = Exec.first;
        private static final VarHandle EXEC;
        public
        static class Exec{
            final int ticket; public final boolean finished;

            static final Exec first = new Exec(0, true);

            Exec(int ticket, boolean finished) {
                this.ticket = ticket;
                this.finished = finished;
            }

            @Override
            public String toString() {
                return "Exec{" +
                        ("\n > ticket = " + ticket +
                        ",\n > finished = " + finished).indent(3)
                        + "}@" + hashCode();
            }
        }

        static {
            try {
                EXEC = MethodHandles.lookup().findVarHandle(ScopedExecutor.class, "execution", Exec.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        static final long
                baseWaitingNanos = Duration.ofNanos(90).toNanos();

        /**
         * @see ScopedExecutor
         * */
        public ScopedExecutor(
                Executor executor,
                BooleanSupplier action
        ) {
            this.executor = executor;
            sysAction = () -> {
                int curr = ticket.getOpaque();
                Object prev = EXEC.getOpaque(this);
                ScopedExecutor.Exec newE;
                int pass = 1;
                long localWaitNanos = baseWaitingNanos;
                while (true) {
                    do {
                            newE = new ScopedExecutor.Exec(curr, true);
                    } while (
                            curr != (curr = ticket.getOpaque())
                            ||
                            !action.getAsBoolean()
                    );
                    if (
                            curr == ticket.getOpaque()
                    ) {
                        boolean done = EXEC.compareAndSet(this, prev, newE);
                        assert done;
                        break;
                    } else {
                        pass++;
                        if (pass < 21) {
                            // short sleep and repeat....
                            long currentNano = System.nanoTime();
                            final long end = currentNano + localWaitNanos;
                            while (currentNano < end) {
                                LockSupport.parkNanos(end - currentNano);
                                currentNano = System.nanoTime();
                            }

                            localWaitNanos *= 2;
                            curr = ticket.getOpaque();
                        } else {
                            throw new RuntimeException("Process timed out while executing the  task."
                                    + "\n t = " + baseWaitingNanos * (long)(Math.pow(2, pass) - 1) + " nanos"
                                    + "\n The cause may be that the action [" + action + "] is stuck on a deadlock by always returning `false`."
                            );
                        }
                    }
                }
            };
        }

        public boolean isIdle() { return execution.finished; }

        public boolean isBusy() { return !execution.finished; }

        public boolean execute() {
            int newT = ticket.incrementAndGet();
            ScopedExecutor.Exec prev = (Exec) EXEC.getOpaque(this)
                    , next, wit;

            if (
                    prev.finished
                    &&
                            newT > prev.ticket
                            &&
                            newT == ticket.getOpaque()
            ) {
                next = new ScopedExecutor.Exec(prev.ticket, false);
                if (
                        prev == (wit = (ScopedExecutor.Exec) EXEC.compareAndExchange(this, prev, next))) {
                    executor.execute(sysAction);
                    return true;
                } else if (
                        wit != null
                                &&
                                wit.ticket > newT
                ) return false;
                else return false;
            }
            return false;
        }

        @Override
        public String toString() {
            String hash = Integer.toString(hashCode());
            return "ScopedExecutor@".concat(hash).concat("{" +
                    "\n >>> sysAction=" + sysAction +
                    ",\n >>> ticket=" + ticket.getOpaque() +
                    ",\n >>> execution=\n" + execution.toString().concat(",").indent(3) +
                    " >>> executor=\n" + executor.toString().indent(3) +
                    "}@").concat(hash);
        }
    }


    public interface Activator {
        boolean start();
        boolean stop();
        boolean isActive();
        Activator default_activator = new Activator() {
            @Override
            public boolean start() {
                return false;
            }

            @Override
            public boolean stop() {
                return false;
            }

            @Override
            public boolean isActive() {
                return false;
            }
        };
    }

    public static final class FixedScheduler implements Activator {
        final ThreadFactory factory;
        final ScheduleParams params;
        final Runnable command;

        public record ScheduleParams(
                long initital_delay, long delay, TimeUnit unit, int repetitions
        ){
            record NO_SCHEDULE() {
                static final ScheduleParams ref = new ScheduleParams(0, 0, TimeUnit.SECONDS, 0);
            }
            public static final ScheduleParams NO_SCHEDULE() {
                return NO_SCHEDULE.ref;
            }
            record TEN_SEC() {
                static final ScheduleParams ref = new ScheduleParams(10, 0, TimeUnit.SECONDS, 0);
            }
            record FIFTEEN_SEC() {
                static final ScheduleParams ref = new ScheduleParams(15, 0, TimeUnit.SECONDS, 0);
            }
            /**
             * @return a 10 second Scheduled scan of the {@link FixedScheduler}
             * */
            public static final ScheduleParams TEN_SEC() {
                return TEN_SEC.ref;
            }
            /**
             * @return a 15 second Scheduled scan of the {@link FixedScheduler}
             * */
            public static final ScheduleParams FIFTEEN_SEC() {
                return FIFTEEN_SEC.ref;
            }
            record BASE_PERIODIC() {
                static final ScheduleParams ref = new ScheduleParams(3, 2, TimeUnit.SECONDS, 5);
            }
            /**
             * @return A ScheduledParams with the given configuration:
             * <ul>
             *     <li> {@link TimeUnit} = {@link TimeUnit#SECONDS} </li>
             *     <li> {@link ScheduleParams#initital_delay} = 3 </li>
             *     <li> {@link ScheduleParams#delay} = 2 </li>
             *     <li> {@link ScheduleParams#repetitions} = 5 </li>
             * </ul>
             * */
            public static final ScheduleParams BASE_PERIODIC() { return BASE_PERIODIC.ref; }

            public static final ScheduleParams delayed(long duration, TimeUnit unit) {
                return new ScheduleParams(duration, 0, unit, 0);
            }

            /**
             * @return A ScheduledParams with the given configuration:
             * <ul>
             *     <li> {@link TimeUnit} = {@link TimeUnit#SECONDS} </li>
             *     <li> {@link ScheduleParams#initital_delay} = 3 </li>
             *     <li> {@link ScheduleParams#delay} = 2 </li>
             * </ul>
             * */
            public static final ScheduleParams periodic(int repetitions) {
                return new ScheduleParams(3, 2, TimeUnit.SECONDS, repetitions);
            }

            @Override
            public String toString() {
                return "ScheduleParams{" +
                        "\n  >> initital_delay=" + initital_delay +
                        ",\n  >> delay=" + delay +
                        ",\n  >> unit=" + unit +
                        ",\n  >> repetitions=" + repetitions +
                        "\n }";
            }
        }

        AtomicBoolean started = new AtomicBoolean();
        final Locks.Valet valet;
        final long nanos, initialNanos;
        final int reps;

        public ScheduleParams getParams() {
            return params;
        }

        public FixedScheduler(
                ThreadFactory factory,
                ScheduleParams params,
                Runnable command
        ) {
            this.factory = factory;
            this.params = params;
            this.command = command;
            if (params.delay == 0 && params.repetitions != 0)
                throw new IllegalStateException("""
                        Repetitions are not allowed, when a `0` delay value is passed.
                         0 delay, means 0 repetitions.
                        """);
            valet = new Locks.Valet();
            TimeUnit unit = params.unit;
            initialNanos = unit.toNanos(params.initital_delay);
            nanos = unit.toNanos(params.delay);
            reps = params.repetitions;
        }

        public FixedScheduler(
                ScheduleParams params,
                Runnable command
        ) {
            this(Executors.factory(Thread.NORM_PRIORITY), params, command);
        }

        @Override
        public boolean isActive() {
            return started.getOpaque();
        }

        @Override
        public boolean stop() {
            if (started.compareAndSet(true, false)) {
                valet.interrupt();
                return true;
            }
            return false;
        }
        @Override
        public boolean start() {
            if (started.compareAndSet(false, true)) {
                factory.newThread(
                        () -> {
                            if (started.getOpaque()) {
                                Boolean parked = valet.parkUnpark(initialNanos);
                                assert parked != null;
                                if (!parked) {
                                    return;
                                }
                            } else return;
                            boolean broken = false;
                            for (int i = 0; i < reps; i++) {
                                if (!(broken = !started.getOpaque())) {
                                    command.run();
                                    Boolean parked = valet.parkUnpark(nanos);
                                    assert parked != null;
                                    if (!parked) {
                                        broken = true;
                                        break;
                                    }
                                } else break;
                            }
                            if (broken) return;
                            command.run();
                            started.set(false);
                        }
                ).start();
                return true;
            }
            return false;
        }

        public static<T> FixedScheduler generator(
                ThreadFactory factory,
                ScheduleParams params,
                Supplier<T> supplier,
                Consumer<T> consumer
        ) {
            return new FixedScheduler(
                    factory,
                    params,
                    () -> consumer.accept(supplier.get())
            );
        }

        public static<T> FixedScheduler generator(
                ScheduleParams params,
                Supplier<T> supplier,
                Consumer<T> consumer
        ) {
            return new FixedScheduler(
                    cleanFactory(Thread.NORM_PRIORITY),
                    params,
                    () -> consumer.accept(supplier.get())
            );
        }

        @Override
        public String toString() {
            return "FixedScheduler{" +
                    ("\n factory=\n" + factory.toString().indent(3) +
                            ",\n >> params=\n" + params.toString().indent(3) +
                            " >> command=" + command +
                            ",\n >> started=" + started +
                            ",\n valet=\n" + valet.toString().indent(3)).indent(3)
                    + "}@".concat(Integer.toString(hashCode()));
        }
    }
}