package com.skylarkarms.concur;

import com.skylarkarms.lambdas.Consumers;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

public final class Executors {

    private static final String handler_tag = "Skylarkarms.Concurrents.Executor.AUTO_EXIT_HANDLER";
    public static Thread.UncaughtExceptionHandler AUTO_EXIT_HANDLER(Consumers.OfString printer) {
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
                    public String toString() { return handler_tag; }
                } :
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace(System.err);
                        t.interrupt();
                        System.exit(0);
                    }

                    @Override
                    public String toString() { return handler_tag; }
                };
    }

    public static Thread.UncaughtExceptionHandler auto_exit_handler() {return auto_exit_handler.ref; }
    private record auto_exit_handler() {
        static final Thread.UncaughtExceptionHandler ref
                = AUTO_EXIT_HANDLER(null);
    }

    private static final String
            max_hand_tag = "Skylarkarms.Concurrents.com.skylarkarms.concurrents.Executors.ThreadFactory#MAX_PRIOR. " +
            "\n [handler =";
    public static ThreadFactory factory(int priority, Thread.UncaughtExceptionHandler handler) {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread t = new Thread(runnable, max_hand_tag);
                t.setPriority(priority);
                t.setUncaughtExceptionHandler(handler);
                return t;
            }

            @Override
            public String toString() { return max_hand_tag.concat(handler.toString()).concat("]"); }
        };
    }

    public static ThreadFactory MAX_PRIOR() {return MAX_PRIOR.ref;}
    private record MAX_PRIOR() {static final ThreadFactory ref
            = factory(Thread.MAX_PRIORITY, auto_exit_handler.ref);
    }

    private static final String
            executor_tag = "Skylarkarms.Concurrents.com.skylarkarms.concurrents.Executors.Executor#UNBRIDLED."
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
     * from {@link MAX_PRIOR#ref} factory
     * with an {@link Executors#auto_exit_handler} exception handler
     * */
    public static Executor UNBRIDLED() {return UNBRIDLED.ref;}
    private record UNBRIDLED() {
        static final Executor ref = UNBRIDLED(MAX_PRIOR.ref);
    }

    /**
     * @return a {@link ExecutorDelayer.ContentiousExecutor}.
     * */
    public static ExecutorDelayer.ContentiousExecutor getContentious(Executor executor) {
        return new ExecutorDelayer.ContentiousExecutor(executor);
    }

    public static ExecutorDelayer.Delayer getDelayer(Executor executor, TimeUnit unit, long duration) {
        return new ExecutorDelayer.Delayer(executor, unit, duration);
    }

    /**
     * Delivers a {@link ExecutorDelayer.Delayer} with a single Thread pool,
     * <p> with {@link ThreadFactory} defined at {@link java.util.concurrent.Executors#defaultThreadFactory()}
     * */
    public static ExecutorDelayer.Delayer getSingleExecDelayer(TimeUnit unit, long duration) {
        return new ExecutorDelayer.Delayer(
                java.util.concurrent.Executors.newSingleThreadExecutor()
                , unit, duration);
    }

    public static abstract class ExecutorDelayer extends BaseExecutor {
        protected ExecutorDelayer(Executor executor) { super(executor); }

        /**
         * @return true on a success execute.
         * */
        public abstract boolean onExecute(Runnable command);

        @Override
        public final void execute(Runnable command) { onExecute(command); }

        final void sysOnExecute(Runnable command) { super.execute(command); }

        public boolean interrupt() {
            throw new IllegalStateException("Implemented when a duration longer than 0 is specified.");
        }

        public boolean isDelayer() { return false; }

        public static ExecutorDelayer getExecutorDelayer(Executor executor, TimeUnit unit, long duration) {
            if (duration > 0) return new Delayer(executor, unit, duration);
            else return new ContentiousExecutor(executor);
        }

        public static Delayer getDelayer(Executor executor, TimeUnit unit, long duration) {
            assert duration != 0 : "Use ContentiousExecutor instead.";
            return new Delayer(executor, unit, duration);
        }

        public static ContentiousExecutor getContentious(Executor executor) {
            return new ContentiousExecutor(executor);
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
                extends ExecutorDelayer
        {
            final long nanoTime;
            volatile Leader leader;

            /**
             * @return true if it is still waiting.
             * <p> false if the waiting period resumed AND:
             * <ul>
             *     <li>
             *         The Runnable command is processing OR
             *     </li>
             *     <li>
             *         The Runnable command has finished processing
             *     </li>
             * </ul>
             * */
            public boolean isWaiting() { return leader.isWaiting(); }

            Runnable command;
            static final VarHandle LEADER, COMMAND;

            static {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                try {
                    COMMAND = lookup.findVarHandle(
                            Delayer.class, "command",
                            Runnable.class);
                    LEADER = lookup.findVarHandle(Delayer.class, "leader", Leader.class);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            final Runnable valet = () -> {
                Leader current = new Leader();
                assert leader == null;
                LEADER.setOpaque(this, current);
                Runnable early = (Runnable) COMMAND.getOpaque(this);
                //Prevents context switching spurious failures.
                //On first pass, the Thread may still be busy with a previous task, including this task.
                if (current.firstPass() && COMMAND.compareAndSet(this, early, null)) {
                    leader = null;
                    VarHandle.storeStoreFence();
                    early.run();
                } else {
                    while (
                            (early = (Runnable) COMMAND.getOpaque(this)) != null
                    ) {
                        if (
                                current.normalPass() &&
                                        COMMAND.compareAndSet(this,
                                                early,
                                                null)
                        ) {
                            leader = null;
                            VarHandle.storeStoreFence();
                            early.run();
                            break;
                        }
                    }
                    leader = null;
                }
            };

            Delayer(Executor executor, TimeUnit unit, long duration) { this(executor, unit.toNanos(duration)); }

            Delayer(Executor executor, long nanoTime) {
                super(executor);
                this.nanoTime = nanoTime;
                if (nanoTime == 0) throw new IllegalStateException("Time cannot be 0");
            }

            @Override
            public boolean isDelayer() { return true; }

            final class Leader {
                final Thread lead;

                Leader() { lead = Thread.currentThread(); }

                private static int ver = 0;

                final int version = ver++;

                private static final int
                        open = 0,
                        interrupted = 1,
                        waiting = 2;
                final String valueOf(int val) {
                    return switch (val) {
                        case open -> "open";
                        case interrupted -> "interrupted";
                        case waiting -> "waiting";
                        default -> throw new IllegalStateException("Unexpected value: " + val);
                    } + "\n version = " + version +
                            ",\n @" + hashCode() + "}";
                }

                volatile int state = open;
                static final VarHandle STATE;
                static {
                    try {
                        STATE = MethodHandles.lookup().findVarHandle(Leader.class, "state", int.class);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new ExceptionInInitializerError(e);
                    }
                }

                boolean isWaiting() { return waiting == (int)STATE.getOpaque(this); }

                /**
                 * @return true if properly closed, false if interrupted
                 * */
                final boolean normalPass() {
                    if (interrupted == (int) STATE.getAndSet(this, waiting)) {
                        LockSupport.parkNanos(nanoTime);
                    }
                    LockSupport.parkNanos(this, nanoTime);
                    return waiting == (int) STATE.getAndSet(this, open);
                }
                /*
                 * A spurious wakeup happens if the Threadpool sends a previously
                 * unparked Thread which happened a couple of nanos before a parkNanos.
                 * One reason may be that, because the Thread is nulled before the Runnable has finished processing,
                 * a new process may start while the same Thread is still processing.
                 * So there may be 2 instances of the same Thread simultaneously.
                 * */
                final boolean firstPass() {
                    if (interrupted == (int) STATE.getAndSet(this, waiting)) {
                        LockSupport.parkNanos(nanoTime);
                    }
                    long prev = System.nanoTime();
                    LockSupport.parkNanos(this, nanoTime);
                    long span = System.nanoTime() - prev;
                    final int res;
                    if (waiting ==
                            (res = (int) STATE.getAndSet(this, open))
                            && span < nanoTime) {
                        LockSupport.parkNanos(this, nanoTime);
                    }
                    return res == waiting;
                }
                final boolean unpark() {
                    if (
                            STATE.compareAndSet(this, waiting, interrupted)
                    ) {
                        LockSupport.unpark(lead);
                        return true;
                    }
                    return false;
                }

                final void sysUnpark() {
                    if (
                            STATE.compareAndSet(this, waiting, interrupted)
                    ) {
                        LockSupport.unpark(lead);
                        VarHandle.fullFence();
                    }
                }

                @Override
                public String toString() {
                    return "Leader{" +
                            "lead=" + lead +
                            "\n, state=" + valueOf((int)STATE.getOpaque(this));
                }
            }

            @Override
            public boolean onExecute(Runnable command) {
                //true if an execution is already processing
                if (COMMAND.getAndSet(this, command) != null) {
                    Leader current;
                    if ((current = (Leader) LEADER.getOpaque(this)) != null) {
                        current.sysUnpark();
                    }
                    return false;
                } else {
                    super.sysOnExecute(valet);
                    return true;
                }
            }

            @Override
            public boolean interrupt() {
                Leader current;
                if ((current = (Leader) LEADER.getOpaque(this)) != null) {
                    return current.unpark();
                }
                return false;
            }

            public static void oneShot(
                    Executor executor,
                    long duration, TimeUnit unit, Runnable runnable) {
                executor.execute(
                        () -> {
                            LockSupport.parkNanos(unit.toNanos(duration));
                            runnable.run();
                        }
                );
            }
            /**
             * Uses {@link #UNBRIDLED} executor as default Executor
             * */
            public static void oneShot(
                    long duration, TimeUnit unit, Runnable runnable) {
                UNBRIDLED.ref.execute(
                        () -> {
                            LockSupport.parkNanos(unit.toNanos(duration));
                            runnable.run();
                        }
                );
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
                extends ExecutorDelayer {
            private final AtomicBoolean isActive = new AtomicBoolean();
            private volatile Runnable commandRef;
            private static final VarHandle COMMAND_REF;
            private final Runnable executor = () -> {
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
            ContentiousExecutor(Executor executor) { super(executor); }
            /**
             * @return true, if this command triggers <p>
             * the creation of a new thread.
             * */
            @Override
            public boolean onExecute(Runnable command) {
                commandRef = command;
                if (isActive.compareAndSet(false, true)) {
                    super.sysOnExecute(executor);
                    return true;
                }
                //Here a command may be lost... so we retry setting it to true.
                // If succeeded, the inner do while will catch up on it.
                return false;
            }
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
                    factory(Thread.NORM_PRIORITY, auto_exit_handler.ref)
            );
        }

        public ThrowableExecutor(
                int corePoolSize, boolean preestartCores, int maxPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory factory) {
            this(
                    corePoolSize, preestartCores, maxPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>(), factory);
        }
    }

    static abstract class BaseExecutor implements Executor {
        private final Executor executor;

        protected BaseExecutor(Executor executor) {
            this.executor = executor;
            assert executor != null : "Executor shouldn't be null.";
        }

        @Override
        public void execute(Runnable command) { executor.execute(command); }
    }
    @FunctionalInterface
    public interface RetryExecutor {
        /**@return true will retry, a false will attempt to end*/
        boolean execute();

        static RetryExecutor get(
                Executor executor,
                BooleanSupplier action
        ) {
            return new RetryExecutorImpl(executor, action);
        }
    }

    /**returning a true will retry, a false will attempt to end*/
    static class RetryExecutorImpl extends BaseExecutor implements RetryExecutor {
        private final Runnable sysAction;

        /*volatile */VersionedObject gate = VersionedObject.OPEN;
        private static final VarHandle GATE;
        static {
            try {
                GATE = MethodHandles.lookup().findVarHandle(RetryExecutorImpl.class, "gate", VersionedObject.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @Override
        public String toString() {
            return "RetryExecutorImplTEST{" +
                    "\n gate=" + gate +
                    "\n sysAction=" + sysAction +
                    "\n }@" + hashCode();
        }

        protected RetryExecutorImpl(
                Executor executor,
                BooleanSupplier action
        ) {
            super(executor);
            sysAction = () -> {
                VersionedObject prev;
                do {
                    prev = gate;
                } while (
                        action.getAsBoolean() //retry
                                ||
                                !tryOpen(prev) //tryOpen misses because
                    // a next between action
                );

            };
        }

        boolean compareAndSet(VersionedObject expectedValue, VersionedObject newValue) {
            return GATE.compareAndSet(this, expectedValue, newValue);
        }

        boolean tryOpen(VersionedObject prev) { return compareAndSet(prev, VersionedObject.OPEN); }

        boolean tryClose(VersionedObject prev) {
            return compareAndSet(prev, VersionedObject.CLOSE);
        }

        record VersionedObject(int version) {
            private static final VersionedObject
                    CLOSE = new VersionedObject(0),
                    OPEN = new VersionedObject(1);

            public VersionedObject next() { return new VersionedObject(version + 1); }

            @Override
            public String toString() {
                String open = this == OPEN ? "OPEN" : this == CLOSE ? "CLOSE" : "";
                return "VersionedObject{" +
                        "\n dataVersion=" + version +
                        "}" + open;
            }
        }

        @Override
        public boolean execute() {
            VersionedObject prev = gate, next;

            if (prev == VersionedObject.OPEN && tryClose(prev)) {
                try {
                    execute(
                            sysAction
                    );
                } catch (Exception | Error e) {
                    throw new RuntimeException(e);
                }
                return true;
            } else {
                boolean failedCauseOpened = false;
                next = prev.next();
                while (
                        !compareAndSet(prev, next) //If cas fails retry
                                &&
                                !(failedCauseOpened = (prev = (VersionedObject) GATE.getOpaque(this)) == VersionedObject.OPEN)) //NOT by close
                { //contentious auto-incrementation
                    next = prev.next(); //retry increment
                }
                if (failedCauseOpened && (tryClose(prev))) { //If failed cause opened, and is the first
                    // to CLOSE again, then execute.
                    execute(
                            sysAction
                    );
                }
                return false;
            }
        }
    }
}