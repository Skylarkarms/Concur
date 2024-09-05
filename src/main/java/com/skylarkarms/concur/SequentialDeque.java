package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * This class allows the concurrent enqueuing of tasks while ensuring a sequential flushing mechanism.
 * Once a flushing has begun it cannot be stopped.
 * Subsequent enques that happen WHILE a flushing has began, will enqueue the task to the last postion in the Queue,
 * If NO flushing is currently being performed, methods such as {@link #pushAndFlush(Runnable)} OR {@link Async#pushAndFlush(Async.CommandFunction)}
 * will trigger a new flush on the Thread specified in the constructor... OR on the same Thread executing the push if no specification is set.
 * All enqueued Commands (Commands which have been pushed via {@link Async#push(Async.CommandFunction)} OR {@link #push(Runnable)})
 * Will enqueue tasks WITHOUT triggering a flush, and a calling {@link #beginFlush()}
 * will execute all enqueued tasks in the Thread in which the flushing {@link #beginFlush()} has being called, OR IF a flushing has
 * been triggered by {@link Async#pushAndFlush(Async.CommandFunction)} OR {@link #pushAndFlush(Runnable)}.
 * {@link Async.CommandFunction} is a special command which is meant to be used for executions which will happen non-sequentially.
 * Either it be because of an asyncrhonous callback or a delayed execution.
 * To continue the flushing this delayed commands MUST execute {@link Async.Nextable#next()} to continue executing enqueued commands.
 * {@link Async.Nextable#next()} will suspend the Thread until EVERY command in the Dequeue has been processed, and return a {@code true}
 * if there was at least ONE process enqueued, or {@code false} if it was the last.
 * */
public class SequentialDeque {
    final BooleanSupplier flusher;
    final Deque<Runnable> commands = new ConcurrentLinkedDeque<>();
    private volatile boolean flushing;
    private final AtomicInteger size = new AtomicInteger();
    private static final VarHandle FLUSHING;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            FLUSHING = l.findVarHandle(SequentialDeque.class, "flushing", boolean.class);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
    public SequentialDeque(Executor exitExecutor) {
        flusher =
                exitExecutor != null ?
                        () -> attemptFlush(
                                () -> {
                                    exitExecutor.execute(
                                            this::sysFlush
                                    );
                                    return true;
                                }
                        ) :
                        () -> attemptFlush(this::sysFlush);
    }

    protected boolean attemptFlush(BooleanSupplier flush) {
        if (FLUSHING.compareAndSet(this, false, true)) {
            return flush.getAsBoolean();
        } else return false;
    }

    boolean sysFlush() {
        Runnable last;
        while ((last = commands.pollLast()) != null) {
            proccess(last);
        }
        iterativeFlush();
        return true;
    }

    private void proccess(Runnable last) {
        size.decrementAndGet();
        last.run();
    }

    /**
     * @return false if the Runnable missed the flush.
     *         true if a flush has not being triggered
     *         yet, OR the Runnable did NOT missed the flush.
     * */
    public boolean push(Runnable seq) {
        size.incrementAndGet();
        if (!(boolean)FLUSHING.getOpaque(this)) {
            commands.push(seq);
            return true;
        } else {
            commands.push(seq);
            return !(boolean)FLUSHING.getOpaque(this)
                    && size.getOpaque() != 0;
        }
    }

    /**
     * @return true, if this method succesfully triggers a flush..
     * */
    public boolean pushAndFlush(Runnable seq) {
        size.incrementAndGet();
        commands.push(
                seq
        );
        return flusher.getAsBoolean();
    }

    private void iterativeFlush() {
        boolean notEmpty;
        FLUSHING.setOpaque(this,
                (notEmpty = !(size.getOpaque() == 0)));
        if (notEmpty) {
            Runnable last;
            while ((last = commands.pollLast()) != null) {
                proccess(last);
            }
            iterativeFlush();
        }
    }

    public SequentialDeque() {
        this(null);
    }

    public boolean isFlushing() {
        return flushing;
    }

    /**
     * Will always return {@code false} If a flush is already processing
     * OR {@code true} If this method triggered a flush.
     * */
    public boolean beginFlush() {
        return flusher.getAsBoolean();
    }

    /**
     * Unspecified concurrent behavior...
     * */
    public List<Runnable> clear() {
        final List<Runnable> res = new ArrayList<>(size.getOpaque());
        Runnable last;
        while ((last = commands.pollLast()) != null) {
            res.add(last);
            size.decrementAndGet();
        }
        return res;
        // Enforce next will set to false if a flush is active.
    }

    public static class Async extends SequentialDeque {
        @FunctionalInterface
        public interface Nextable {
            /**
             * The next method will force the execution of the next command ({@link Runnable} OR {@link CommandFunction}) in the Dequeue.
             * This method will suspend the Thread until EVERY command in the Dequeue has been processed, and return a {@code true}
             * if there was at least ONE process enqueued, or {@code false} if it was the last.
             * */
            boolean next();
        }
        /**
         * {@link CommandFunction} is a special command which is meant to be used for executions which will happen non-sequentially.
         * Either it'd be because of an asynchronous callback or a delayed execution.
         * To continue the flushing this delayed commands MUST execute {@link Nextable#next()} to continue executing enqueued commands.
         * {@link Nextable#next()} will suspend the Thread until EVERY command in the Dequeue has been processed, and return a {@code true}
         * if there was at least ONE process enqueued, or {@code false} if it was the last.
         * */
        @FunctionalInterface
        public interface CommandFunction extends Function<Nextable, Runnable> {}

        private final Nextable enforceNext = () -> {
            final Runnable last;
            if ((last = commands.pollLast()) == null) {
                //We don't need to "exchange" or keep on relaxed double-checking, since each
                // individual pushAndFlush will check any failure on their own.
                FLUSHING.setOpaque(this, false);
                return false;
            } else {
                last.run();
                return true;
            }
        };

        @Override
        boolean sysFlush() {
            return enforceNext.next();
        }

        @Override
        public boolean push(Runnable seq) {
            return super.push(
                    getRunnable(seq)
            );
        }

        @Override
        public boolean pushAndFlush(Runnable seq) {
            return super.pushAndFlush(getRunnable(seq));
        }

        private Runnable getRunnable(Runnable seq) {
            return () -> {
                seq.run();
                enforceNext.next();
            };
        }

        /**
         * Will always return {@code false} If a flush is already processing
         * OR {@code true} If this method triggered a flush.
         * */
        public boolean pushAndFlush(CommandFunction command) {
            commands.push(command.apply(
                    enforceNext
            ));
            return flusher.getAsBoolean();
        }

        public void push(CommandFunction command) {
            commands.push(command.apply(
                            enforceNext
                    )
            );
        }
    }
}