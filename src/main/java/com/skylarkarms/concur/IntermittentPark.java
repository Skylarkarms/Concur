package com.skylarkarms.concur;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A class that allows the parking of Threads {@link #park()} for a given time specified by the {@link Time} object.
 * */
public class IntermittentPark {

    private static final LongConsumer emp = value -> {};
    public static volatile LongConsumer logger = emp;
    private static String logToS(LongConsumer l) {
        return l == logger ?
                l == emp ? "[NONE]"
                        :
                        "[IntermittentPark.GLOBAL_LOGGER] = ".concat(String.valueOf(l))
                :
                String.valueOf(l);
    }
    private static volatile boolean logger_grabbed;

    static final int thous = 1_000, mill = 1_000_000;

    /**
     * @param logger a consumer that will consume the total nanos waited.
     * */
    public static synchronized void setLogger(LongConsumer logger) {
        if (logger_grabbed) throw new IllegalStateException("Logger already initialized.");
        IntermittentPark.logger = logger == null ? emp : logger;
    }

    record GLOB_LOG() {
        static {logger_grabbed = true;}
        static final LongConsumer ref = logger;
    }

    final Time time;
    private final LongConsumer instanceLogger;

    static LongSupplier rest(Random rest, TimeUnit unit) {
        return () -> switch (unit) {
            case NANOSECONDS -> 0;
            case MICROSECONDS -> rest.nextInt(thous);
            case MILLISECONDS -> rest.nextInt(mill);
            case SECONDS -> ((long) rest.nextInt(mill) * thous) + rest.nextInt(thous);
            default -> throw new IllegalStateException("Unexpected value: " + unit);
        };
    }

    /**
     * A parameter object that defines the waiting method, either:
     * <ul>
     *     <li>
     *         {@link #randomized(int, int, TimeUnit)}
     *     </li>
     *     <li>
     *         {@link #fixed(int, TimeUnit)}
     *     </li>
     * </ul>
     * */
    public static final class Time {
        final int lower_bound, higher_bound, delta;
        final TimeUnit unit;
        final LongSupplier nanSup;

        /**
         * @return a nanos supplier randomizer between the {@link  #lower_bound} and {@link #higher_bound},
         * <p> parking the Thread for a randomized period.
         * */
        public static Time randomized(int lower_bound, int higher_bound, TimeUnit unit) {
            return new Time(lower_bound, higher_bound, unit);
        }

        /**
         * @return a nanos supplier that will return the same fixed {@link #lower_bound} every time it is called.
         * <p> parking the Thread for a fixed time each call to {@link IntermittentPark#park()}
         * */
        public static Time fixed(int lower_bound, TimeUnit unit) {
            return new Time(lower_bound, 0, unit);
        }

        private Time(int lower_bound, int higher_bound, TimeUnit unit) {
            this.lower_bound = lower_bound;
            this.higher_bound = higher_bound;
            this.unit = unit;


            if (higher_bound != 0) {
                delta = higher_bound - lower_bound;
                if (delta < 0) throw new IllegalStateException(
                        "\nBound must be positive: " + delta
                                + "\n higher bound = " + higher_bound
                                + "\n lower bound = " + lower_bound
                );
                final Random
                        random = new Random()
                        , rest = new Random()
                        ;
                final LongSupplier restSupplier = rest(rest, unit);
                nanSup = () -> unit.toNanos(lower_bound + random.nextInt(delta)) + restSupplier.getAsLong();
            } else {
                delta = lower_bound;
                nanSup = () -> unit.toNanos(lower_bound);
            }
        }

        long nans() {
            return nanSup.getAsLong();
        }

        public int getLower_bound() {
            return lower_bound;
        }

        public int getHigher_bound() {
            return higher_bound;
        }

        public int getDelta() {
            return delta;
        }

        public TimeUnit getUnit() {
            return unit;
        }

        @Override
        public String toString() {
            return "Time{" +
                    "fixed=" + (higher_bound == 0) +
                    "lower_bound=" + lower_bound +
                    ", higher_bound=" + higher_bound +
                    ", delta=" + delta +
                    ", unit=" + unit +
                    ", nanSup=" + nanSup +
                    '}';
        }
    }

    public IntermittentPark(
            Time time) {
        this(time, GLOB_LOG.ref);
    }

    /**
     * @param time see {@link Time}.
     * @param log the final nanos waited returned AFTER the sleep has finished.
     * */
    public IntermittentPark(
            Time time,
            LongConsumer log) {
        this.time = time;
        this.instanceLogger = log != null ? log : emp;
    }

    public void park() {
        long nans = time.nanSup.getAsLong();
        Locks.robustPark(nans);
        instanceLogger.accept(nans);
    }

    @Override
    public String toString() {
        return "IntermittentWait{" +
                "time=" + time +
                ", instanceLogger=" + logToS(instanceLogger) +
                '}';
    }
}
