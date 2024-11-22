import com.skylarkarms.concur.Executors;
import com.skylarkarms.concur.LazyHolder;
import com.skylarkarms.concur.Locks;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

public class RetryExecutorTest {

    static final WarmUp warmUp = new WarmUp();

    static final class Chrono {
        final AtomicLong start = new AtomicLong(System.nanoTime());

        public long lapse() {
            return System.nanoTime() - start.getOpaque();
        }

        public String lapsString() {
            long nanos = System.nanoTime() - start.getOpaque();
            return formatNanos(nanos);
        }

        public static String formatNanos(long nanos) {
            // Break down nanos into seconds, milliseconds, and remaining nanoseconds
            long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
            long millis = TimeUnit.NANOSECONDS.toMillis(nanos) % 1000;
            long remainingNanos = nanos % 1_000_000;

            // Build the formatted string
            return String.format("%d[seconds]: %03d[millis]: %03d[nanos]", seconds, millis, remainingNanos);
        }
    }

    public static void main(String[] args) {


        System.setErr(new PrintStream(System.err, true));
        System.setErr(new PrintStream(System.out, true));
        int wp = warmUp.begin();
        System.out.println("Warm up int = " + wp);

        for (int i = 0; i <= 20; i++) {
            scopeTest();
        }




        Locks.robustPark(Duration.ofSeconds(5).toNanos());

        System.out.println("<<<<<<<<||||||>>>>>>>");
        Locks.robustPark(Duration.ofSeconds(5).toNanos());
        Executors.Delayer delayer =
                new Executors.Delayer(5, TimeUnit.SECONDS);

        long nano = System.nanoTime();
        delayer.execute(
                () -> {
                    long currNano = System.nanoTime();
                    System.err.println("5 seconds later..." +
                            ",\n duration = " + Duration.ofNanos(currNano - nano).toSeconds()
                    );
                }
        );

        System.out.println(delayer);

        System.err.println("<<<<<<<<||||||>>>>>>>");
        Locks.robustPark(Duration.ofSeconds(5).toNanos());
        Executors.ContentiousExecutor cont =
                new Executors.ContentiousExecutor(Executors.UNBRIDLED());

        System.out.println(cont);

        System.err.println("<<<<<<<<||||||>>>>>>>");
        Locks.robustPark(Duration.ofSeconds(5).toNanos());

        LazyHolder.debug = true;
        LazyHolder.setGlobalConfig(LazyHolder.SpinnerConfig.custom(
                params -> {
                    params.amplify(1.5);
                }
        ));

        record TAG(double i, String name, String tag){}
        LazyHolder.Supplier<TAG> integerSupplier = new LazyHolder.Supplier<>(
                params -> params.amplify(3),
                () -> new TAG(Math.pow(5, 6), "Juan", "LOL")
        );


        System.out.println(integerSupplier);
        TAG tag = integerSupplier.get();
        System.out.println(tag);
        System.out.println(integerSupplier);
        System.out.println(integerSupplier.toStringDetailed());
    }

    private static void scopeTest(
    ) {
        System.out.println("Loop begins...");

        Locks.robustPark(3, TimeUnit.SECONDS);
        VarHandle.fullFence();
        Scope[] scopeArr = new Scope[1];
        final Scope scope;
        Chrono c = new Chrono();
        AtomicLong seq_lapse = new AtomicLong(-1);
        scope = scopeArr[0] = new Scope(
                timeParked -> {
                    long p_nan = c.lapse();
                    long finalL = p_nan - timeParked;
                    long seq_op = Locks.getUnless(Locks.ExceptionConfig.runtime(), seq_lapse::getOpaque, aLong -> aLong == -1);
                    long diff = finalL - seq_op;

                    if (Duration.ofNanos(diff).toSeconds() > 6) throw new IllegalStateException("Why so long...: "
                            + "\n >>> time parked = " + Chrono.formatNanos(timeParked)
                            + "\n >>> sequential finish = " + Chrono.formatNanos(seq_op) /*+ Duration.ofMillis(seq_op).toNanosPart() + "[nanos]"*/
                            + "\n >>> Parallel finish (dur) = " + Chrono.formatNanos(finalL)
                            + "\n >>> diff = " + Chrono.formatNanos(diff)
                            + "\n hash " + scopeArr[0].action.hashCode()
                    );
                    System.out.println("\n"
                            + "\n >>> time parked = " + Chrono.formatNanos(timeParked)
                            + "\n >>> sequential finish = " + Chrono.formatNanos(seq_op) /*+ Duration.ofMillis(seq_op).toNanosPart() + "[nanos]"*/
                            + "\n >>> Parallel finish (dur) = " + Chrono.formatNanos(finalL)
                            + "\n >>> diff = " + Chrono.formatNanos(diff)
                            + "\n"
                    );
                }
        );
        for (double i = 0; i < 150; i++) {
            scope.set(i);
        }
        VarHandle.fullFence();
        for (double i = 150; i < 300; i++) {
            scope.set(i);
        }
        Locks.robustPark(100);
        for (double i = 300; i < 400; i++) {
            scope.set(i);
        }
        long nan = c.lapse();
        seq_lapse.setOpaque(nan);
        System.out.println(
                "\n sequential finish = " + Chrono.formatNanos(nan) + "\n"
        );
        assert scope.re.isBusy() : "LOL!!!";
    }

    static class Scope {
        static final class VolatileDouble {
            volatile double d;
            static final VarHandle D;
            static {
                try {
                    D = MethodHandles.lookup().findVarHandle(
                            VolatileDouble.class, "d",
                    double.class);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            public void setaDouble(double aDouble) {
                D.setOpaque(this, aDouble);
            }

            public double getaDouble() {
                return d;
            }

            public double opaqueDouble() {
                return (double) D.getOpaque(this);
            }

        }
        VolatileDouble shared = new VolatileDouble();

        final LongConsumer done;

        public void set(double toSet) {
            this.shared.setaDouble(toSet);
            if (toSet == 150) {
                System.out.println("\n 150 at hash = " + action.hashCode());
            }
            read();
        }

        BooleanSupplier action = new BooleanSupplier() {

            @Override
            public boolean getAsBoolean() {
                double local = shared.getaDouble();
                double res = Math.pow(local, 5);
                if (local != shared.opaqueDouble()) {
                    System.out.println("return A = " + this.hashCode());
                    return false;
                } //6212, 4873, 4634
                int loops = (int) res;
                if (local != shared.opaqueDouble()) {
                    System.out.println("return B = " + this.hashCode());
                    return false;
                }
                int halfLoops = loops /2;
                long start = System.nanoTime();
                for (int i = 0; i < halfLoops; i++) {
                    res -= i;
                }
                double witness;
                if (local != (witness = shared.opaqueDouble())) {
                    System.out.println("\n return alpha:"
                            + "\n >>> middle time = " + Chrono.formatNanos(System.nanoTime() - start)
                            + "\n >>> witness = " + witness
                            + "\n >>> local = " + local
                            + "\n >>> for = " + this.hashCode()
                    );
                    return false;
                }
                for (int i = halfLoops; i < loops; i++) {
                    res -= i;
                }
                long end = System.nanoTime() - start;
                System.out.println(""
                        + "\n >>> time = " + Chrono.formatNanos(end)
                        + "\n >>> for = " + this.hashCode()
                );

                if (local == shared.opaqueDouble()) {
                    int secs = 1;
                    Chrono chrono = new Chrono();
                    Executors.Delayer.oneShot(
                            secs, TimeUnit.SECONDS,
                            () -> {
                                if (local == shared.getaDouble()) {
                                    done.accept(chrono.lapse());
                                    assert re.isIdle(): " local = " + local;
                                }
                            }
                    );
                    return local == shared.getaDouble();
                }
                else {
                    System.out.println("return C = " + this.hashCode());
                    return false;
                }
            }
        };


        private final Executors.ScopedExecutor re = new Executors.ScopedExecutor(
                Executors.UNBRIDLED(Thread.MAX_PRIORITY),
                action
        );

        Scope(LongConsumer done) {
            this.done = done;

        }

        public void read(){
            try {
                re.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public String toString() {
            return "Scope{" +
                    " >>> shared=" + shared +
                    ",\n >>> re=" + re +
                    "\n }";
        }
    }
}
