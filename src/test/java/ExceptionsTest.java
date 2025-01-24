import com.skylarkarms.concur.LazyHolder;
import com.skylarkarms.concur.Locks;

import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ExceptionsTest {

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
        System.setOut(new PrintStream(System.out, true));
        int wp = warmUp.begin();
        System.out.println("Warm up int = " + wp);

        System.out.println("<<<<<<<<||||||>>>>>>>");
        Locks.robustPark(Duration.ofSeconds(5).toNanos());

//        LazyHolder.setGlobalConfig(
//                LazyHolder.SpinnerConfig.timeout(TimeUnit.SECONDS.toMillis(3))
//        );

        System.err.println("<<<<<<<<||||||>>>>>>>");
        Locks.robustPark(Duration.ofSeconds(5).toNanos());

        LazyHolder.setDebug(true);
//        LazyHolder.SpinnerConfig customConfig = LazyHolder.SpinnerConfig.custom(
//                params -> {
//                    params.amplify(3);
//                }
//        );

        Locks.ExceptionConfig<RuntimeException> shortRT = Locks.ExceptionConfig.runtime(
//                        builder -> builder.setDurationUnit()
                builder -> {
                    builder.setDurationUnit(3500, TimeUnit.MILLISECONDS);
                    builder.setInitialWaitFraction(200);
                    builder.setMaxWaitFraction(10);
                    builder.setBackOffFactor(1.6);
                }
        );

        LazyHolder.setHolderConfig(
                shortRT
//                Locks.ExceptionConfig.runtime(
////                        builder -> builder.setDurationUnit()
//                        builder -> builder.setDurationUnit(3500, TimeUnit.MILLISECONDS)
//                )
        );

        record TAG(double i, String name, String tag){}
        LazyHolder.Supplier<TAG> integerSupplier = LazyHolder.Supplier.getNew(
//                Locks.ExceptionConfig.largeRuntime(),
//                customConfig,
//                params -> params.amplify(3),
                () -> {
                    long start = System.nanoTime();
                    Locks.robustPark(4, TimeUnit.SECONDS);
                    TAG res = new TAG(Math.pow(5, 6), "Juan", "LOL");
                    long end = System.nanoTime();
                    System.out.println("BUILT!!! "
                            + "\n TAG = \n" + res.toString().indent(3)
                            + " at time = " + Chrono.formatNanos(end - start)
                    );
                    return res;
                }
        );


        System.err.println(integerSupplier);
        long start = System.nanoTime();

        new Thread(
                () -> {
                    new Thread(
                            () -> {
                                try {
                                    TAG tag2 = integerSupplier.get();
                                    long end = System.nanoTime();
                                    System.out.println(""
                                            + "[GET SUCCESS] = " + tag2
                                            + "\n at = " + Thread.currentThread().getStackTrace()[1]
                                            + "\n time = " + Chrono.formatNanos(end - start)
                                    );
                                } catch (Exception e) {
                                    long end = System.nanoTime();
                                    StackTraceElement[] es = e.getStackTrace();
                                    System.err.println(
                                            "[GET FAILED] at = " + Chrono.formatNanos(end - start)
                                            + "\n error = \n" + e.getCause().toString().indent(3)
                                            + " prov = " + formatStack(es).indent(3)
                                            + " at = \n" + Thread.currentThread().getStackTrace()[1]
//                                            + "\n error = \n" + e.getCause().getMessage().indent(3)
//                                            + "\n prov = \n" + LazyHolder.formatStack(es).indent(3)
                                    );
                                }
                            }
                    ).start();
                    TAG tag = integerSupplier.get();

                    integerSupplier.clear(tag);
                    long dest_end = System.nanoTime();

                    System.out.println(tag
                            + "\n ...destroyed already...at = " + Chrono.formatNanos(dest_end - start)
                    );
                    System.out.println(integerSupplier);
                    System.out.println(integerSupplier.toStringDetailed());
                }
        ).start();

        new Thread(
                () -> {
                    Locks.robustPark(200, TimeUnit.MILLISECONDS);
                    try {
                        TAG tag = integerSupplier.getAndClear();
                        long end = System.nanoTime();
                        System.out.println(""
                                + "\n [DESTROY SUCCESS] \n" + (tag != null ? tag.toString().indent(3) : "   null")
                                + "\n at \n" + RetryExecutorTest.Chrono.formatNanos(end - start)
                                + "\n at \n" + Thread.currentThread().getStackTrace()[1]
                        );
                    } catch (Exception e) {
                        long end = System.nanoTime();
//                        e.printStackTrace();
                        StackTraceElement[] es = e.getStackTrace();
                        System.err.println(
                                "[DESTROY FAILED] at = " + Chrono.formatNanos(end - start)
                                + "\n error = \n" + e.getCause().toString().indent(3)
                                + " prov = " + formatStack(es).indent(3)
                                + " at = \n" + Thread.currentThread().getStackTrace()[1].toString().indent(3)
//                                + "\n error = \n" + e.getCause().getMessage().indent(3)
//                                + "\n prov = \n" + es[0].toString().indent(3)
                        );
                    }
                }
        ).start();

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
}
