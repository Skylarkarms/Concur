import com.skylarkarms.concur.Executors;
import com.skylarkarms.concur.Locks;

import java.util.concurrent.TimeUnit;

public class DelayerTest {
    private static final String TAG = "DelayerTest";

    public static void main(String[] args) {
        final Executors.Delayer delayer2 = new Executors.Delayer(5, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            Locks.robustPark(2500, TimeUnit.MILLISECONDS);
            int finalI = i;
            long start = 0;
            if (i == 9) {
                start = System.nanoTime();
            }
            long finalStart = start;
            delayer2.onExecute(
                    () -> {
                        if (finalStart != 0) {
                            long end = System.nanoTime();
                            System.err.println(""
                                    + "\n called = " + finalI
                                    + "\n end = " + formatNanos(end - finalStart)
                            );
                        } else {
                            System.err.println("called = " + finalI);
                        }
                    }
            );
        }
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
