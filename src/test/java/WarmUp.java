import com.skylarkarms.concur.Executors;
import com.skylarkarms.concur.Locks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class WarmUp {

    private final Locks.Valet doneVal = new Locks.Valet();

    volatile int res = -1;

    private final ExecutorService executor = new Executors.ThrowableExecutor(4);
    Random r = new Random();
    int rand = 200 - r.nextInt(100);
    AtomicInteger procs = new AtomicInteger();

    public int begin() {
        int[] res = new int[rand];
        res[0] = r.nextInt(10);
        for (int i = 1; i < rand; i++) {
            int finalI = i;
            executor.execute(
                    () -> {
                        res[finalI] = res[finalI - 1] + finalI;
                        int num = procs.incrementAndGet();
                        if (num == (rand - 1)) {
                            System.out.println("" +
                                    "\n >>> unparking...\n");
                            doneVal.discharge();
                        }
                    }
            );
        }
        int[] nextRand = new int[1];
        executor.execute(
                () -> {
                    nextRand[0] = (10 - r.nextInt(5));
                    try {
                        doneVal.park();
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }

                    int res_i = 0;
                    System.out.println("\n >> res = \n" + Arrays.toString(res) + "\n");
                    for (int i:res
                    ) {
                        res_i += i;
                    }
                    this.res = res_i;
                    executor.shutdown();
                }
        );
        try {
            Locks.waitIf(
                    Locks.ExceptionConfig.timeout(100),
                    () -> {
                        int fRes;
                        boolean shouldWait = (fRes = this.res) == -1;
                        System.out.println("should wait? " + shouldWait
                                + "\n res = " + fRes);
                        return shouldWait;
                    }
            );
        } catch (TimeoutException e) {
            e.printStackTrace();
            try {
                throw e;
            } catch (TimeoutException ex) {
                ex.printStackTrace();
            }
        }
        int ffRes = this.res;
        System.out.println(" "
                + "\n >>> FFRes = " + ffRes
                + "\n >>> nextRand = " + nextRand[0]
        );
        return ffRes * nextRand[0];
    }
}
