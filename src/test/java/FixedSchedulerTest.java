import com.skylarkarms.concur.Executors;

import java.util.concurrent.atomic.AtomicInteger;

public class FixedSchedulerTest {
    private static final String TAG = "FixedSchedulerTest";

    public static void main(String[] args) {
        WarmUp w = new WarmUp();
        w.begin();

        Executors.FixedScheduler.ScheduleParams params = Executors.FixedScheduler.ScheduleParams.periodic(1);
        AtomicInteger times = new AtomicInteger();
        Executors.FixedScheduler scheduler = Executors.FixedScheduler.generator(
                params,
                () -> "Times = " + times.incrementAndGet() + "/" + (params.repetitions() + 1),
                System.err::println

        );
        scheduler.start();
    }
}
