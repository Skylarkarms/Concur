import com.skylarkarms.concur.Executors;
import com.skylarkarms.concur.LazyHolder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

public class RetryExecutorTest {
    private static final String TAG = "RetryExecutorTest";
    public static void main(String[] args) {
        Scope scope = new Scope();
        scope.read();
        System.out.println("Loop begins...");
        LockSupport.parkNanos(Duration.ofSeconds(3).toNanos());
        for (double i = 0; i < 50; i++) {
            scope.set(i);
        }
        LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
        System.out.println(scope);

        System.out.println("<<<<<<<<||||||>>>>>>>");
        LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
        Executors.BaseExecutor.Delayer delayer =
                new Executors.BaseExecutor.Delayer(Executors.UNBRIDLED(), TimeUnit.SECONDS, 5);

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

        System.out.println("<<<<<<<<||||||>>>>>>>");
        LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
        Executors.BaseExecutor.ContentiousExecutor cont =
                new Executors.BaseExecutor.ContentiousExecutor(Executors.UNBRIDLED());

        System.out.println(cont);

        System.out.println("<<<<<<<<||||||>>>>>>>");
        LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());

        LazyHolder.debug = true;
        LazyHolder.spinnerGlobalConfig.setUnbridled();

        record TAG(double i, String name, String tag){}
        LazyHolder.Supplier<TAG> integerSupplier = new LazyHolder.Supplier<>(
                params -> params.broaden(3),
                () -> new TAG(Math.pow(5, 6), "Juan", "LOL")
        );
        System.out.println(integerSupplier);
        TAG tag = integerSupplier.get();
        System.out.println(tag);
        System.out.println(integerSupplier);
        System.out.println(integerSupplier.toStringDetailed());
    }

    static class Scope {
        volatile double shared;

        public void set(double toSet) {
            this.shared = toSet;
            read();
        }

        private final Executors.ScopedExecutor re = new Executors.ScopedExecutor(
                Executors.UNBRIDLED(),
                new BooleanSupplier() {
                    @Override
                    public boolean getAsBoolean() {
                        double local = shared;
                        double res = Math.pow(local, 5);
                        if (local == shared) {
                            System.err.println(
                                    "Res = " + res
                                            + ",\n for = " + local
                            );
                            return local == shared;
                        } else return false;
                    }
                }
        );

        public void read(){
            re.execute();
        }

        @Override
        public String toString() {
            return "Scope{" +
                    "shared=" + shared +
                    ", re=" + re +
                    '}';
        }
    }
}
