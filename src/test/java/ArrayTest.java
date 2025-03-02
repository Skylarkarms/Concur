import com.skylarkarms.concur.CopyOnWriteArray;
import com.skylarkarms.concur.Locks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ArrayTest {
    private static final String TAG = "ArrayTest";
    public static void main(String[] args) {
        record Obj(int i, String s){}
        int num = 100_000;
        Obj[] objs = new Obj[num];
        Arrays.setAll(
                objs,
                i -> new Obj(i, Integer.toString(i))
        );

        CopyOnWriteArray<Obj> copyOnWriteArray = new CopyOnWriteArray<>(Obj.class);
        ExecutorService service = Executors.newScheduledThreadPool(1_000);
        for (int i = 0; i < num; i++) {
            Obj cur = objs[i];
            int finalI = i;
            service.execute(
                    () -> {
                        copyOnWriteArray.add(cur);
                        if (finalI == (num - 1)) {
                            Locks.robustPark(2, TimeUnit.SECONDS);
                            Obj[] res = copyOnWriteArray.get();
                            System.out.println(
                                    "last on collection = " + res[res.length - 1]
                                    + "\n real last = " + cur
                            );
                            Locks.robustPark(2, TimeUnit.SECONDS);
                            shuffle(objs, new Random());
                            for (int j = 0; j < num; j++) {
                                Obj r = objs[j];
                                int finalJ = j;
                                service.execute(
                                        () -> {
                                            copyOnWriteArray.contentiousRemove(r);
                                            if (finalJ == (num - 1)) {
                                                Locks.robustPark(2, TimeUnit.SECONDS);
                                                if (!copyOnWriteArray.isEmpty()) throw new IllegalStateException("Something went wrong: " + copyOnWriteArray);
                                                System.out.println("FINISHED");
                                                service.shutdown();
                                            }
                                        }
                                );
                            }
                        }
                    }
            );
        }
    }

    static<T> void shuffle(T[] original, Random rnd) {
        // Shuffle array
        int i = original.length - 1, j;
        for (; i > 0; i--) {
            T tmp = original[i];
            original[i] = original[(j = rnd.nextInt(i + 1))];
            original[j] = tmp;
        }
    }
}
