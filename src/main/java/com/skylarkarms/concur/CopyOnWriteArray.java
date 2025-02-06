package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class CopyOnWriteArray<T> implements Supplier<T[]> {
    private final T[] EMPTY;

    @SuppressWarnings("unchecked")
    public CopyOnWriteArray(Class<T> type) {
        EMPTY = (T[]) Array.newInstance(type, 0);
        localArr = EMPTY;
    }

    public boolean isEmpty() { return EMPTY == get(); }

    public boolean isEmptyOpaque() { return EMPTY == VALUE.getOpaque(this); }

    @SuppressWarnings("FieldMayBeFinal")
    private volatile T[] localArr;
    /**This field will store an atomic snapshot
     * This snapshot may be used for parallel for-loops with interleaving mutations.
     * The parallel for-loop may or may not be in the need to abort the loop if a change in the snapshot has been performed.
     * */
    private volatile T[] snapshot;
    private static final VarHandle VALUE;
    private static final VarHandle SNAPSHOT;

    public T[] takePlainSnapshot() {
        snapshot = localArr;
        return snapshot;
    }

    public T[] getSnapshot() { return snapshot; }

    public boolean clearSnapshot(T[] expected) { return SNAPSHOT.compareAndSet(this, expected, EMPTY); }

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VALUE = l.findVarHandle(CopyOnWriteArray.class, "localArr", Object[].class);
            SNAPSHOT = l.findVarHandle(CopyOnWriteArray.class, "snapshot", Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    int updateAndGetLength(UnaryOperator<T[]> updateFunction) {
        T[] prev = get(), next = null;
        for (boolean haveNext = false;;) {
            if (!haveNext)
                next = updateFunction.apply(prev);
            if (weakCompareAndSetVolatile(prev, next))
                return next.length;
            haveNext = (prev == (prev = get()));
        }
    }

    int updateAndGetLengthShortCircuit(UnaryOperator<T[]> updateFunction) {
        T[] prev = get(), next = null;
        for (boolean haveNext = false;;) {
            if (!haveNext) next = updateFunction.apply(prev);

            if (next != prev) {
                if (weakCompareAndSetVolatile(prev, next))
                    return next.length;
                haveNext = (prev == (prev = get()));
            } else return 0;
        }
    }

    public boolean weakCompareAndSetVolatile(T[] expectedValue, T[] newValue) {
        return VALUE.weakCompareAndSet(this, expectedValue, newValue);
    }

    /**@return the index in which the item was inserted*/
    public int add(T t) {
        return updateAndGetLength(
                prev -> {
                    int newI = prev.length;
                    T[] clone = Arrays.copyOf(prev, newI + 1);
                    clone[newI] = t;
                    return clone;
                }
        ) - 1;
    }

    /**@return the index in which the item was inserted, or -1 if '{@code allow}' returns true*/
    public int add(T t, BooleanSupplier allow) {
        return updateAndGetLengthShortCircuit(
                prev -> {
                    if (!allow.getAsBoolean()) return prev;
                    int newI = prev.length;
                    T[] clone = Arrays.copyOf(prev, newI + 1);
                    clone[newI] = t;
                    if (!allow.getAsBoolean()) return prev;
                    return clone;
                }
        ) - 1;
    }

    public boolean hardRemove30Throw(T t) {
        T[] prev = localArr, next;
        int index, tries = 0;
        boolean allowed;
        while (
                (allowed = (index = indexOf(
                        prev,
                        t1 -> t1 == t
                )) != -1)
                        ||
                        tries++ < 100
        ) {
            if (allowed) {
                next = fastRemove(prev, EMPTY, index);
                if (VALUE.compareAndSet(this, prev, next)) {
                    return next.length == 0;
                }
            }
            prev = localArr;
        }
        throw new IllegalStateException("Object " + t + " not present in collection..." +
                ",\n tries = " + tries);
    }

    /**
     * Extremely contentious remove will attempt to remove until succeeds and if the
     * item is not found an {@link AssertionError} will throw.
     * */
    public boolean contentiousRemove(T t) {
        T[] prev = localArr,
                next = fastRemove(
                        prev,
                        EMPTY,
                        assertFoundIndex(t, prev)
                );
        while (!VALUE.weakCompareAndSet(this, prev,
                next
        )) {
            T[] wit = localArr; // this is better than dup_2... ("haveNext")
            // and even better if we hoist `wit`, ONLY if CAS
            // is assumed to fail a lot.
            if (wit != prev) {
                prev = wit;
                next = fastRemove(
                        wit,
                        EMPTY,
                        assertFoundIndex(t, wit)
                );
            }
        }
        return next.length == 0;
    }

    /**Non-contentious remove, will try ONCE and not throw*/
    public boolean nonContRemove(T t) { return nonContRemove(t1 -> t1 == t); }

    /**Non-contentious remove, will try ONCE and not throw*/
    public boolean nonContRemove(Predicate<T> when) {
        T[] prev = localArr, next;
        int index = indexOf(
                prev,
                when
        );
        assert index != -1 : "Item not found Error.";
        next = fastRemove(prev, EMPTY, index);
        return VALUE.compareAndSet(this, prev, next)
                && next.length == 0;
    }

    private int assertFoundIndex(T t, T[] prev) {
        int found = indexOf(
                prev, t1 -> t1 == t
        );
        assert found != -1 : "Item not found Error.";
        return found;
    }

    /**Will try to remove up to 200 tries, waiting for the activation to complete*/
    public T[] removeAll200() {
        T[] prev = localArr;
        int s_tries = 0, s_toThrow = 0;
        while (
                !(prev != EMPTY
                        && VALUE.weakCompareAndSet(this, prev, EMPTY))
        ) {
            if (s_tries++ > tries) {
                if (s_toThrow++ == toThrow) {
                    throw new RuntimeException(
                            "prev was Empty, tries = " + s_tries +
                                    ",\n arr = " + Arrays.toString(localArr) +
                                    ",\n EMPTY = " + Arrays.toString(EMPTY)
                    );
                } else {
                    LockSupport.parkNanos(250); //weak park, that's ok.
                    s_tries = 0;
                }
            }
            prev = localArr;
        }
        return prev;
    }

    @SuppressWarnings("unchecked")
    T[] weakGetAndSet(T[] next) {
        return (T[]) VALUE.getAndSet(this, next);
    }

    public static final int tries = 6300;
    // 6001 fails at AMD Ryzen 5 5500U ("onStateChanged fix")
    // 5801 fails at AMD Ryzen 5 5500U ("One read ++++" Activator)
    // 4801 fails at AMD Ryzen 5 5500U ("One read ++" Activator)
    // 4201 fails at AMD Ryzen 5 5500U ("One read" Activator)
    // 2101 fails at AMD Ryzen 5 5500U
    // 3801 fails at Intel(R) Core(TM) i7-4700HQ

    private static final int toThrow = 8;

    /**@return previous array*/
    public T[] addAll(T[] ts) {
        T[] prev = weakGetAndSet(ts);
        return prev == EMPTY ? null : prev;
    }

    public int size() {return localArr.length;}

    private static<E> E[] fastRemove(E[] prevArray, E[] empty, int i) {
        int newSize = prevArray.length - 1;
        if (newSize == 0) return empty;
        E[] dest_arr = prevArray;
        if (newSize >= i) {
            dest_arr = Arrays.copyOf(prevArray, newSize);
            System.arraycopy(prevArray, i + 1, dest_arr, i, newSize - i);
        }
        return dest_arr;
    }

    @SuppressWarnings("unchecked")
    public boolean contains(T object) {
        T[] prevArray = (T[])VALUE.getOpaque(this);
        int al = prevArray.length;
        for (int i = 0; i < al; i++) {
            T toTest = prevArray[i];
            if (toTest == object || (toTest != null && toTest.equals(object))) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public boolean contains(Predicate<T> when) {
        T[] prevArray = (T[])VALUE.getOpaque(this);
        int al = prevArray.length;
        for (int i = 0; i < al; i++) {
            T toTest = prevArray[i];
            if (toTest != null && when.test(toTest)) return true;
        }
        return false;
    }

    /**Wil shortC upon first one found*/
    private static<E> int indexOf(E[] prevArray, Predicate<E> when) {
        for (int i = 0; i < prevArray.length; i++) {
            E toTest = prevArray[i];
            if (toTest != null && when.test(toTest)) return i;
        }
        return -1;
    }

    @Override
    public T[] get() { return localArr; }

    @Override
    public String toString() {
        String hash = Integer.toString(hashCode());
        return "CopyOnWriteArray@".concat(hash).concat("{" +
                "\n >>> local=\n" + Arrays.toString(localArr).concat(",").indent(3) +
                " >>> size=" + localArr.length +
                "\n}@").concat(hash);
    }
}