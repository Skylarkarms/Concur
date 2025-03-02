package com.skylarkarms.concur;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

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

    /**@return the index in which the item was inserted*/
    public int add(T t) {
        Object[] prev = localArr, next = null;
        for (boolean haveNext = false;;) {
            if (!haveNext)
                next = getNext(t, prev);
            if (VALUE.weakCompareAndSet(this, prev, next))
                return next.length - 1;
            haveNext = (prev == (prev = get()));
        }
    }

    private static Object[] getNext(Object t, Object[] prev) {
        int newI = prev.length;

        int newLength = newI + 1;
        Class<? extends Object[]> newType = prev.getClass();
        Object[] clone = (newType == Object[].class)
                ? new Object[newLength]
                : (Object[]) Array.newInstance(newType.getComponentType(), newLength);
        System.arraycopy(prev, 0, clone, 0,prev.length);

        clone[newI] = t;
        return clone;
    }

    /**@return the index in which the item was inserted, or -1 if '{@code allow}' returns true*/
    public int add(T t, BooleanSupplier allow) {
        Object[] prev = localArr, next = null;
        for (boolean haveNext = false;;) {
            if (!haveNext) next = getNext(t, allow, prev);

            if (next != prev) {
                if (VALUE.weakCompareAndSet(this, prev, next))
                    return next.length - 1;
                haveNext = (prev == (prev = get()));
            } else return -1;
        }
    }

    private static final Object[] getNext(Object t, BooleanSupplier allow, Object[] prev) {
        if (!allow.getAsBoolean()) return prev;
        int newI = prev.length;
        Object[] clone = Arrays.copyOf(prev, newI + 1, prev.getClass());
        clone[newI] = t;
        if (!allow.getAsBoolean()) return prev;
        return clone;
    }

    /**
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (prev[i] == o) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    public boolean hardRemove30Throw(T t) {
        T[] prev = localArr, next;
        int index, tries = 0;
        boolean allowed;
        while (
                (allowed = (index = fastIndexOf(
                        prev,
                        t
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
     * item is not found an {@link IllegalStateException} will throw.
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             T pt = prev[i];
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (pt == o || pt != null && pt.equals(o)) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    public boolean contentiousRemove(Object t) {
        T[] prev = localArr,
                next = equalsRemove(
                        prev,
                        EMPTY,
                        t
                );
        while (!VALUE.weakCompareAndSet(this, prev,
                next
        )) {
            T[] wit = localArr; // this is better than dup_2... ("haveNext")
            // and even better if we hoist `wit`, ONLY if CAS
            // is assumed to fail a lot.
            if (wit != prev) {
                prev = wit;
                next = equalsRemove(
                        wit,
                        EMPTY,
                        t
                );
            }
        }
        return next.length == 0;
    }

    public boolean fastContentiousRemove(T t) {
        T[] prev = localArr,
                next = fastRemove(
                        prev,
                        EMPTY,
                        t
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
                        t
                );
            }
        }
        return next.length == 0;
    }

    /**
     * An object with the result from the search.
     * Comprises:
     * <ul>
     *     <li>
     *         {@link #size} = the current size of the array after the action
     *     </li>
     *     <li>
     *         {@link #find} = the found object, or null if {@code !found}
     *     </li>
     *     <li>
     *         {@link #found} = {@code `false`} if the search was unsuccessful
     *     </li>
     * </ul>
     * */
    public record Search<T>(int size, T find, boolean found){
        public Search(int size, T find) {
            this(size, find, true);
        }

        static final Search<?> not_found = new Search<>(-1, null, false);

        @SuppressWarnings("unchecked")
        static<S> Search<S> not_found() {
            return (Search<S>) not_found;
        }
    }

    /**Non-contentious remove, will try ONCE and not throw.
     *  @return : {@link Search}
     *  <ul>
     *      <li>
     *          {@link Search#found()} = item found
     *      </li>
     *      <li>
     *          {@code `null`} = contention met, swap failed
     *      </li>
     *  </ul>
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             T pt = prev[i];
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (pt == o || pt != null && pt.equals(o)) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    public Search<T> nonContRemove(Object o) {
        T[] prev = localArr;
        int pl = prev.length;
        for (int i = 0; i < pl; i++) {
            // inner objects will be forced to use their respective equal(Object) methods
            T pt = prev[i];
            if (pt == o || pt != null && pt.equals(o)) {
                int newSize = pl - 1;
                if (newSize == 0) return VALUE.compareAndSet(this, prev, EMPTY) ? new Search<>(0, pt) : null;
                Object[] destArr = Arrays.copyOf(prev, newSize, prev.getClass());
                if (i < newSize) {
                    System.arraycopy(prev, i + 1, destArr, i, newSize - i);
                }
                return VALUE.compareAndSet(this, prev, destArr) ? new Search<>(newSize, pt) : null;
            }
        }
        return Search.not_found();
    }

    /**Non-contentious remove, will try ONCE and not throw.
     *  @return :
     *  <ul>
     *      <li>
     *          -1 = item not found
     *      </li>
     *      <li>
     *          -2 = contention met, swap failed
     *      </li>
     *      <li>
     *         n >= 0 = new size
     *      </li>
     *  </ul>
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (prev[i] == o) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    public int fastNonContRemove(T o) {
        T[] prev = localArr/*, next*/;
        int pl = prev.length;
        for (int i = 0; i < pl; i++) {
            // inner objects will be forced to use their respective equal(Object) methods
            if (prev[i] == o) {

                int newSize = pl - 1;
                if (newSize == 0) {
                    return VALUE.compareAndSet(this, prev, EMPTY) ? 0 : -2;
                }
                Object[] destArr = Arrays.copyOf(prev, newSize, prev.getClass());
                if (i < newSize) {
                    System.arraycopy(prev, i + 1, destArr, i, newSize - i);
                }
                return !VALUE.compareAndSet(this, prev, destArr) ? newSize : -2;
            }
        }
        return -1;
    }

    /**Non-contentious remove, will try ONCE
     * @return true if the item removed was the LAST one.
     * */
//    /**Non-contentious remove, will try ONCE and throw if not found.*/
    public int nonContRemove(Predicate<T> when) {
        T[] prev = localArr/*, next*/;
        int pl = prev.length;
        for (int i = 0; i < pl; i++) {
            T p = prev[i];
            if (p != null && when.test(p)) {
                int newSize = pl - 1;
                if (newSize == 0) return VALUE.compareAndSet(this, prev, EMPTY) ? 0 : -2;

                Object[] destArr = Arrays.copyOf(prev, newSize, prev.getClass());
                if (i < newSize) {
                    System.arraycopy(prev, i + 1, destArr, i, newSize - i);
                }
                return VALUE.compareAndSet(this, prev, destArr) ? newSize : -2;
            }
        }
        return -1;
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

    public static final int tries = 6300;
    // 6001 fails at AMD Ryzen 5 5500U ("onStateChanged fix")
    // 5801 fails at AMD Ryzen 5 5500U ("One read ++++" Activator)
    // 4801 fails at AMD Ryzen 5 5500U ("One read ++" Activator)
    // 4201 fails at AMD Ryzen 5 5500U ("One read" Activator)
    // 2101 fails at AMD Ryzen 5 5500U
    // 3801 fails at Intel(R) Core(TM) i7-4700HQ

    private static final int toThrow = 8;

    /**@return previous array*/
    @SuppressWarnings("unchecked")
    public T[] addAll(T[] ts) {
        T[] prev = (T[]) VALUE.getAndSet(this, ts);
        return prev == EMPTY ? null : prev;
    }

    public int size() {return localArr.length;}

    @SuppressWarnings("unchecked")
    private static<E> E[] fastRemove(E[] prevArray, E[] empty, int i) {
        int newSize = prevArray.length - 1;
        if (newSize == 0) return empty;
        Object[] destArr = Arrays.copyOf(prevArray, newSize, prevArray.getClass());
        if (i < newSize) {
            System.arraycopy(prevArray, i + 1, destArr, i, newSize - i);
        }
        return (E[]) destArr;
    }

    @SuppressWarnings("unchecked")
    private static<E> E[] equalsRemove(E[] prevArray, E[] empty, Object e) {
        int pl = prevArray.length;
        int newSize = pl - 1;
        if (newSize == 0) return empty;
        for (int i = 0; i < pl; i++) {
            E pe = prevArray[i];
            if (pe == e || pe != null && pe.equals(e)) {
                Object[] destArr = Arrays.copyOf(prevArray, newSize, prevArray.getClass());
                if (i < newSize) {
                    System.arraycopy(prevArray, i + 1, destArr, i, newSize - i);
                }
                return (E[]) destArr;
            }
        }
        throw new IllegalStateException("Item not found.");
    }

    @SuppressWarnings("unchecked")
    private static<E> E[] fastRemove(E[] prevArray, E[] empty, E e) {
        int pl = prevArray.length;
        int newSize = pl - 1;
        if (newSize == 0) return empty;
        for (int i = 0; i < pl; i++) {
            if (prevArray[i] == e) {
                Object[] destArr = Arrays.copyOf(prevArray, newSize, prevArray.getClass());
                if (i < newSize) {
                    System.arraycopy(prevArray, i + 1, destArr, i, newSize - i);
                }
                return (E[]) destArr;
            }
        }
        throw new IllegalStateException("Item not found.");
    }

    public boolean contains(Object object) {
        Object[] prevArray = (Object[])VALUE.getOpaque(this);
        int al = prevArray.length;
        for (int i = 0; i < al; i++) {
            Object toTest = prevArray[i];
            if (toTest == object || (toTest != null && toTest.equals(object))) return true;
        }
        return false;
    }

    /**
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             T toTest = prev[i];
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (toTest.equals(o)) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    @SuppressWarnings("unchecked")
    public T get(Object object) {
        T[] prevArray = (T[])VALUE.getOpaque(this);
        int al = prevArray.length;
        for (int i = 0; i < al; i++) {
            T toTest = prevArray[i];
            if (toTest == object || (toTest != null && toTest.equals(object))) return toTest;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public boolean contains(Predicate<T> when) {
        Object[] prevArray = (Object[])VALUE.getOpaque(this);
        int al = prevArray.length;
        for (int i = 0; i < al; i++) {
            Object toTest = prevArray[i];
            if (toTest != null && when.test((T)toTest)) return true;
        }
        return false;
    }

    /**
     * <p> The comparison methodology used is as follows:
     * <pre>{@code
     *     void method(Object o) {
     *        // ... ...
     *         for (int i = 0; i < pl; i++) {
     *             // inner objects will be forced to use their
     *             // respective equal(Object) methods against the parameter object.
     *             if (prev[i] == o) {
     *                // proceed to remove `o`
     *             }
     *         }
     *     }
     * }</pre>
     * */
    private static final int fastIndexOf(Object[] prevArray, Object instance) {
        int pl = prevArray.length;
        for (int i = 0; i < pl; i++) {
            Object toTest = prevArray[i];
            if (toTest == instance) return i;
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