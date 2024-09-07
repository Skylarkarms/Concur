package com.skylarkarms.concur;

import com.skylarkarms.lambdas.Exceptionals;
import com.skylarkarms.lambdas.Lambdas;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;
import java.util.function.UnaryOperator;

/**
 * Lock-free lazy initialization holder.
 * <p> Lazy concurrent holder that will spinlock concurrent calls until the inner value is considered
 * {@link #CREATED}.
 * <p> Different spin-lock strategies can be defined at {@link TimeoutParams} set via {@link UnaryOperator}&lt;{@link TimeoutParamBuilder}&gt;
 * */
public class LazyHolder<T> {
    /**
     * Set to {@code true} for more detailed {@link Exception}s
     * <p> Setting this to {@code true} will hamper performance.
     * */
    public static boolean debug = false;
    private static final int
            NULL_PHASE = -1,
            CREATING_PHASE = 0,
    /**
     * Once the holder reaches this phase, the value is assumed fully assigned to this atomic reference.
     * */
    CREATED = 1;

    private static final Versioned<?>
            NULL = new Versioned<>(NULL_PHASE, null),
    /**
     * Process signaling the processing of the inner state
     * */
    CREATING = new Versioned<>(CREATING_PHASE, null);

    /**
     * The value to define the {@link LazyHolder.TimeoutParamBuilder#timeoutMillis} for all {@link LazyHolder}s by default
     * */
    public static long global_timeoutMillis = 1;

    /**
     * The value to define the {@link LazyHolder.TimeoutParamBuilder#parkedSpans} for all {@link LazyHolder}s by default
     * */
    public static int global_parked_spans = 8;

    /**
     * The value to define the {@link LazyHolder.TimeoutParamBuilder#spanReads} for all {@link LazyHolder}s by default
     * */
    public static final int global_span_reads = 6000;

    final TimeoutParams params;

    /**
     * The parameters that will define the way in which busy {@link Supplier#get()} will get resolved before a TimeoutException throws.
     * <p> The parameters consist of:
     * <ul>
     *     <li>
     *         {@link #spanReads}
     *     </li>
     *     <li>
     *         {@link #parkedSpans}
     *     </li>
     *     <li>
     *         {@link #timeoutMillis}
     *     </li>
     * </ul>
     * */
    public final class TimeoutParamBuilder {
        /**
         * The allowed number of attempts ({@code threshold}) that the current Thread will {@code busy-wait} while
         * attempt a loading from the specified reference defined by the {@link java.util.function.Supplier}.
         * If the number has been reached and the value has not been {@link #CREATED} yet,
         * the busy wait is considered "{@code failed}"
         * and the Thread will be parked.
         * */
        private int spanReads = global_span_reads;
        /**
         * If the {@link #spanReads} threshold has been reached without a successful load,
         * the {@link Thread} will enter a {@link LockSupport#parkNanos(long)} mode before retrying a {@code busy-wait} once again.
         * Each parking that follows a {@code failed busy-wait} attempt, represents 1 additional span.
         * Once the amount of spans has reached the span number defined here, the {@link Supplier#get()} will throw an {@link Exception}.
         * The amount of nanos that the {@link Thread} will spent parked is defined by dividing {@link #timeoutMillis} by this number.
         * */
        private int parkedSpans = global_parked_spans;
        /**
         * Comprises the sum of all the {@code nanos} the Thread wil spend parked ({@link LockSupport#parkNanos(long)}) after {@code failed busy-waits}:
         * <p> parkingNanos = timeoutMillis (to nanos) / {@link #parkedSpans}
         * */
        private long timeoutMillis = global_timeoutMillis;

        public TimeoutParamBuilder set(
                int spanReads
                , int parkedSpans
                , long timeoutMillis
        ) {
            this.spanReads = spanReads;
            this.parkedSpans = parkedSpans;
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        /**To define {@link #parkedSpans}*/
        public TimeoutParamBuilder setParkedSpans(int parkedSpans) {
            this.parkedSpans = parkedSpans;
            return this;
        }

        /**To define {@link #timeoutMillis}*/
        public TimeoutParamBuilder setTimeoutMillis(long timeoutMillis) {
            if (timeoutMillis == 0) throw new IllegalStateException("Use unbridled() instead");
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * Amplifies all the base default values for this specific com.skylarkarms.concurrents.LazyHolder.
         * <p> The amplification occurs by multiplying them for the '{@code multiplier}' provided.
         * <p> The default values pre-defined are set at:
         * <ul>
         *     <li>
         *         {@link LazyHolder#global_timeoutMillis}
         *     </li>
         *     <li>
         *         {@link LazyHolder#global_parked_spans}
         *     </li>
         *     <li>
         *         {@link LazyHolder#global_span_reads}
         *     </li>
         * </ul>
         * */
        public TimeoutParamBuilder broaden(float multiplier) {
            this.timeoutMillis = (long) (this.timeoutMillis * multiplier);
            this.parkedSpans = (int) (this.parkedSpans * multiplier);
            this.spanReads = (int) (this.spanReads * multiplier);
            return this;
        }

        /**
         * To define {@link #spanReads}
         * */
        public TimeoutParamBuilder setSpanReads(int spanReads) {
            this.spanReads = spanReads;
            return this;
        }

        public TimeoutParamBuilder unbridled() {
            this.timeoutMillis = 0;
            this.parkedSpans = 0;
            spanReads = Integer.MAX_VALUE;
            return this;
        }

        private TimeoutParams build() {
            return new TimeoutParams(timeoutMillis, parkedSpans, spanReads);
        }
    }

    private final class TimeoutParams {
        private TimeoutParams(
                long timeoutMillis
                , int parkedSpans
                , int spanReads
        ) {
            this.millis = Duration.ofMillis(timeoutMillis);
            this.waitingNanos = millis.dividedBy(parkedSpans).toNanos();
            this.parkedSpans = parkedSpans;
            this.spanReads = spanReads;
        }

        private final Duration millis;
        private final long waitingNanos;
        private final int parkedSpans;
        private final int spanReads;
        Versioned<T> timeOutSpin() {
            Versioned<T> prev;
            int tries = 0, toThrow = 0;
            while ((prev = ref).version() < CREATED) {
                tries++;
                if (tries > spanReads) {
                    if (toThrow++ == parkedSpans) {
                        String m = es == null ?
                                "TimeOutException:"
                                        + "\n TimeoutParams = " + this
                                        + "\n Waiting = " + (waitingNanos * toThrow) + " nanos, "
                                        + "\n            for " + (toThrow) + " tries. "
                                        + "\n This Holder is waiting too much on this Supplier."
                                        + "\n One option is to broaden the values of the TimeoutParams of the problematic Holder"
                                        + "\n If this Exception keeps appearing, the cause may be a cyclic referencing"
                                        + "\n  to find the possible source of the error, set 'com.skylarkarms.concurrents.LazyHolder.debug = true'."
                                :
                                "TimeOutException:"
                                        + "\n TimeoutParams = " + this
                                        + "\n Waiting = " + (waitingNanos * toThrow) + " nanos, "
                                        + "\n            for " + (toThrow) + " tries. "
                                        + "\n This Holder is waiting too much on this Supplier."
                                        + "\n One option is to broaden the values of the TimeoutParams of the problematic Holder"
                                        + "\n If this Exception keeps appearing, the cause may be a cyclic referencing"
                                        + "\n at = " + Exceptionals.formatStack(0, es);
                        throw new RuntimeException(m);
                    } else {
                        LockSupport.parkNanos(waitingNanos);
                        tries = 0;
                    }
                }
            }
            return prev;
        }

        @Override
        public String toString() {
            return "TimeoutParams{" +
                    "\n >>> millis=" + millis.toMillis() + ", [nanos =" + millis.toNanos() +"]" +
                    ",\n >>> waitingNanos=" + waitingNanos +
                    ",\n >>> spans=" + parkedSpans +
                    ",\n >>> spanReads=" + spanReads +
                    "\n }";
        }
    }

    @SuppressWarnings("unchecked")
    private static<T> T getNull() { return (T) NULL; }

    @SuppressWarnings("unchecked")
    private static<T> T getCreating() { return (T) CREATING; }

    volatile Versioned<T> ref = getNull();

    private static final VarHandle VALUE;
    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(LazyHolder.class, "ref", Versioned.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public boolean isNull() { return NULL == VALUE.getOpaque(this); }
    final StackTraceElement[] es;

    LazyHolder(
            UnaryOperator<TimeoutParamBuilder> params
    ) {
        if (debug) {
            es = Thread.currentThread().getStackTrace();
        } else es = null;

        this.params = params.apply(new TimeoutParamBuilder()).build();
    }

    @SuppressWarnings("unchecked")
    public T getAndDestroy() { return ((Versioned<T>) VALUE.getAndSet(this, getNull())).value(); }

    /**
     * @return true if the {@code expect}-ed value matched the inner value.
     * @param expect the expected value that will allow the reference clearing.
     * */
    public boolean destroy(T expect) {
        assert expect != null : "We'd rather expect that 'expect' was not null... thanks...";
        Versioned<T> prev = null;
        while (prev != (prev = ref)) { //in case of spurious failures
            if (!Objects.equals(prev.value(), expect)) return false;
            else if (VALUE.weakCompareAndSet(this, prev, getNull())) return true;
        }
        return false;
    }

    /**
     * Will not trigger a {@link #CREATING} process
     * @return The current value.
     * */
    @SuppressWarnings("unchecked")
    public T getOpaque() { return ((Versioned<T>) VALUE.getOpaque(this)).value(); }

    /**
     * Lazy and stateful {@link java.util.function.Supplier}.
     * Concurrent calls to {@link #get()} will spin-lock until the
     * {@link #builder} has finished.
     * @see LazyHolder
     * */
    public static class Supplier<T> extends LazyHolder<T> implements java.util.function.Supplier<T> {
        private final java.util.function.Supplier<T> builder;

        /**
         * Called once, while this reference is being {@link #CREATED}
         * */
        protected void onAssigned(T value) {}

        /**
         * Main {@link Supplier} constructor
         * @see TimeoutParamBuilder
         * */
        public Supplier(
                UnaryOperator<TimeoutParamBuilder> params
                , java.util.function.Supplier<T> builder
        ) {
            super(params);
            this.builder = builder;
            if (builder == null) throw new IllegalStateException("'builder' Supplier cannot be null");
        }

        public Supplier(java.util.function.Supplier<T> builder) {
            this(Lambdas.Identities.identity(), builder);
        }

        @SuppressWarnings("StatementWithEmptyBody")
        @Override
        public T get() {
            Versioned<T> prev;
            if ((prev = ref) == NULL) {
                if (VALUE.compareAndSet(this, getNull(), getCreating())) {
                    T res = builder.get();
                    onAssigned(res);
                    ref = new Versioned<>(CREATED, res);
                    return res;
                }
                while ((prev = ref).version() < CREATED) {}
            } else if (prev.version() < CREATED) {
                prev = params.timeOutSpin();
            }
            return prev.value();
        }

        public static final class OfInt implements IntSupplier {
            private final IntSupplier builder;

            volatile ValState ref = ValState.NULL;
            private static final VarHandle REF;
            final StackTraceElement[] es;

            static {
                try {
                    REF = MethodHandles.lookup().findVarHandle(OfInt.class, "ref", ValState.class);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            public OfInt(IntSupplier builder) {
                this.builder = builder;
                if (debug) {
                    es = Thread.currentThread().getStackTrace();
                } else es = null;
            }

            record ValState(int val, int state){
                static ValState NULL = new ValState(-1, NULL_PHASE);
                static ValState CREATING = new ValState(-1, CREATING_PHASE);
            }

            @SuppressWarnings("StatementWithEmptyBody")
            @Override
            public int getAsInt() {
                ValState prev;
                if ((prev = ref) == ValState.NULL) {
                    if (REF.compareAndSet(this, ValState.NULL, ValState.CREATING)) {
                        int res = builder.getAsInt();
                        ref = new ValState(res, CREATED);
                        return res;
                    }
                    while ((prev = ref).state < CREATED) {}
                } else if (prev.state < CREATED) {
                    int tries = 0, toThrow = 0;
                    while ((prev = ref).state < CREATED) {
                        tries++;
                        if (tries > 1000) {
                            LockSupport.parkNanos(250);
                            if (toThrow++ == 8) {
                                String m = es == null ?
                                        """
                                                TimeOutException:\s
                                                 This Exception may occur because of cyclic referencing\s
                                                 to find the possible source of the error, set 'com.skylarkarms.concurrents.LazyHolder.debug = true'."""
                                        :
                                        "TimeOutException:" +
                                                "\n This Exception may occur because of cyclic referencing " +
                                                "\n at = " + Exceptionals.formatStack(0, es);
                                throw new RuntimeException(m);
                            } else {
                                tries = 0;
                            }
                        }
                    }
                }
                return prev.val;
            }
        }
    }

    /**
     * Will apply and store the result.
     * <p> The {@link #builder} function will apply ONCE, any concurrent calls to {@link #apply} will spinlock until the result has been resolved.
     * <p> The stored result will capture the scope of builder function in the constructor,
     * unless every value captured is properly de-referenced (deep copy).
     *
     * <p> Lambdas are prone to false-positive memory-leaks by APIs like Leak-Cannary.
     * Anonymous classes prevent them.
     * */
    public static final class Function<S, T> extends LazyHolder<T>
            implements java.util.function.Function<S, T> {

        private final java.util.function.Function<S, T> builder;

        /**
         * @see #global_timeoutMillis
         * */
        public Function(
                UnaryOperator<TimeoutParamBuilder> params
                , java.util.function.Function<S, T> builder
        ) {
            super(params);
            this.builder = builder;
        }

        public Function(java.util.function.Function<S, T> builder) { this(Lambdas.Identities.identity(), builder); }

        @SuppressWarnings("StatementWithEmptyBody")
        @Override
        public T apply(S s) {
            Versioned<T> prev;
            if ((prev = ref) == NULL) {
                if (VALUE.compareAndSet(this, getNull(), getCreating())) {
                    T res = builder.apply(s);
                    ref = new Versioned<>(CREATED, res);
                    return res;
                }
                while ((prev = ref).version() < CREATED) {}
            } else if (prev.version() < CREATED) {
                prev = params.timeOutSpin();
            }
            return prev.value();
        }
    }

    /**
     * Collection of generic classes ({@link V}) that extend {@link Supplier}s that can be stored and retrieved via keys of common type {@link K} .
     * */
    public static class KeyedCollection2<K, V extends Supplier<?>> extends AbstractMap<K, V> {
        /**
         * Default implementation of {@link KeyedCollection2} where {@code V} = {@link Supplier} of type {@code ?}
         * */
        public static class Default<K> extends KeyedCollection2<K, Supplier<?>> {

            @SafeVarargs
            public
            <E extends SupplierEntry<?, K, Supplier<?>>>
            Default(
                    boolean unmodifiable
                    , E... entries
            ) {
                super(unmodifiable, entries);
            }

            @SafeVarargs
            public Default(boolean unmodifiable, SupplierEntry.Default<K, ?>... entries) {
                super(unmodifiable, entries);
            }

            public Default() { super(); }
        }
        /**The reason behind the map NOT being of at least of type AbstractEntry...
         * is because there should be a flexible way to populate it from the start.*/
        final Map<K, V> map;

        /**
         * Key-value pair entry for the {@link KeyedCollection2} class
         * */
        public static class SupplierEntry<T, K, V extends Supplier<T>>{
            final K key;
            final V value;

            public SupplierEntry(K key, V value) {
                this.key = key;
                this.value = value;
            }

            protected V valueTemplate(Object... varArgs) {return null;}

            public SupplierEntry(K key, Object... varArgs) {
                this.key = key;
                this.value = valueTemplate(varArgs);
                if (value == null) throw new IllegalStateException("Must override valueTemplate(Object... varArgs)");
            }

            public static class Default<K, T> extends SupplierEntry<T, K, Supplier<T>> {

                public Default(K key, java.util.function.Supplier<T> value) {
                    super(key,
                            new Supplier<>(value)
                    );
                }

                private Default(K key, Supplier<T> value) { super(key, value); }
            }
        }

        @SafeVarargs
        public<E extends SupplierEntry<?, K, V>> KeyedCollection2(
                boolean unmodifiable,
                final E... entries
        ) {
            if (unmodifiable) {
                this.map = getUnmodifiableAbs(entries);
            } else {
                final Map<K, V> map = new ConcurrentHashMap<>();
                if (entries != null) {
                    for (E e:entries) {
                        exceptPut(map, e);
                    }
                }
                this.map = map;
            }
        }

        @SafeVarargs
        public KeyedCollection2(
                boolean unmodifiable,
                final SupplierEntry.Default<K, ?>... entries
        ) {
            if (unmodifiable) {
                this.map = getUnmodifiableAbs(entries);
            } else {
                final Map<K, V> map = new ConcurrentHashMap<>();
                if (entries != null) {
                    for (SupplierEntry.Default<K, ?> e:entries) {
                        exceptTypedPut(map, e);
                    }
                }
                this.map = map;
            }
        }

        <E extends SupplierEntry<?, K, V>> void exceptPut(Map<K, V> map, E e) {
            if (map.putIfAbsent(e.key, e.value) == null) {
                onAdded(e.value);
            } else throw new IllegalStateException("Key " + e.key + " already present in map = " + map);
        }

        @SuppressWarnings("unchecked")
        <T, E extends SupplierEntry<T, K, ? extends Supplier<T>>> void exceptTypedPut(Map<K, V> map, E e) {
            if (map.putIfAbsent(e.key, (V) e.value) == null) {
                onAdded((V) e.value);
            } else throw new IllegalStateException("Key " + e.key + " already present in map = " + map);
        }

        /**
         * Will get called once a key-value pair ({@link SupplierEntry}) has been successfully added.
         * @see Map#putIfAbsent(Object, Object)
         * */
        protected void onAdded(V value) {}

        public KeyedCollection2() { this.map = new ConcurrentHashMap<>(); }

        /**
         * This method will only accept new keys.
         * @throws IllegalStateException if the key was already contained in the {@link Map}.
         * @return the value inserted.
         * */
        @Override
        public V put(K key, V value) {
            V prev = map.put(key, value);
            if (prev != null) throw new IllegalStateException("Key [" + key + "] already present in map = " + map);
            return value;
        }

        public<T, S extends Supplier<T>, E extends SupplierEntry<T, K, S>> E put(E value) {
            exceptTypedPut(map, value);
            return value;
        }

        @Override
        public void clear() { map.clear(); }


        @Override
        public V get(Object key) { return map.get(key); }

        @Override
        public V remove(Object key) { return map.remove(key); }

        @Override
        public Set<Entry<K, V>> entrySet() { return map.entrySet(); }

        @SafeVarargs
        private <E extends SupplierEntry<?, K, V>>
        Map<K, V>
        getUnmodifiableAbs(final E... entries) {
            Map<K, V> map = new HashMap<>();
            if (entries == null) throw new IllegalStateException("Entries must not be null");
            for (E e:entries) {
                exceptPut(map, e);
            }
            return Collections.unmodifiableMap(map);
        }

        @SafeVarargs
        private
        Map<K, V>
        getUnmodifiableAbs(final SupplierEntry.Default<K, ?>... entries) {
            Map<K, V> map = new HashMap<>();
            if (entries == null) throw new IllegalStateException("Entries must not be null");
            for (SupplierEntry.Default<K, ?> e:entries) {
                exceptTypedPut(map, e);
            }
            return Collections.unmodifiableMap(map);
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) { map.forEach(action); }
    }

    /**
     * Collection of {@link LazyHolder}s that allows for the storage of a <p>
     * single instance of any given class inserted.
     * <p> The collection cannot be changed once the Collection is created. <p>
     * */
    public static
    class SingletonCollection<T, S extends Supplier<T>> extends KeyedCollection2<Class<T>, S>{
        public static class Default extends KeyedCollection2.Default<Class<?>> {

            @SafeVarargs
            public
            <E extends SupplierEntry<?, Class<?>, Supplier<?>>>
            Default(
                    final E... entries
            ) {
                super(true, entries);
            }

            @SafeVarargs
            public Default(SupplierEntry.Default<Class<?>, ?>... entries) { super(true, entries); }

            @SafeVarargs
            public <E extends SupplierEntry<?, Class<?>, Supplier<?>>> Default(boolean unmodifiable, E... entries) {
                super(unmodifiable, entries);
            }

            public<C> C get(Class<C> tClass) {
                Supplier<?> supplier = Objects.requireNonNull(map.get(tClass), () -> "Type: " + tClass + " not present in map " + map);
                return tClass.cast(supplier.get());
            }

            @Override
            public Supplier<?> get(Object key) {
                throw new UnsupportedOperationException("SingletonCollection cannot accept anything other than tClass keys.");
            }
        }

        public static class SingletonEntry<T, S extends Supplier<T>>
                extends SupplierEntry<T, Class<T>, S> {

            public SingletonEntry(Class<T> key, S value) { super(key, value); }
        }

        @SafeVarargs
        public<E extends SingletonEntry<T, S>> SingletonCollection(
                boolean unmodifiable
                , E... entries) {
            super(unmodifiable, entries);
        }

        @SafeVarargs
        public <E extends SupplierEntry<T, Class<T>, S>> SingletonCollection(
                E... entries) {
            super(true, entries);
        }

        private SingletonCollection() { }

        public<C> C get(Class<C> tClass) {
            Supplier<?> supplier = Objects.requireNonNull(map.get(tClass), () -> "Type: " + tClass + " not present in map " + map);
            return tClass.cast(supplier.get());
        }

        @Override
        public S get(Object key) {
            throw new UnsupportedOperationException("SingletonCollection cannot accept anything other than tClass keys.");
        }
    }

    @Override
    public String toString() {
        return es != null ?
                "com.skylarkarms.concurrents.LazyHolder{" +
                        "\n >>> ref=" + ref +
                        ",\n >>> TimeoutParams=" + params +
                        ",\n >>> at=" + Exceptionals.formatStack(0, es) +
                        "\n }"
                :
                "com.skylarkarms.concurrents.LazyHolder{" +
                        "\n >>> ref=" + ref +
                        ",\n >>> TimeoutParams=" + params +
                        "\n }";
    }
}