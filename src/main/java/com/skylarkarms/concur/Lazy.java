package com.skylarkarms.concur;

import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public interface Lazy<T> {
    T getAndClear();
    <E extends Exception> T getAndClear(final Locks.ExceptionConfig<E> timeoutConfig) throws E;
    boolean clear(T expect);
    T getOpaque();
    boolean isNull();

    interface LazySupplier<T> extends Lazy<T>, Supplier<T> {
        T reviveGet(int maxTries) throws TimeoutException;
        T reviveGet();
        <E extends Exception> T reviveGet(Locks.ExceptionConfig<E> config, final int maxTries) throws TimeoutException, E;
        T get();
        <E extends Exception> T get(Locks.ExceptionConfig<E> config) throws E;
    }

    interface LazyFunction<T, U> extends Lazy<U>, Function<T, U> {
        U reviveApply(T s);
        U reviveApply(T s, final int maxTries) throws TimeoutException;
        <E extends Exception> U reviveApply(T s, final int maxTries, Locks.ExceptionConfig<E> config) throws TimeoutException, E;
        U apply(T s);
        <E extends Exception> U apply(T s, Locks.ExceptionConfig<E> config) throws E;
    }
    
    interface OfIntSupplier extends IntSupplier {
        int getAsInt();
        <E extends Exception> int getAsInt(Locks.ExceptionConfig<E> config) throws E;
        int getAndClear();
        <E extends Exception> int getAndClear(final Locks.ExceptionConfig<E> timeoutConfig) throws E;
        boolean clear(int expect);
        int reviveGet(int maxTries) throws TimeoutException;
        <E extends Exception> int reviveGet(Locks.ExceptionConfig<E> config, int maxTries) throws TimeoutException, E;
    }
}
