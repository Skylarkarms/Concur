package com.skylarkarms.concur;

import java.util.Objects;
import java.util.function.Function;

/**
 * An {@code int}-{@link T} value pair record useful for version control
 * in contentious concurrent operations.
 * */
public record Versioned<T>(
        int version,
        T value)
{
    public boolean isDiff(T that) { return !Objects.equals(value, that); }

    public boolean isDiff(Versioned<T> that) { return isDiff(that.value); }

    public<M> M applyToVal(Function<T, M> map) { return map.apply(value); }

    private static volatile boolean def_init = false;
    private record DEFAULT() {static{def_init = true;}
        static final Versioned<?> ref = new Versioned<>(0, null);
    }
    @SuppressWarnings("unchecked")
    public static<T> Versioned<T> getDefault() { return (Versioned<T>) DEFAULT.ref; }
    public static <T> Versioned<T> first(T t) {
        assert t != null : "First value must not be null";
        return new Versioned<>(1, t);
    }

    public boolean isNewerThan(Versioned<T> that) {
        return (
                value != that.value
                        && version > that.version
        );
    }

    public boolean isNewerThan(int thatVersion) { return version > thatVersion; }

    public boolean equalVersion(Versioned<?> that) {
        return (
                version == that.version
        );
    }

    public boolean isNewerThanVersion(Versioned<?> that) { return version > that.version; }

    /**
     * @return a new {@link Versioned} of type {@link M} by applying {@link Function#apply(Object)} on the value of this {@link #value}.
     * The {@code version} will remain the same.
     * @param map the {@link Function} to be applied to this value.
     * */
    public <M> Versioned<M> apply(Function<T, M> map) { return new Versioned<>(version, map.apply(value)); }

    /**Will swap the value of this container while keeping the same {@code version}*/
    public <M> Versioned<M> swapType(M newValue) { return new Versioned<>(version, newValue); }

    public boolean isDefault() { return def_init && this == DEFAULT.ref; }

    /**
     * @return a new value with a {@code version} that equals {@code this.version} + 1.
     * */
    public Versioned<T> newValue(T t) { return new Versioned<>(version + 1, t); }

    String valueToString() {
        T val;
        return (val = value) == null ?
                "null" :
                val.toString();
    }

    @Override
    public String toString() {
        String hash = Integer.toString(hashCode());
        return isDefault() ? "[DEFAULT VERSIONED]=" + version :
                "Versioned@".concat(hash).concat("{" +
                "\n >>> value=\n" + valueToString().indent(3) +
                "}Versioned@").concat(hash);
    }
    public String toStringDetailed() {
        String hash = Integer.toString(hashCode());
        return isDefault() ? "[DEFAULT VERSIONED]=" + version :
                "Versioned@".concat(hash).concat("{" +
                "\n >>> dataVersion=" + version +
                ",\n >>> value=\n" + valueToString().indent(3) +
                "}Versioned@").concat(hash);
    }
}
