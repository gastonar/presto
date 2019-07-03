package com.facebook.presto;

import io.airlift.slice.Slice;

public interface StatisticalDigest<T>
{
    default void add(double value, long weight)
    {
        throw new UnsupportedOperationException("Cannot add a double to a q-digest");
    }

    default void add(long value, long weight)
    {
        throw new UnsupportedOperationException("Cannot add a long to a t-digest");
    }

    void merge(StatisticalDigest<T> other);

    long estimatedInMemorySizeInBytes();

    long estimatedSerializedSizeInBytes();

    Slice serialize();

    T getDigest();
}
