package com.facebook.presto.type;

import com.facebook.presto.StatisticalDigest;
import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;

public class StatisticalTDigest
        implements StatisticalDigest<TDigest>
{
    private final TDigest tDigest;

    public StatisticalTDigest(TDigest tDigest)
    {
        this.tDigest = tDigest;
    }

    @Override
    public void add(double value, long weight)
    {
        tDigest.add(value, weight);
    }

    @Override
    public void merge(StatisticalDigest<TDigest> other)
    {
        tDigest.merge(other.getDigest());
    }

    @Override
    public long estimatedInMemorySizeInBytes()
    {
        return tDigest.estimatedInMemorySizeInBytes();
    }

    @Override
    public long estimatedSerializedSizeInBytes()
    {
        return tDigest.estimatedSerializedSizeInBytes();
    }

    @Override
    public Slice serialize()
    {
        return tDigest.serialize();
    }

    @Override
    public TDigest getDigest()
    {
        return tDigest;
    }
}
