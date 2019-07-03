package com.facebook.presto.type;

import com.facebook.presto.StatisticalDigest;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;

public class StatisticalQuantileDigest
        implements StatisticalDigest<QuantileDigest>
{
    private final QuantileDigest qdigest;

    public StatisticalQuantileDigest(QuantileDigest qdigest)
    {
        this.qdigest = qdigest;
    }

    @Override
    public void add(long value, long weight)
    {
        qdigest.add(value, weight);
    }

    @Override
    public void merge(StatisticalDigest<QuantileDigest> other)
    {
        qdigest.merge(other.getDigest());
    }

    @Override
    public long estimatedInMemorySizeInBytes()
    {
        return qdigest.estimatedInMemorySizeInBytes();
    }

    @Override
    public long estimatedSerializedSizeInBytes()
    {
        return qdigest.estimatedSerializedSizeInBytes();
    }

    @Override
    public Slice serialize()
    {
        return qdigest.serialize();
    }

    @Override
    public QuantileDigest getDigest()
    {
        return qdigest;
    }
}
