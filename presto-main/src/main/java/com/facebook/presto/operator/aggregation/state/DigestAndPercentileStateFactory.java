/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.StatisticalDigest;
import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.tdigest.TDigest;
import com.facebook.presto.type.StatisticalQuantileDigest;
import com.facebook.presto.type.StatisticalTDigest;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.Function;

import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static java.util.Objects.requireNonNull;

public class DigestAndPercentileStateFactory<T>
        implements AccumulatorStateFactory<DigestAndPercentileState>
{
    private final Function<Slice, StatisticalDigest<T>> deserializer;

    public static DigestAndPercentileStateFactory<TDigest> createTDigestAndPercentileArrayStateFactory()
    {
        return new DigestAndPercentileStateFactory<TDigest>((slice) -> new StatisticalTDigest(createTDigest(slice)));
    }

    public static DigestAndPercentileStateFactory<QuantileDigest> createQuantileDigestAndPercentileArrayStateFactory()
    {
        return new DigestAndPercentileStateFactory<QuantileDigest>((slice) -> new StatisticalQuantileDigest(new QuantileDigest(slice)));
    }

    private DigestAndPercentileStateFactory(Function<Slice, StatisticalDigest<T>> deserializer)
    {
        this.deserializer = deserializer;
    }

    @Override
    public DigestAndPercentileState createSingleState()
    {
        return new SingleDigestAndPercentileState(deserializer);
    }

    @Override
    public Class<? extends DigestAndPercentileState> getSingleStateClass()
    {
        return SingleDigestAndPercentileState.class;
    }

    @Override
    public DigestAndPercentileState createGroupedState()
    {
        return new GroupedDigestAndPercentileState(deserializer);
    }

    @Override
    public Class<? extends DigestAndPercentileState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileState.class;
    }

    public static class GroupedDigestAndPercentileState<T>
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedDigestAndPercentileState.class).instanceSize();
        private final ObjectBigArray<StatisticalDigest<T>> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;
        private final Function<Slice, StatisticalDigest<T>> deserializer;

        public GroupedDigestAndPercentileState(Function<Slice, StatisticalDigest<T>> deserializer)
        {
            this.deserializer = deserializer;
        }

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public StatisticalDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(StatisticalDigest digest)
        {
            requireNonNull(digest, "value is null");
            digests.set(getGroupId(), digest);
        }

        @Override
        public double getPercentile()
        {
            return percentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double percentile)
        {
            percentiles.set(getGroupId(), percentile);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public StatisticalDigest deserialize(Slice slice)
        {
            return deserializer.apply(slice);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class SingleDigestAndPercentileState<T>
            implements DigestAndPercentileState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleDigestAndPercentileState.class).instanceSize();
        private StatisticalDigest<T> digest;
        private double percentile;
        private final Function<Slice, StatisticalDigest<T>> deserializer;

        public SingleDigestAndPercentileState(Function<Slice, StatisticalDigest<T>> deserializer)
        {
            this.deserializer = deserializer;
        }

        @Override
        public StatisticalDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(StatisticalDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public StatisticalDigest deserialize(Slice slice)
        {
            return deserializer.apply(slice);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }
    }
}
