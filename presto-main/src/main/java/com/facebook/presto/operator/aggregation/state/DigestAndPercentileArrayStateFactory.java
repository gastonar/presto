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
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.tdigest.TDigest;
import com.facebook.presto.type.StatisticalQuantileDigest;
import com.facebook.presto.type.StatisticalTDigest;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static io.airlift.slice.SizeOf.sizeOfDoubleArray;
import static java.util.Objects.requireNonNull;

public class DigestAndPercentileArrayStateFactory<T>
        implements AccumulatorStateFactory<DigestAndPercentileArrayState>
{
    private final Function<Slice, StatisticalDigest<T>> deserializer;

    public static DigestAndPercentileArrayStateFactory<TDigest> createTDigestAndPercentileArrayStateFactory()
    {
        return new DigestAndPercentileArrayStateFactory<TDigest>((slice) -> new StatisticalTDigest(createTDigest(slice)));
    }

    public static DigestAndPercentileArrayStateFactory<QuantileDigest> createQuantileDigestAndPercentileArrayStateFactory()
    {
        return new DigestAndPercentileArrayStateFactory<QuantileDigest>((slice) -> new StatisticalQuantileDigest(new QuantileDigest(slice)));
    }

    private DigestAndPercentileArrayStateFactory(Function<Slice, StatisticalDigest<T>> deserializer)
    {
        this.deserializer = deserializer;
    }

    @Override
    public DigestAndPercentileArrayState createSingleState()
    {
        return new SingleDigestAndPercentileArrayState(deserializer);
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getSingleStateClass()
    {
        return SingleDigestAndPercentileArrayState.class;
    }

    @Override
    public DigestAndPercentileArrayState createGroupedState()
    {
        return new GroupedDigestAndPercentileArrayState(deserializer);
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileArrayState.class;
    }

    public static class GroupedDigestAndPercentileArrayState<T>
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileArrayState<T>
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedDigestAndPercentileArrayState.class).instanceSize();
        private final ObjectBigArray<StatisticalDigest<T>> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;
        private final Function<Slice, StatisticalDigest<T>> deserializer;

        public GroupedDigestAndPercentileArrayState(Function<Slice, StatisticalDigest<T>> deserializer)
        {
            this.deserializer = deserializer;
        }

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public StatisticalDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(StatisticalDigest digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
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
            return INSTANCE_SIZE + size + digests.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class SingleDigestAndPercentileArrayState<T>
            implements DigestAndPercentileArrayState<T>
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleDigestAndPercentileArrayState.class).instanceSize();
        private StatisticalDigest<T> digest;
        private List<Double> percentiles;
        private final Function<Slice, StatisticalDigest<T>> deserializer;

        public SingleDigestAndPercentileArrayState(Function<Slice, StatisticalDigest<T>> deserializer)
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
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
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
            if (percentiles != null) {
                estimatedSize += sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
