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
    public void merge(StatisticalDigest other)
    {
        StatisticalQuantileDigest toMerge = (StatisticalQuantileDigest) other;
        qdigest.merge(toMerge.qdigest);
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
    public StatisticalDigest<QuantileDigest> getDigest()
    {
        return new StatisticalQuantileDigest(qdigest);
    }
}
