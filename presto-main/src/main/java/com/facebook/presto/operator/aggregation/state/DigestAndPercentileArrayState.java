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
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;

import java.util.List;

@AccumulatorStateMetadata(stateSerializerClass = DigestAndPercentileArrayStateSerializer.class, stateFactoryClass = DigestAndPercentileArrayStateFactory.class)
public interface DigestAndPercentileArrayState<T>
        extends AccumulatorState
{
    StatisticalDigest<T> getDigest();

    void setDigest(StatisticalDigest<T> digest);

    void setPercentiles(List<Double> percentiles);

    List<Double> getPercentiles();

    void addMemoryUsage(long value);

    StatisticalDigest<T> deserialize(Slice slice);
}
