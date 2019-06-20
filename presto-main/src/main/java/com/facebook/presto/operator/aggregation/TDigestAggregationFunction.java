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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.StatisticalDigest;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.tdigest.TDigest;
import com.facebook.presto.type.StatisticalTDigest;

import static com.facebook.presto.operator.scalar.TDigestFunctions.verifyTDigestParameter;
import static com.facebook.presto.spi.type.StandardTypes.TDIGEST;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class TDigestAggregationFunction
        extends AbstractStatisticalDigestAggregationFunction
{
    public static final TDigestAggregationFunction TDIGEST_AGG = new TDigestAggregationFunction(parseTypeSignature("V"));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT = new TDigestAggregationFunction(parseTypeSignature("V"), parseTypeSignature(StandardTypes.BIGINT));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT_AND_COMPRESSION = new TDigestAggregationFunction(parseTypeSignature("V"), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.DOUBLE));

    public static final String NAME = "tdigest_agg";

    public TDigestAggregationFunction(TypeSignature... typeSignatures)
    {
        super(NAME, TDIGEST, true, typeSignatures);
    }

    @Override
    public String getDescription()
    {
        return "Returns a tdigest from the set of doubles";
    }

    public static void inputDouble(StatisticalDigestState state, double value, long weight, double parameter)
    {
        StatisticalDigest digest = getOrCreateTDigest(state, verifyTDigestParameter(parameter));
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, verifyTDigestParameter(weight));
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    private static StatisticalDigest getOrCreateTDigest(StatisticalDigestState state, double parameter)
    {
        StatisticalDigest digest = state.getStatisticalDigest();
        if (digest == null) {
            digest = new StatisticalTDigest(new TDigest(parameter));
            state.setStatisticalDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }
        return digest;
    }
}
