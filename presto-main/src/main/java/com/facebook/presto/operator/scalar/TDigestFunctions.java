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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;

public final class TDigestFunctions
{
    public static final double DEFAULT_COMPRESSION = 100;
    public static final long DEFAULT_WEIGHT = 1L;

    private TDigestFunctions() {}

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantileDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return new TDigest(input).getQuantile(quantile);
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType("array(double)")
    public static Block valuesAtQuantilesDouble(@SqlType("tdigest(double)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        TDigest tDigest = new TDigest(input);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            DOUBLE.writeDouble(output, tDigest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)));
        }
        return output.build();
    }

    @ScalarFunction("quantile_at_value")
    @Description("Given an input x between min/max values of t-digest, find which quantile is represented by that value")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double quantileAtValueDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return new TDigest(input).getCdf(value);
    }

    @ScalarFunction("quantiles_at_values")
    @Description("For each input x between min/max values of t-digest, find which quantile is represented by that value")
    @SqlType("array(double)")
    @SqlNullable
    public static Block quantilesAtValuesDouble(@SqlType("tdigest(double)") Slice input, @SqlType("array(double)") Block valuesArrayBlock)
    {
        TDigest tDigest = new TDigest(input);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, valuesArrayBlock.getPositionCount());
        for (int i = 0; i < valuesArrayBlock.getPositionCount(); i++) {
            DOUBLE.writeDouble(output, tDigest.getCdf(DOUBLE.getDouble(valuesArrayBlock, i)));
        }
        return output.build();
    }

    public static long verifyTDigestParameter(long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "Value must be > 0, was %s", value);
        return value;
    }

    public static double verifyTDigestParameter(double value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "Value must be > 0, was %s", value);
        return value;
    }
}
