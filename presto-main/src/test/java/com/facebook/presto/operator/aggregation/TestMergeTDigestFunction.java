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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.tdigest.TDigest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.TDigestParametricType.TDIGEST;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class TestMergeTDigestFunction
        extends AbstractTestAggregationFunction
{
    public static final BiFunction<Object, Object, Boolean> TDIGEST_EQUALITY = (actualBinary, expectedBinary) -> {
        if (actualBinary == null && expectedBinary == null) {
            return true;
        }
        requireNonNull(actualBinary, "actual value was null");
        requireNonNull(expectedBinary, "expected value was null");

        TDigest actual = createTDigest(wrappedBuffer(((SqlVarbinary) actualBinary).getBytes()));
        TDigest expected = createTDigest(wrappedBuffer(((SqlVarbinary) expectedBinary).getBytes()));
        return actual.getSize() == expected.getSize() &&
                actual.getMin() == expected.getMin() &&
                actual.getMax() == expected.getMax() &&
                actual.getCompressionFactor() == expected.getCompressionFactor();
    };

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        Type type = TDIGEST.createType(typeRegistry, ImmutableList.of(TypeParameter.of(DoubleType.DOUBLE)));
        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            TDigest tdigest = new TDigest(100);
            tdigest.add(i);
            type.writeSlice(blockBuilder, tdigest.serialize());
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "merge";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("tdigest(double)");
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        TDigest tdigest = new TDigest(100);
        for (int i = start; i < start + length; i++) {
            tdigest.add(i);
        }
        return new SqlVarbinary(tdigest.serialize().getBytes());
    }

    @Test
    @Override
    public void testMultiplePositions()
    {
        assertAggregation(getFunction(),
                TDIGEST_EQUALITY,
                "test multiple positions",
                new Page(getSequenceBlocks(0, 5)),
                getExpectedValue(0, 5));
    }

    @Test
    @Override
    public void testMixedNullAndNonNullPositions()
    {
        assertAggregation(getFunction(),
                TDIGEST_EQUALITY,
                "test mixed null and nonnull position",
                new Page(createAlternatingNullsBlock(getFunction().getParameterTypes(), getSequenceBlocks(0, 10))),
                getExpectedValueIncludingNulls(0, 10, 20));
    }
}
