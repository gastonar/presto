/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.tdigest.TDigest;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.TestMergeTDigestFunction.TDIGEST_EQUALITY;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.String.format;

public class TestTDigestAggregationFunction
        extends AbstractTestFunctions
{
    private static final Joiner ARRAY_JOINER = Joiner.on(",");
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testDoublesWithWeights()
    {
        testAggregationDouble(
                createDoublesBlock(1.0, null, 2.0, null, 3.0, null, 4.0, null, 5.0, null),
                createRLEBlock(1, 10),
                100, 1.0, 2.0, 3.0, 4.0, 5.0);
        testAggregationDouble(
                createDoublesBlock(null, null, null, null, null),
                createRLEBlock(1, 5),
                NaN);
        testAggregationDouble(
                createDoublesBlock(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0),
                createRLEBlock(1, 10),
                100, -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0);
        testAggregationDouble(
                createDoublesBlock(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
                createRLEBlock(1, 10),
                100, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        testAggregationDouble(
                createDoublesBlock(),
                createRLEBlock(1, 0),
                NaN);
        testAggregationDouble(
                createDoublesBlock(1.0),
                createRLEBlock(1, 1),
                100, 1.0);
        testAggregationDouble(
                createDoubleSequenceBlock(-1000, 1000),
                createRLEBlock(1, 2000),
                100,
                LongStream.range(-1000, 1000).asDoubleStream().toArray());
    }

    private InternalAggregationFunction getAggregationFunction(Type... type)
    {
        FunctionManager functionManager = METADATA.getFunctionManager();
        return functionManager.getAggregateFunctionImplementation(
                functionManager.lookupFunction("tdigest_agg", fromTypes(type)));
    }

    private void testAggregationDouble(Block longsBlock, Block weightsBlock, double compression, double... inputs)
    {
        // Test without weights and accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE),
                new Page(longsBlock),
                compression,
                inputs);
        // Test with weights and without accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE, BIGINT),
                new Page(longsBlock, weightsBlock),
                compression,
                inputs);
        // Test with weights and accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE, BIGINT, DOUBLE),
                new Page(longsBlock, weightsBlock, createRLEBlock(compression, longsBlock.getPositionCount())),
                compression,
                inputs);
    }

    private void testAggregationDoubles(InternalAggregationFunction function, Page page, double compression, double... inputs)
    {
        assertAggregation(function,
                TDIGEST_EQUALITY,
                "test multiple positions",
                page,
                getExpectedValueDoubles(compression, inputs));

        // test scalars
        List<Double> rows = Arrays.stream(inputs).sorted().boxed().collect(Collectors.toList());

        SqlVarbinary returned = (SqlVarbinary) AggregationTestUtils.aggregation(function, page);
        assertPercentileWithinError(StandardTypes.DOUBLE, returned, compression, rows, 0.1, 0.5, 0.9, 0.99);
    }

    private Object getExpectedValueDoubles(double compression, double... values)
    {
        if (values.length == 0) {
            return null;
        }
        TDigest tDigest = new TDigest(compression);
        Arrays.stream(values).forEach(tDigest::add);
        return new SqlVarbinary(tDigest.serialize().getBytes());
    }

    private void assertPercentileWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double... percentiles)
    {
        if (rows.isEmpty()) {
            // Nothing to assert except that the tdigest is empty
            return;
        }

        // Test each quantile individually (value_at_quantile)
        for (double percentile : percentiles) {
            assertPercentileWithinError(type, binary, error, rows, percentile);
        }

        // Test all the quantiles (values_at_quantiles)
        assertPercentilesWithinError(type, binary, error, rows, percentiles);
    }

    private void assertValueWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double value)
    {
        Number lowerBound = getLowerBound(error, rows, value);
        Number upperBound = getUpperBound(error, rows, value);

        // Check that the chosen quantile is within the upper and lower bound of the error
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) >= %s", binary.toString().replaceAll("\\s+", " "), type, value, lowerBound),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) <= %s", binary.toString().replaceAll("\\s+", " "), type, value, upperBound),
                BOOLEAN,
                true);
    }

    private void assertPercentileWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double percentile)
    {
        Number lowerBound = getLowerBound(error, rows, percentile);
        Number upperBound = getUpperBound(error, rows, percentile);

        // Check that the chosen quantile is within the upper and lower bound of the error
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) >= %s", binary.toString().replaceAll("\\s+", " "), type, percentile, lowerBound),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) <= %s", binary.toString().replaceAll("\\s+", " "), type, percentile, upperBound),
                BOOLEAN,
                true);
    }

    private void assertPercentilesWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double[] percentiles)
    {
        List<Double> boxedPercentiles = Arrays.stream(percentiles).sorted().boxed().collect(toImmutableList());
        List<Number> lowerBounds = boxedPercentiles.stream().map(percentile -> getLowerBound(error, rows, percentile)).collect(toImmutableList());
        List<Number> upperBounds = boxedPercentiles.stream().map(percentile -> getUpperBound(error, rows, percentile)).collect(toImmutableList());

        // Ensure that the lower bound of each item in the distribution is not greater than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, lowerbound) -> value >= lowerbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(lowerBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));

        // Ensure that the upper bound of each item in the distribution is not less than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, upperbound) -> value <= upperbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(upperBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));
    }

    private Number getLowerBound(double error, List<? extends Number> rows, double percentile)
    {
        int medianIndex = (int) (rows.size() * percentile);
        int marginOfError = (int) (rows.size() * error / 2);
        return rows.get(max(medianIndex - marginOfError, 0));
    }

    private Number getUpperBound(double error, List<? extends Number> rows, double percentile)
    {
        int medianIndex = (int) (rows.size() * percentile);
        int marginOfError = (int) (rows.size() * error / 2);
        return rows.get(min(medianIndex + marginOfError, rows.size() - 1));
    }
}
