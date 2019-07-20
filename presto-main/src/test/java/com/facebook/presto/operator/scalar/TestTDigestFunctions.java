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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tdigest.TDigest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.facebook.presto.Session.builder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.sort;

public class TestTDigestFunctions
        extends AbstractTestFunctions
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;
    private static final double STANDARD_ERROR = 0.01;
    private static final double[] quantiles = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
            0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
    private static final List<List<Double>> quantileBoundPairs = ImmutableList.of(
            ImmutableList.of(0.4, 0.6),
            ImmutableList.of(0.0, 0.95),
            ImmutableList.of(0.0, 0.1),
            ImmutableList.of(0.2, 0.3),
            ImmutableList.of(0.01, 0.05),
            ImmutableList.of(0.95, 0.99),
            ImmutableList.of(0.1, 0.9));

    private static final Joiner ARRAY_JOINER = Joiner.on(",");
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    private TestTDigestFunctions()
    {
        super(builder(TEST_SESSION).setSystemProperty("tdigest_enabled", "true").build());
    }

    @Test
    public void testNullTDigestGetValueAtQuantile()
    {
        functionAssertions.assertFunction("value_at_quantile(CAST(NULL AS tdigest(double)), 0.3)", DOUBLE, null);
    }

    @Test
    public void testNullTDigestGetQuantileAtValue()
    {
        functionAssertions.assertFunction("quantile_at_value(CAST(NULL AS tdigest(double)), 0.3)", DOUBLE, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetValueAtQuantileOverOne()
    {
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS tdigest(double)), 1.5)",
                new SqlVarbinary(createTDigest(STANDARD_COMPRESSION_FACTOR).serialize().getBytes()).toString().replaceAll("\\s+", " ")),
                DOUBLE,
                null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetValueAtQuantileBelowZero()
    {
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS tdigest(double)), -0.2)",
                new SqlVarbinary(createTDigest(STANDARD_COMPRESSION_FACTOR).serialize().getBytes()).toString().replaceAll("\\s+", " ")),
                DOUBLE,
                null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidSerializationFormat()
    {
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS tdigest(double)), 0.5)",
                new SqlVarbinary(createTDigest(STANDARD_COMPRESSION_FACTOR).serialize().getBytes()).toString().substring(0, 80).replaceAll("\\s+", " ")),
                DOUBLE,
                null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptySerialization()
    {
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS tdigest(double)), 0.5)",
                new SqlVarbinary(new byte[0])),
                DOUBLE,
                null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTDigestNaNCompression()
    {
        functionAssertions.assertFunction(format("value_at_quantile(CAST(X'%s' AS tdigest(double)), 0.2)",
                new SqlVarbinary(createTDigest(NaN).serialize().getBytes()).toString().replaceAll("\\s+", " ")),
                DOUBLE,
                null);
    }

    @Test
    public void testMergeTwoNormalDistributionsGetQuantile()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(0, 50);

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            double value1 = normal.sample();
            double value2 = normal.sample();
            tDigest1.add(value1);
            tDigest2.add(value2);
            list.add(value1);
            list.add(value2);
        }

        tDigest1.merge(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertValueWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testGetQuantileAtValueOutsideRange()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = Math.random() * NUMBER_OF_ENTRIES;
            tDigest.add(value);
        }

        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) = 1",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        1_000_000_000d),
                BOOLEAN,
                true);

        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) = 0",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        -500d),
                BOOLEAN,
                true);
    }

    @Test
    public void testNormalDistributionHighVarianceValuesArray()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        NormalDistribution normal = new NormalDistribution(0, 1);
        List<Double> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        sort(list);

        double[] values = new double[quantiles.length];

        for (int i = 0; i < quantiles.length; i++) {
            values[i] = list.get((int) (quantiles[i] * NUMBER_OF_ENTRIES));
        }

        assertBlockValues(values, STANDARD_ERROR, tDigest);
    }

    @Test
    public void testAddElementsInOrderQuantileArray()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
            list.add((double) i);
        }

        sort(list);

        assertBlockQuantiles(quantiles, STANDARD_ERROR, list, tDigest);
    }

    @Test
    public void testNormalDistributionHighVarianceQuantileArray()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(0, 1);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        sort(list);

        assertBlockQuantiles(quantiles, STANDARD_ERROR, list, tDigest);
    }

    @Test
    public void testAddElementsInOrder()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
            list.add(i);
        }

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeTwoDistributionsWithoutOverlap()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i + NUMBER_OF_ENTRIES / 2);
            list.add(i);
            list.add(i + NUMBER_OF_ENTRIES / 2);
        }

        tDigest1.merge(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testMergeTwoDistributionsWithOverlap()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i);
            list.add(i);
            list.add(i);
        }

        tDigest2.merge(tDigest1);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest2);
        }
    }

    @Test
    public void testAddElementsRandomized()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = Math.random() * NUMBER_OF_ENTRIES;
            tDigest.add(value);
            list.add(value);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testNormalDistributionLowVariance()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(1000, 1);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testNormalDistributionHighVariance()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(0, 1);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeTwoNormalDistributions()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(0, 50);

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            double value1 = normal.sample();
            double value2 = normal.sample();
            tDigest1.add(value1);
            tDigest2.add(value2);
            list.add(value1);
            list.add(value2);
        }

        tDigest1.merge(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testMergeManySmallNormalDistributions()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 100_000;

        for (int k = 0; k < digests; k++) {
            TDigest current = createTDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < 10; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.merge(current);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeManyLargeNormalDistributions()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 1000;

        for (int k = 0; k < digests; k++) {
            TDigest current = createTDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < NUMBER_OF_ENTRIES / digests; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.merge(current);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    // disabled because test takes almost 10s
    @Test(enabled = false)
    public void testBinomialDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            BinomialDistribution binomial = new BinomialDistribution(trials, k * 0.1);
            List<Integer> list = new ArrayList<>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = binomial.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            Collections.sort(list);

            for (int i = 0; i < quantiles.length; i++) {
                assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    @Test(enabled = false)
    public void testGeometricDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            GeometricDistribution geometric = new GeometricDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = geometric.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            Collections.sort(list);

            for (int i = 0; i < quantiles.length; i++) {
                assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    @Test(enabled = false)
    public void testPoissonDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            PoissonDistribution poisson = new PoissonDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = poisson.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            Collections.sort(list);

            for (int i = 0; i < quantiles.length; i++) {
                assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    @Test
    public void testTruncatedMean()
    {
        for (List<Double> quantileBound : quantileBoundPairs) {
            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(STANDARD_COMPRESSION_FACTOR, 1.0, 2.0, 3.0, 4.0, 5.0),
                    STANDARD_ERROR,
                    quantileBound,
                    1.0, 2.0, 3.0, 4.0, 5.0);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(STANDARD_COMPRESSION_FACTOR),
                    STANDARD_ERROR,
                    quantileBound);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(STANDARD_COMPRESSION_FACTOR, -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0),
                    STANDARD_ERROR,
                    quantileBound,
                    -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(STANDARD_COMPRESSION_FACTOR, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
                    STANDARD_ERROR,
                    quantileBound,
                    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(NaN),
                    STANDARD_ERROR,
                    quantileBound);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(100, 1.0),
                    STANDARD_ERROR,
                    quantileBound,
                    1.0);

            assertTruncatedMeanWithinError(
                    DOUBLE,
                    getExpectedValueDoubles(100, LongStream.range(-1000, 1000).asDoubleStream().toArray()),
                    STANDARD_ERROR,
                    quantileBound,
                    LongStream.range(-1000, 1000).asDoubleStream().toArray());
        }
    }

    private SqlVarbinary getExpectedValueDoubles(double compression, double... values)
    {
        if (values.length == 0) {
            return null;
        }

        TDigest tDigest = createTDigest(compression);

        for (double value : values) {
            tDigest.add(value);
        }
        return new SqlVarbinary(tDigest.serialize().getBytes());
    }

    private void assertTruncatedMeanWithinError(Type type, SqlVarbinary binary, double error, List<Double> quantileBounds, double... inputs)
    {
        List<Double> rows = Arrays.stream(inputs).sorted().boxed().collect(Collectors.toList());

        if (rows.isEmpty()) {
            return;
        }

        Double lowerQuantile = quantileBounds.get(0);
        Double upperQuantile = quantileBounds.get(1);
        Double truncatedLowerMean = getTruncatedMean(rows, max(0, lowerQuantile - error), upperQuantile);
        Double truncatedUpperMean = getTruncatedMean(rows, lowerQuantile, min(upperQuantile + error, 1));

        functionAssertions.assertFunction(
                format("truncated_mean(CAST(X'%s' AS tdigest(%s)), %s, %s) >= %s",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        lowerQuantile,
                        upperQuantile,
                        truncatedLowerMean),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("truncated_mean(CAST(X'%s' AS tdigest(%s)), %s, %s) <= %s",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        lowerQuantile,
                        upperQuantile,
                        truncatedUpperMean),
                BOOLEAN,
                true);
    }

    private Double getTruncatedMean(List<Double> rows, double lowerQuantile, double upperQuantile)
    {
        double sum = 0;
        for (int i = 0; i < rows.size(); i++) {
            double l = (double) i / rows.size();
            double h = (double) (i + 1) / rows.size();
            double overlap = max(min(h, upperQuantile) - max(l, lowerQuantile), 0);

            double val = rows.get(min(i, rows.size() - 1));
            sum += val * overlap;
        }
        return sum / ((upperQuantile - lowerQuantile));
    }

    private void assertValueWithinBound(double quantile, double error, List<Double> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        list.get((int) (NUMBER_OF_ENTRIES * quantile)),
                        getUpperBoundQuantile(quantile, error)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) >= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        list.get((int) (NUMBER_OF_ENTRIES * quantile)),
                        getLowerBoundQuantile(quantile, error)),
                BOOLEAN,
                true);
    }

    private void assertDiscreteQuantileWithinBound(double quantile, double error, List<Integer> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("round(value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s)) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getUpperBoundValue(quantile, error, list)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("round(value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s)) >= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getLowerBoundValue(quantile, error, list)),
                BOOLEAN,
                true);
    }

    private void assertContinuousQuantileWithinBound(double quantile, double error, List<Double> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getUpperBoundValue(quantile, error, list)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) >= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getLowerBoundValue(quantile, error, list)),
                BOOLEAN,
                true);
    }

    private double getLowerBoundValue(double quantile, double error, List<? extends Number> values)
    {
        return values.get((int) Math.max(NUMBER_OF_ENTRIES * (quantile - error), 0)).doubleValue();
    }

    private double getUpperBoundValue(double quantile, double error, List<? extends Number> values)
    {
        return values.get((int) Math.min(NUMBER_OF_ENTRIES * (quantile + error), values.size() - 1)).doubleValue();
    }
    private double getLowerBoundQuantile(double quantile, double error)
    {
        return Math.max(0, quantile - error);
    }

    private double getUpperBoundQuantile(double quantile, double error)
    {
        return Math.min(1, quantile + error);
    }

    private void assertBlockQuantiles(double[] percentiles, double error, List<? extends Number> rows, TDigest tDigest)
    {
        List<Double> boxedPercentiles = Arrays.stream(percentiles).sorted().boxed().collect(toImmutableList());
        List<Number> lowerBounds = boxedPercentiles.stream().map(percentile -> getLowerBoundValue(percentile, error, rows)).collect(toImmutableList());
        List<Number> upperBounds = boxedPercentiles.stream().map(percentile -> getUpperBoundValue(percentile, error, rows)).collect(toImmutableList());

        // Ensure that the lower bound of each item in the distribution is not greater than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, lowerbound) -> value >= lowerbound)",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        "double",
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(lowerBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));

        // Ensure that the upper bound of each item in the distribution is not less than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, upperbound) -> value <= upperbound)",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        "double",
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(upperBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));
    }

    private void assertBlockValues(double[] values, double error, TDigest tDigest)
    {
        List<Double> boxedValues = Arrays.stream(values).sorted().boxed().collect(toImmutableList());
        List<Double> boxedPercentiles = Arrays.stream(quantiles).sorted().boxed().collect(toImmutableList());
        List<Number> lowerBounds = boxedPercentiles.stream().map(percentile -> getLowerBoundQuantile(percentile, error)).collect(toImmutableList());
        List<Number> upperBounds = boxedPercentiles.stream().map(percentile -> getUpperBoundQuantile(percentile, error)).collect(toImmutableList());

        // Ensure that the lower bound of each item in the distribution is not greater than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(quantiles_at_values(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, lowerbound) -> value >= lowerbound)",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        "double",
                        ARRAY_JOINER.join(boxedValues),
                        ARRAY_JOINER.join(lowerBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(values.length, true));

        // Ensure that the upper bound of each item in the distribution is not less than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(quantiles_at_values(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, upperbound) -> value <= upperbound)",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        "double",
                        ARRAY_JOINER.join(boxedValues),
                        ARRAY_JOINER.join(upperBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(values.length, true));
    }
}
