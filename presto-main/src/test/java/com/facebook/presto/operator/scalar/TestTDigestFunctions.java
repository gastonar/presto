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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.tdigest.TDigest;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.sort;

public class TestTDigestFunctions
        extends AbstractTestFunctions
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;
    private static final double STANDARD_ERROR = 0.01;
    private final double[] quantiles = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
            0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

    private static final Joiner ARRAY_JOINER = Joiner.on(",");
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test(enabled = true)
    public void testMergeTwoNormalDistributionsGetQuantile()
    {
        TDigest tDigest1 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

        tDigest1.add(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertValueWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testNormalDistributionHighVarianceValuesArray()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
            list.add((double) i);
        }

        sort(list);

        assertBlockQuantiles(quantiles, STANDARD_ERROR, list, tDigest);
    }

    @Test(invocationCount = 5)
    public void testNormalDistributionHighVarianceQuantileArray()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
            list.add(i);
        }

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test(invocationCount = 5)
    public void testMergeTwoDistributionsWithoutOverlap()
    {
        TDigest tDigest1 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i + NUMBER_OF_ENTRIES / 2);
            list.add(i);
            list.add(i + NUMBER_OF_ENTRIES / 2);
        }

        tDigest1.add(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test(invocationCount = 5)
    public void testMergeTwoDistributionsWithOverlap()
    {
        TDigest tDigest1 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i);
            list.add(i);
            list.add(i);
        }

        tDigest2.add(tDigest1);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertDiscreteQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest2);
        }
    }

    @Test (invocationCount = 5)
    public void testAddElementsRandomized()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

    @Test (invocationCount = 5)
    public void testNormalDistributionLowVariance()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

    @Test(invocationCount = 5)
    public void testNormalDistributionHighVariance()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

    @Test(invocationCount = 5)
    public void testMergeTwoNormalDistributions()
    {
        TDigest tDigest1 = new TDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

        tDigest1.add(tDigest2);
        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testMergeManySmallNormalDistributions()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 100_000;

        for (int k = 0; k < digests; k++) {
            TDigest current = new TDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < 10; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.add(current);
        }

        sort(list);

        for (int i = 0; i < quantiles.length; i++) {
            assertContinuousQuantileWithinBound(quantiles[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeManyLargeNormalDistributions()
    {
        TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 1000;

        for (int k = 0; k < digests; k++) {
            TDigest current = new TDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < NUMBER_OF_ENTRIES / digests; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.add(current);
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
            TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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
            TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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
            TDigest tDigest = new TDigest(STANDARD_COMPRESSION_FACTOR);
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

    private String getValueProjectionString(TDigest tDigest, double quantile)
    {
        Slice firstSerial = tDigest.serialize();

        byte[] firstBytes = firstSerial.getBytes();

        String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

        return "value_at_quantile(CAST(X'" + firstEncode + "' AS tdigest(double)), " + quantile + ")";
    }

    private String getValueProjectionString(TDigest tDigest, double[] quantile)
    {
        Slice firstSerial = tDigest.serialize();

        byte[] firstBytes = firstSerial.getBytes();

        String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

        String projection = "values_at_quantiles(CAST(X'" + firstEncode + "' AS tdigest(double)), ARRAY[";

        ImmutableList.Builder<String> arrayValues = ImmutableList.builder();

        for (int i = 0; i < quantile.length; i++) {
            arrayValues.add("" + quantile[i]);
        }

        projection += Joiner.on(", ").join(arrayValues.build());
        projection += "])";

        return projection;
    }

    private String getQuantileProjectionString(TDigest tDigest, double value)
    {
        Slice firstSerial = tDigest.serialize();

        byte[] firstBytes = firstSerial.getBytes();

        String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

        return "quantile_at_value(CAST(X'" + firstEncode + "' AS tdigest(double)), " + value + ")";
    }

    private String getQuantileProjectionString(TDigest tDigest, double[] values)
    {
        Slice firstSerial = tDigest.serialize();

        byte[] firstBytes = firstSerial.getBytes();

        String firstEncode = BaseEncoding.base16().lowerCase().encode(firstBytes);

        String projection = "quantiles_at_values(CAST(X'" + firstEncode + "' AS tdigest(double)), ARRAY[";

        ImmutableList.Builder<String> arrayValues = ImmutableList.builder();

        for (int i = 0; i < values.length; i++) {
            arrayValues.add("" + values[i]);
        }

        projection += Joiner.on(", ").join(arrayValues.build());
        projection += "])";

        return projection;
    }

    private void assertValueWithinBound(double quantile, double bound, List<Double> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        list.get((int) (NUMBER_OF_ENTRIES * quantile)),
                        getUpperBoundQuantile(quantile, bound)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                   format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) >= %s",
                           new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                           DOUBLE,
                           list.get((int) (NUMBER_OF_ENTRIES * quantile)),
                           getLowerBoundQuantile(quantile, bound)),
                   BOOLEAN,
                   true);
    }

    private void assertDiscreteQuantileWithinBound(double quantile, double bound, List<Integer> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("round(value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s)) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getUpperBoundValue(quantile, bound, list)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("round(value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s)) >= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getLowerBoundValue(quantile, bound, list)),
                BOOLEAN,
                true);
    }

    private void assertContinuousQuantileWithinBound(double quantile, double bound, List<Double> list, TDigest tDigest)
    {
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) <= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getUpperBoundValue(quantile, bound, list)),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS tdigest(%s)), %s) >= %s",
                        new SqlVarbinary(tDigest.serialize().getBytes()).toString().replaceAll("\\s+", " "),
                        DOUBLE,
                        quantile,
                        getLowerBoundValue(quantile, bound, list)),
                BOOLEAN,
                true);
    }

    private double getLowerBoundValue(double quantile, double bound, List<? extends Number> values)
    {
        return values.get((int) Math.max(NUMBER_OF_ENTRIES * (quantile - bound), 0)).doubleValue();
    }

    private double getUpperBoundValue(double quantile, double bound, List<? extends Number> values)
    {
        return values.get((int) Math.min(NUMBER_OF_ENTRIES * (quantile + bound), values.size() - 1)).doubleValue();
    }
    private double getLowerBoundQuantile(double quantile, double bound)
    {
        return Math.max(0, quantile - bound);
    }

    private double getUpperBoundQuantile(double quantile, double bound)
    {
        return Math.min(1, quantile + bound);
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
