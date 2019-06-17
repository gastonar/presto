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

package com.facebook.presto.operator.scalar.tdigest;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.kToQ;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.max;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.normalizer;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.qToK;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.reverse;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.sort;
import static com.facebook.presto.operator.scalar.tdigest.TDigestUtils.weightedAverage;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static java.util.Collections.shuffle;

/**
 * Maintains a t-digest by collecting new points in a buffer that is then sorted occasionally and merged
 * into a sorted array that contains previously computed centroids.
 * <p>
 * This can be very fast because the cost of sorting and merging is amortized over several insertion. If
 * we keep N centroids total and have the input array is k long, then the amortized cost is something like
 * <p>
 * N/k + log k
 * <p>
 * These costs even out when N/k = log k.  Balancing costs is often a good place to start in optimizing an
 * algorithm.
 * The virtues of this kind of t-digest implementation include:
 * <ul>
 * <li>No allocation is required after initialization</li>
 * <li>The data structure automatically compresses existing centroids when possible</li>
 * <li>No Java object overhead is incurred for centroids since data is kept in primitive arrays</li>
 * </ul>
 * <p>
 */

public class TDigest
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TDigest.class).instanceSize();

    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    private final Random random = ThreadLocalRandom.current();

    private int mergeCount;

    private final double publicCompression;
    private final double compression;

    // points to the first unused centroid
    private int activeCentroids;

    private double totalWeight;

    private final double[] weight;

    private final double[] mean;

    private double unmergedWeight;

    // this is the index of the next temporary centroid
    // this is a more Java-like convention than activeCentroids uses
    private int tempUsed;
    private final double[] tempWeight;
    private final double[] tempMean;

    // array used for sorting the temp centroids
    // to avoid allocations during operation
    private final int[] order;

    public TDigest(double compression)
    {
        // ensure compression >= 10
        // default size = 2 * ceil(compression)
        // default bufferSize = 5 * size
        // scale = max(2, bufferSize / size - 1)
        // compression, publicCompression = sqrt(scale-1)*compression, compression
        // ensure size > 2 * compression + weightLimitFudge
        // ensure bufferSize > 2*size

        if (compression < 10) {
            compression = 10;
        }

        double sizeFudge = 10;
        if (compression < 30) {
            sizeFudge += 20;
        }

        // publicCompression is how many centroids the user asked for
        // compression is how many we actually keep
        this.publicCompression = compression;

        int size = (int) Math.ceil(this.publicCompression + sizeFudge);
        int bufferSize = 5 * size;

        // scale is the ratio of extra buffer to the final size
        // we have to account for the fact that we copy all live centroids into the incoming space
        double scale = Math.max(1, bufferSize / size - 1);

        this.compression = Math.sqrt(scale) * publicCompression;

        size = (int) Math.ceil(this.compression + sizeFudge);

        weight = new double[size];
        mean = new double[size];

        tempWeight = new double[bufferSize];
        tempMean = new double[bufferSize];
        order = new int[bufferSize];

        activeCentroids = 0;
    }

    public TDigest(Slice slice)
    {
        SliceInput sliceInput = new BasicSliceInput(slice);
        byte format = sliceInput.readByte();
        checkArgument(format == 0, "Invalid format");
        byte type = sliceInput.readByte();
        checkArgument(type == 0, "Invalid type; expecting '0' (type double)");
        this.min = sliceInput.readDouble();
        this.max = sliceInput.readDouble();
        this.publicCompression = sliceInput.readDouble();
        this.totalWeight = sliceInput.readDouble();
        this.activeCentroids = sliceInput.readInt();

        double sizeFudge = 10;
        if (this.publicCompression < 30) {
            sizeFudge += 20;
        }

        int size = (int) Math.ceil(this.publicCompression + sizeFudge);
        int bufferSize = 5 * size;

        // scale is the ratio of extra buffer to the final size
        // we have to account for the fact that we copy all live centroids into the incoming space
        double scale = Math.max(1, bufferSize / size - 1);

        this.compression = Math.sqrt(scale) * publicCompression;

        size = (int) Math.ceil(this.compression + sizeFudge);

        this.weight = new double[size];
        this.mean = new double[size];

        this.tempWeight = new double[bufferSize];
        this.tempMean = new double[bufferSize];
        this.order = new int[bufferSize];

        sliceInput.readBytes(wrappedDoubleArray(this.weight), this.activeCentroids * SIZE_OF_DOUBLE);
        sliceInput.readBytes(wrappedDoubleArray(this.mean), this.activeCentroids * SIZE_OF_DOUBLE);
        sliceInput.close();
    }

    public void add(double value)
    {
        add(value, 1);
    }

    public void add(double mean, long weight)
    {
        checkArgument(!Double.isNaN(mean), "Cannot add NaN to t-digest");

        if (tempUsed >= tempWeight.length - activeCentroids - 1) {
            mergeNewValues();
        }
        int where = tempUsed++;
        tempWeight[where] = weight;
        tempMean[where] = mean;
        unmergedWeight += weight;
        if (mean < min) {
            min = mean;
        }
        if (mean > max) {
            max = mean;
        }
    }

    private void add(double[] mean, double[] weight, int count)
    {
        checkArgument(mean.length == weight.length, "Arrays not same length");

        if (mean.length < count + activeCentroids) {
            // make room to add existing centroids
            double[] mean1 = new double[count + activeCentroids];
            System.arraycopy(mean, 0, mean1, 0, count);
            mean = mean1;
            double[] weight1 = new double[count + activeCentroids];
            System.arraycopy(weight, 0, weight1, 0, count);
            weight = weight1;
        }
        double total = 0;
        for (int i = 0; i < count; i++) {
            total += weight[i];
        }
        merge(mean, weight, count, null, total, false, compression);
    }

    public void add(TDigest other)
    {
        List<Centroid> tmp = new ArrayList<>();
        for (Centroid centroid : other.centroids()) {
            tmp.add(centroid);
        }

        shuffle(tmp, random);
        for (Centroid centroid : tmp) {
            add(centroid.getMean(), centroid.getWeight());
        }
    }

    private void mergeNewValues()
    {
        mergeNewValues(false, compression);
    }

    private void mergeNewValues(boolean force, double compression)
    {
        if (totalWeight == 0 && unmergedWeight == 0) {
            return;
        }
        if (force || unmergedWeight > 0) {
            // note that we run the merge in reverse every other merge to avoid left-to-right bias in merging
            merge(tempMean, tempWeight, tempUsed, order, unmergedWeight,
                    mergeCount % 2 == 1, compression);
            mergeCount++;
            tempUsed = 0;
            unmergedWeight = 0;
        }
    }

    private void merge(double[] incomingMean, double[] incomingWeight, int incomingCount, int[] incomingOrder,
            double unmergedWeight, boolean runBackwards, double compression)
    {
        System.arraycopy(mean, 0, incomingMean, incomingCount, activeCentroids);
        System.arraycopy(weight, 0, incomingWeight, incomingCount, activeCentroids);
        incomingCount += activeCentroids;

        if (incomingOrder == null) {
            incomingOrder = new int[incomingCount];
        }
        sort(incomingOrder, incomingMean, incomingCount);

        if (runBackwards) {
            reverse(incomingOrder, 0, incomingCount);
        }

        totalWeight += unmergedWeight;

        checkArgument((activeCentroids + incomingCount) > 0, "error");
        activeCentroids = 0;
        mean[activeCentroids] = incomingMean[incomingOrder[0]];
        weight[activeCentroids] = incomingWeight[incomingOrder[0]];
        double weightSoFar = 0;

        double normalizer = normalizer(compression, totalWeight);
        double k1 = qToK(0, normalizer);
        double weightLimit = totalWeight * kToQ(k1 + 1, normalizer);
        for (int i = 1; i < incomingCount; i++) {
            int ix = incomingOrder[i];
            double proposedWeight = weight[activeCentroids] + incomingWeight[ix];
            double projectedWeight = weightSoFar + proposedWeight;
            boolean addThis;

            double q0 = weightSoFar / totalWeight;
            double q2 = (weightSoFar + proposedWeight) / totalWeight;
            addThis = proposedWeight <= totalWeight * Math.min(max(q0, normalizer), max(q2, normalizer));

            if (addThis) {
                // next point will fit so merge into existing centroid
                weight[activeCentroids] += incomingWeight[ix];
                mean[activeCentroids] = mean[activeCentroids] + (incomingMean[ix] - mean[activeCentroids]) * incomingWeight[ix] / weight[activeCentroids];
                incomingWeight[ix] = 0;
            }
            else {
                // didn't fit ... move to next output, copy out first centroid
                weightSoFar += weight[activeCentroids];

                activeCentroids++;
                mean[activeCentroids] = incomingMean[ix];
                weight[activeCentroids] = incomingWeight[ix];
                incomingWeight[ix] = 0;
            }
        }
        activeCentroids++;

        // sanity check
        double sum = 0;
        for (int i = 0; i < activeCentroids; i++) {
            sum += weight[i];
        }

        checkArgument(sum == totalWeight, "error");
        if (runBackwards) {
            reverse(mean, 0, activeCentroids);
            reverse(weight, 0, activeCentroids);
        }

        if (totalWeight > 0) {
            min = Math.min(min, mean[0]);
            max = Math.max(max, mean[activeCentroids - 1]);
        }
    }

    /**
     * Merges any pending inputs and compresses the data down to the public setting.
     */
    public void compress()
    {
        mergeNewValues(true, publicCompression);
    }

    public long getSize()
    {
        return (long) (totalWeight + unmergedWeight);
    }

    public double getCdf(double x)
    {
        checkArgument(x >= min && x <= max, String.format("X must be in range of t-digest, between[%s, %s]", min, max));
        compress();

        if (activeCentroids == 0) {
            return Double.NaN;
        }
        else if (activeCentroids == 1) {
            double width = max - min;
            if (x < min) {
                return 0;
            }
            else if (x > max) {
                return 1;
            }
            else if (x - min <= width) {
                // min and max are too close together to do any viable interpolation
                return 0.5;
            }
            else {
                return (x - min) / (max - min);
            }
        }
        else {
            int n = activeCentroids;
            if (x < min) {
                return 0;
            }

            if (x > max) {
                return 1;
            }

            // check for the left tail
            if (x < mean[0]) {
                // guarantees we divide by non-zero number and interpolation works
                if (mean[0] - min > 0) {
                    // must be a sample exactly at min
                    if (x == min) {
                        return 0.5 / totalWeight;
                    }
                    else {
                        return (1 + (x - min) / (mean[0] - min) * (weight[0] / 2 - 1)) / totalWeight;
                    }
                }
                else {
                    return 0;
                }
            }
            checkArgument(x >= mean[0], "error");

            // and the right tail
            if (x > mean[n - 1]) {
                if (max - mean[n - 1] > 0) {
                    if (x == max) {
                        return 1 - 0.5 / totalWeight;
                    }
                    else {
                        // there has to be a single sample exactly at max
                        double dq = (1 + (max - x) / (max - mean[n - 1]) * (weight[n - 1] / 2 - 1)) / totalWeight;
                        return 1 - dq;
                    }
                }
                else {
                    return 1;
                }
            }

            // we know that there are at least two centroids and mean[0] < x < mean[n-1]
            // that means that there are either one or more consecutive centroids all at exactly x
            // or there are consecutive centroids, c0 < x < c1
            double weightSoFar = 0;
            for (int it = 0; it < n - 1; it++) {
                // weightSoFar does not include weight[it] yet
                if (mean[it] == x) {
                    // dw will accumulate the weight of all of the centroids at x
                    double dw = 0;
                    while (it < n && mean[it] == x) {
                        dw += weight[it];
                        it++;
                    }
                    return (weightSoFar + dw / 2) / totalWeight;
                }
                else if (mean[it] <= x && x < mean[it + 1]) {
                    // landed between centroids
                    if (mean[it + 1] - mean[it] > 0) {
                        // no interpolation needed if we have a singleton centroid
                        double leftExcludedW = 0;
                        double rightExcludedW = 0;
                        if (weight[it] == 1) {
                            if (weight[it + 1] == 1) {
                                // two singletons means no interpolation
                                // left singleton is in, right is out
                                return (weightSoFar + 1) / totalWeight;
                            }
                            else {
                                leftExcludedW = 0.5;
                            }
                        }
                        else if (weight[it + 1] == 1) {
                            rightExcludedW = 0.5;
                        }
                        double dw = (weight[it] + weight[it + 1]) / 2;

                        checkArgument(dw > 1, "error");
                        checkArgument((leftExcludedW + rightExcludedW) <= 0.5, "error");

                        // adjust endpoints for any singleton
                        double left = mean[it];
                        double right = mean[it + 1];

                        double dwNoSingleton = dw - leftExcludedW - rightExcludedW;

                        checkArgument(dwNoSingleton > dw / 2, "error");
                        checkArgument(right - left > 0, "error");

                        double base = weightSoFar + weight[it] / 2 + leftExcludedW;
                        return (base + dwNoSingleton * (x - left) / (right - left)) / totalWeight;
                    }
                    else {
                        // caution against floating point madness
                        double dw = (weight[it] + weight[it + 1]) / 2;
                        return (weightSoFar + dw) / totalWeight;
                    }
                }
                else {
                    weightSoFar += weight[it];
                }
            }
            checkArgument(x == mean[n - 1], "Can't happen ... loop fell through");

            return 1 - 0.5 / totalWeight;
        }
    }

    public double getQuantile(double q)
    {
        checkArgument(q >= 0 && q <= 1, "q should be in [0,1], got " + q);

        compress();

        if (activeCentroids == 0) {
            return Double.NaN;
        }
        else if (activeCentroids == 1) {
            return mean[0];
        }

        int n = activeCentroids;

        final double index = q * totalWeight;

        if (index < 1) {
            return min;
        }

        // if the left centroid has more than one sample, we still know
        // that one sample occurred at min so we can do some interpolation
        if (weight[0] > 1 && index < weight[0] / 2) {
            // there is a single sample at min so we interpolate with less weight
            return min + (index - 1) / (weight[0] / 2 - 1) * (mean[0] - min);
        }

        if (index > totalWeight - 1) {
            return max;
        }

        // if the right-most centroid has more than one sample, we still know
        // that one sample occurred at max so we can do some interpolation
        if (weight[n - 1] > 1 && totalWeight - index <= weight[n - 1] / 2) {
            return max - (totalWeight - index - 1) / (weight[n - 1] / 2 - 1) * (max - mean[n - 1]);
        }

        // in between extremes we interpolate between centroids
        double weightSoFar = weight[0] / 2;
        for (int i = 0; i < n - 1; i++) {
            double dw = (weight[i] + weight[i + 1]) / 2;
            if (weightSoFar + dw > index) {
                // centroids i and i+1 bracket our current point

                // check for unit weight
                double leftUnit = 0;
                if (weight[i] == 1) {
                    if (index - weightSoFar < 0.5) {
                        // within the singleton's sphere
                        return mean[i];
                    }
                    else {
                        leftUnit = 0.5;
                    }
                }
                double rightUnit = 0;
                if (weight[i + 1] == 1) {
                    if (weightSoFar + dw - index <= 0.5) {
                        // no interpolation needed near singleton
                        return mean[i + 1];
                    }
                    rightUnit = 0.5;
                }
                double z1 = index - weightSoFar - leftUnit;
                double z2 = weightSoFar + dw - index - rightUnit;
                return weightedAverage(mean[i], z2, mean[i + 1], z1);
            }
            weightSoFar += dw;
        }

        checkArgument(weight[n - 1] > 1, "error");
        checkArgument(index <= totalWeight, "error");
        checkArgument(index >= totalWeight - weight[n - 1] / 2, "error");

        // weightSoFar = totalWeight - weight[n-1]/2 (very nearly)
        // so we interpolate out to max value ever seen
        double z1 = index - totalWeight - weight[n - 1] / 2.0;
        double z2 = weight[n - 1] / 2 - z1;
        return weightedAverage(mean[n - 1], z1, max, z2);
    }

    public int centroidCount()
    {
        mergeNewValues();
        return activeCentroids;
    }

    public Collection<Centroid> centroids()
    {
        compress();
        return new AbstractCollection<Centroid>()
        {
            @Override
            public Iterator<Centroid> iterator()
            {
                return new Iterator<Centroid>()
                {
                    int i;

                    @Override
                    public boolean hasNext()
                    {
                        return i < activeCentroids;
                    }

                    @Override
                    public Centroid next()
                    {
                        Centroid rc = new Centroid(mean[i], (int) weight[i]);
                        i++;
                        return rc;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }

            @Override
            public int size()
            {
                return activeCentroids;
            }
        };
    }

    public double getCompressionFactor()
    {
        return publicCompression;
    }

    public int getByteSize()
    {
        compress();
        return SIZE_OF_BYTE                             // format
                + SIZE_OF_BYTE                          // type (e.g double, float, bigint)
                + SIZE_OF_DOUBLE                        // min
                + SIZE_OF_DOUBLE                        // max
                + SIZE_OF_DOUBLE                        // compression factor
                + SIZE_OF_DOUBLE                        // total weight
                + SIZE_OF_INT                           // number of centroids
                + SIZE_OF_DOUBLE * activeCentroids      // weight[], containing weight of each centroid
                + SIZE_OF_DOUBLE * activeCentroids;     // mean[], containing mean of each centroid
    }

    public long estimatedInMemorySize()
    {
        compress();
        return INSTANCE_SIZE + sizeOf(weight) + sizeOf(mean) + sizeOf(tempWeight) + sizeOf(tempMean);
    }

    public Slice serialize()
    {
        SliceOutput sliceBuilder = new DynamicSliceOutput(getByteSize());

        compress();
        sliceBuilder.writeByte(0); // version 0 of T-Digest serialization                     // 1
        sliceBuilder.writeByte(0); // represents the underlying data type of the distribution // + 1
        sliceBuilder.writeDouble(min);                                                              // + 8
        sliceBuilder.writeDouble(max);                                                              // + 8
        sliceBuilder.writeDouble(publicCompression);                                                // + 8
        sliceBuilder.writeDouble(totalWeight);                                                      // + 8
        sliceBuilder.writeInt(activeCentroids);                                                     // + 4 = 38
        sliceBuilder.writeBytes(wrappedDoubleArray(weight), 0, activeCentroids * SIZE_OF_DOUBLE);
        sliceBuilder.writeBytes(wrappedDoubleArray(mean), 0, activeCentroids * SIZE_OF_DOUBLE);
        return sliceBuilder.slice();
    }

    @SuppressWarnings("SameParameterValue")
    void setMinMax(double min, double max)
    {
        this.min = min;
        this.max = max;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public String toString()
    {
        return String.format("TDigest\nCompression:%s\nCentroid Count:%s\nSize:%s\nMin:%s Median:%s Max:%s",
                publicCompression, activeCentroids, totalWeight, min, getQuantile(0.5), max);
    }
}
