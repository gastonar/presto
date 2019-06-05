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

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
 *
 * The special characteristics of this algorithm are:
 *
 * - smaller summaries than Q-digest
 *
 * - works on doubles as well as integers.
 *
 * - provides part per million accuracy for extreme quantiles and typically &lt;1000 ppm accuracy for middle quantiles
 *
 * - fast
 *
 * - simple
 *
 * - test coverage roughly at 90%
 *
 * - easy to adapt for use with map-reduce
 */

public abstract class TDigest
        implements Serializable
{
    protected ScaleFunction scale = ScaleFunction.K_2;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;

    final Random gen = new Random();
    boolean recordAllData;

    /**
     * Creates an {@link MergingDigest}.  This is generally the best known implementation right now.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the MergingDigest
     */
    @SuppressWarnings("WeakerAccess")
    public static TDigest createMergingDigest(double compression)
    {
        return new MergingDigest(compression);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     * @param w The weight of this point.
     */
    public abstract void add(double x, int w);

    abstract void add(double x, int w, Centroid base);

    public void add(double x)
    {
        add(x, 1);
    }

    public void add(TDigest other)
    {
        List<Centroid> tmp = new ArrayList<>();
        for (Centroid centroid : other.centroids()) {
            tmp.add(centroid);
        }

        Collections.shuffle(tmp, gen);
        for (Centroid centroid : tmp) {
            add(centroid.mean(), centroid.count(), centroid);
        }
    }

    public abstract void add(List<? extends TDigest> others);

    final void checkValue(double x)
    {
        if (Double.isNaN(x)) {
            throw new IllegalArgumentException("Cannot add NaN");
        }
    }

    /**
     * Re-examines a t-digest to determine whether some centroids are redundant.  If your data are
     * perversely ordered, this may be a good idea.  Even if not, this may save 20% or so in space.
     *
     * The cost is roughly the same as adding as many data points as there are centroids.  This
     * is typically &lt; 10 * compression, but could be as high as 100 * compression.
     *
     * This is a destructive operation that is not thread-safe.
     */
    public abstract void compress();

    /**
     * Returns the number of points that have been added to this TDigest.
     *
     * @return The sum of the weights on all centroids.
     */
    public abstract long size();

    /**
     * Returns the fraction of all points added which are &le; x.
     *
     * @param x The cutoff for the cdf.
     * @return The fraction of all data which is less or equal to x.
     */
    public abstract double cdf(double x);

    /**
     * Returns an estimate of the cutoff such that a specified fraction of the data
     * added to this TDigest would be less than or equal to the cutoff.
     *
     * @param q The desired fraction
     * @return The value x such that cdf(x) == q
     */
    public abstract double quantile(double q);

    /**
     * A {@link Collection} that lets you go through the centroids in ascending order by mean.  Centroids
     * returned will not be re-used, but may or may not share storage with this TDigest.
     *
     * @return The centroids in the form of a Collection.
     */
    public abstract Collection<Centroid> centroids();

    /**
     * Returns the current compression factor.
     *
     * @return The compression factor originally used to set up the TDigest.
     */
    public abstract double compression();

    /**
     * Returns the number of bytes required to encode this TDigest using #asBytes().
     *
     * @return The number of bytes required.
     */
    public abstract int byteSize();

    /**
     * Returns the number of bytes required to encode this TDigest using #asSmallBytes().
     *
     * Note that this is just as expensive as actually compressing the digest. If you don't
     * care about time, but want to never over-allocate, this is fine. If you care about compression
     * and speed, you pretty much just have to overallocate by using allocating #byteSize() bytes.
     *
     * @return The number of bytes required.
     */
    public abstract int smallByteSize();

    public void setScaleFunction(ScaleFunction scaleFunction)
    {
        if (scaleFunction.toString().endsWith("NO_NORM")) {
            throw new IllegalArgumentException(
                    String.format("Can't use %s as scale with %s", scaleFunction, this.getClass()));
        }
        this.scale = scaleFunction;
    }

    /**
     * Same as {@link #weightedAverageSorted(double, double, double, double)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
     */
    static double weightedAverage(double x1, double w1, double x2, double w2)
    {
        if (x1 <= x2) {
            return weightedAverageSorted(x1, w1, x2, w2);
        }
        else {
            return weightedAverageSorted(x2, w2, x1, w1);
        }
    }

    /**
     * Compute the weighted average between <code>x1</code> with a weight of
     * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
     * This expects <code>x1</code> to be less than or equal to <code>x2</code>
     * and is guaranteed to return a number between <code>x1</code> and
     * <code>x2</code>.
     */
    private static double weightedAverageSorted(double x1, double w1, double x2, double w2)
    {
        Preconditions.checkArgument(x1 <= x2, "error");
        final double x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return Math.max(x1, Math.min(x, x2));
    }

    /**
     * Serialize this TDigest into a Slice.  Note that the serialization used is
     * very straightforward and is considerably larger than strictly necessary.
     *
     * @return slice The Slice representation of the serialized TDigest.
     */
    public abstract Slice serialize();

    /**
     * Sets up so that all centroids will record all data assigned to them.  For testing only, really.
     */
    public TDigest recordAllData()
    {
        recordAllData = true;
        return this;
    }

    public boolean isRecording()
    {
        return recordAllData;
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     */

    public abstract int centroidCount();

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    /**
     * Over-ride the min and max values for testing purposes
     */
    @SuppressWarnings("SameParameterValue")
    void setMinMax(double min, double max)
    {
        this.min = min;
        this.max = max;
    }
}
