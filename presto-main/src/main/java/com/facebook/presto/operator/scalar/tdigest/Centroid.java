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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single centroid which represents a number of data points.
 */
public class Centroid
        implements Comparable<Centroid>, Serializable
{
    private static final AtomicInteger uniqueCount = new AtomicInteger(1);

    private double mean;
    private int count;

    // The ID is transient because it must be unique within a given JVM. A new
    // ID should be generated from uniqueCount when a Centroid is deserialized.
    private transient int id;

    public Centroid(double value)
    {
        start(value, 1, uniqueCount.getAndIncrement());
    }

    public Centroid(double value, int weight)
    {
        start(value, weight, uniqueCount.getAndIncrement());
    }

    public Centroid(double value, int weight, int id)
    {
        start(value, weight, id);
    }

    private void start(double value, int weight, int id)
    {
        this.id = id;
        add(value, weight);
    }

    public void add(double value, int weight)
    {
        count += weight;
        mean += weight * (value - mean) / count;
    }

    public double getMean()
    {
        return mean;
    }

    public int getWeight()
    {
        return count;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Centroid{" +
                "mean=" + mean +
                ", count=" + count +
                '}';
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Centroid centroid = (Centroid) o;
        return this.mean == centroid.getMean() && this.count == centroid.getWeight();
    }

    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") Centroid o)
    {
        int r = Double.compare(mean, o.mean);
        if (r == 0) {
            r = id - o.id;
        }
        return r;
    }
}
