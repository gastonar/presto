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
package com.facebook.presto;

import io.airlift.slice.Slice;

public interface StatisticalDigest<T>
{
    default void add(double value, long weight)
    {
        throw new UnsupportedOperationException("Cannot add a double to a q-digest");
    }

    default void add(long value, long weight)
    {
        throw new UnsupportedOperationException("Cannot add a long to a t-digest");
    }

    void merge(StatisticalDigest<? extends StatisticalDigest> other);

    long estimatedInMemorySizeInBytes();

    long estimatedSerializedSizeInBytes();

    Slice serialize();

    StatisticalDigest<T> getDigest();
}
