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

package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.scalar.tdigest.TDigest;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class TDigestStateFactory
        implements AccumulatorStateFactory<TDigestState>
{
    @Override
    public TDigestState createSingleState()
    {
        return new SingleTDigestState();
    }

    @Override
    public Class<? extends TDigestState> getSingleStateClass()
    {
        return SingleTDigestState.class;
    }

    @Override
    public TDigestState createGroupedState()
    {
        return new GroupedTDigestState();
    }

    @Override
    public Class<? extends TDigestState> getGroupedStateClass()
    {
        return GroupedTDigestState.class;
    }

    public static class GroupedTDigestState
            extends AbstractGroupedAccumulatorState
            implements TDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTDigestState.class).instanceSize();
        private final ObjectBigArray<TDigest> tdigests = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            tdigests.ensureCapacity(size);
        }

        @Override
        public TDigest getTDigest()
        {
            return tdigests.get(getGroupId());
        }

        @Override
        public void setTDigest(TDigest value)
        {
            requireNonNull(value, "value is null");
            tdigests.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + tdigests.sizeOf();
        }
    }

    public static class SingleTDigestState
            implements TDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleTDigestState.class).instanceSize();
        private TDigest tdigest;

        @Override
        public TDigest getTDigest()
        {
            return tdigest;
        }

        @Override
        public void setTDigest(TDigest value)
        {
            tdigest = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (tdigest != null) {
                estimatedSize += tdigest.getByteSize();
            }
            return estimatedSize;
        }
    }
}
