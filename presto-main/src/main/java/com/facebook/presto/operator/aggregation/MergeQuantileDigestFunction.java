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

import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.StatisticalQuantileDigest;
import io.airlift.stats.QuantileDigest;

import static com.facebook.presto.spi.type.StandardTypes.QDIGEST;

public class MergeQuantileDigestFunction
        extends AbstractMergeStatisticalDigestFunction
{
    public static final MergeQuantileDigestFunction MERGE = new MergeQuantileDigestFunction();
    public static final String NAME = "merge";

    public MergeQuantileDigestFunction()
    {
        super(NAME, QDIGEST, false);
    }

    @InputFunction
    public static void input(Type type, StatisticalDigestState state, Block value, int index)
    {
        merge(state, new StatisticalQuantileDigest(new QuantileDigest(type.getSlice(value, index))));
    }
}
