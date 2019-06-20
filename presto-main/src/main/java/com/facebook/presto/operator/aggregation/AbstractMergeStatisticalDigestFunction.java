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

import com.facebook.presto.StatisticalDigest;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

@AggregationFunction("merge")
public abstract class AbstractMergeStatisticalDigestFunction
        extends SqlAggregationFunction
{
    public final String name;
    public final String type;
    public final boolean tdigest;

    private static final MethodHandle INPUT_FUNCTION_T = methodHandle(MergeTDigestFunction.class, "input", Type.class, StatisticalDigestState.class, Block.class, int.class);
    private static final MethodHandle INPUT_FUNCTION_Q = methodHandle(MergeQuantileDigestFunction.class, "input", Type.class, StatisticalDigestState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMergeStatisticalDigestFunction.class, "combine", StatisticalDigestState.class, StatisticalDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMergeStatisticalDigestFunction.class, "output", StatisticalDigestStateSerializer.class, StatisticalDigestState.class, BlockBuilder.class);

    protected AbstractMergeStatisticalDigestFunction(String name, String type, boolean tdigest)
    {
        super(name,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                parseTypeSignature(type + "(T)"),
                ImmutableList.of(parseTypeSignature(type + "(T)")));
        this.name = name;
        this.type = type;
        this.tdigest = tdigest;
    }

    @Override
    public String getDescription()
    {
        return "Merges the input quantile digests into a single digest";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        Type outputType = typeManager.getParameterizedType(type,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(outputType);
    }

    private InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        StatisticalDigestStateSerializer stateSerializer = new StatisticalDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();
        StatisticalDigestStateFactory stateFactory;

        if (tdigest) {
            stateFactory = StatisticalDigestStateFactory.createTDigestFactory();
        }
        else {
            stateFactory = StatisticalDigestStateFactory.createQuantileDigestFactory();
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature())),
                createInputParameterMetadata(type),
                getMethodHandle().bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                    StatisticalDigestState.class,
                    stateSerializer,
                    stateFactory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, ImmutableList.of(type), ImmutableList.of(intermediateType), type, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type valueType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, valueType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    private MethodHandle getMethodHandle()
    {
        if (tdigest) {
            return INPUT_FUNCTION_T;
        }
        else {
            return INPUT_FUNCTION_Q;
        }
    }

    @CombineFunction
    public static void combine(StatisticalDigestState state, StatisticalDigestState otherState)
    {
        merge(state, otherState.getStatisticalDigest());
    }

    protected static void merge(StatisticalDigestState state, StatisticalDigest input)
    {
        if (input == null) {
            return;
        }
        StatisticalDigest previous = state.getStatisticalDigest();
        if (previous == null) {
            state.setStatisticalDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input.getDigest());
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void output(StatisticalDigestStateSerializer serializer, StatisticalDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
