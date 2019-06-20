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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.TDigestState;
import com.facebook.presto.operator.aggregation.state.TDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.TDigestStateSerializer;
import com.facebook.presto.operator.scalar.tdigest.TDigest;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TDigestType;
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
import static com.google.common.base.Preconditions.checkArgument;

@AggregationFunction("merge_tdigest")
public final class MergeTDigestFunction
        extends SqlAggregationFunction
{
    public static final MergeTDigestFunction MERGE = new MergeTDigestFunction();
    public static final String NAME = "merge";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeTDigestFunction.class, "input", Type.class, TDigestState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MergeTDigestFunction.class, "combine", TDigestState.class, TDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MergeTDigestFunction.class, "output", TDigestStateSerializer.class, TDigestState.class, BlockBuilder.class);

    public MergeTDigestFunction()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                parseTypeSignature("tdigest(T)"),
                ImmutableList.of(parseTypeSignature("tdigest(T)")));
    }

    @Override
    public String getDescription()
    {
        return "Merges the input t digests into a single t digest";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        TDigestType outputType = (TDigestType) typeManager.getParameterizedType(StandardTypes.TDIGEST,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(outputType);
    }

    private static InternalAggregationFunction generateAggregation(TDigestType type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        TDigestStateSerializer stateSerializer = new TDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION.bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        TDigestState.class,
                        stateSerializer,
                        new TDigestStateFactory())),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, ImmutableList.of(type), ImmutableList.of(intermediateType), type, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type valueType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, valueType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    @InputFunction
    public static void input(Type type, TDigestState state, Block value, int index)
    {
        merge(state, new TDigest(type.getSlice(value, index)));
    }

    @CombineFunction
    public static void combine(TDigestState state, TDigestState otherState)
    {
        merge(state, otherState.getTDigest());
    }

    private static void merge(TDigestState state, TDigest input)
    {
        if (input == null) {
            return;
        }
        TDigest previous = state.getTDigest();
        if (previous == null) {
            state.setTDigest(input);
            state.addMemoryUsage(input.getByteSize());
        }
        else {
            checkArgument(previous.getCompressionFactor() == input.getCompressionFactor(),
                    String.format("Cannot merge tdigests with different compressions (%s vs. %s)", state.getTDigest().getCompressionFactor(), input.getCompressionFactor()));
            state.addMemoryUsage(-previous.getByteSize());
            previous.add(input);
            state.addMemoryUsage(previous.getByteSize());
        }
    }

    public static void output(TDigestStateSerializer serializer, TDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
