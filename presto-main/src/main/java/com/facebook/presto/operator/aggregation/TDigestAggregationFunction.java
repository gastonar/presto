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

package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.TDigestState;
import com.facebook.presto.operator.aggregation.state.TDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.TDigestStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TDigestType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.tdigest.TDigest;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.scalar.TDigestFunctions.DEFAULT_COMPRESSION;
import static com.facebook.presto.operator.scalar.TDigestFunctions.DEFAULT_WEIGHT;
import static com.facebook.presto.operator.scalar.TDigestFunctions.verifyTDigestParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;

public final class TDigestAggregationFunction
        extends SqlAggregationFunction
{
    public static final TDigestAggregationFunction TDIGEST_AGG = new TDigestAggregationFunction(parseTypeSignature("V"));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT = new TDigestAggregationFunction(parseTypeSignature("V"), parseTypeSignature(StandardTypes.BIGINT));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT_AND_COMPRESSION = new TDigestAggregationFunction(parseTypeSignature("V"), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.DOUBLE));
    public static final String NAME = "tdigest_agg";

    private static final MethodHandle INPUT_DOUBLE = methodHandle(TDigestAggregationFunction.class, "inputDouble", TDigestState.class, double.class, long.class, double.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(TDigestAggregationFunction.class, "combineState", TDigestState.class, TDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(TDigestAggregationFunction.class, "evaluateFinal", TDigestStateSerializer.class, TDigestState.class, BlockBuilder.class);

    private TDigestAggregationFunction(TypeSignature... typeSignatures)
    {
        super(
                NAME,
                ImmutableList.of(comparableTypeParameter("V")),
                ImmutableList.of(),
                parseTypeSignature("tdigest(V)"),
                ImmutableList.copyOf(typeSignatures));
    }

    @Override
    public String getDescription()
    {
        return "Returns a tdigest from the set of doubles";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type valueType = boundVariables.getTypeVariable("V");
        TDigestType outputType = (TDigestType) typeManager.getParameterizedType(
                StandardTypes.TDIGEST,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(valueType, outputType, arity);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, TDigestType outputType, int arity)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(TDigestAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = getInputTypes(valueType, arity);
        TDigestStateSerializer stateSerializer = new TDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputTypes),
                getMethodHandle(valueType, arity),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        TDigestState.class,
                        stateSerializer,
                        new TDigestStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<Type> getInputTypes(Type valueType, int arity)
    {
        switch (arity) {
            case 1:
                // weight and compression unspecified
                return ImmutableList.of(valueType);
            case 2:
                // weight specified, compression unspecified
                return ImmutableList.of(valueType, BIGINT);
            case 3:
                // weight and compression specified
                return ImmutableList.of(valueType, BIGINT, DOUBLE);
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported number of arguments: %s", arity));
        }
    }

    private static MethodHandle getMethodHandle(Type valueType, int arity)
    {
        final MethodHandle inputFunction;
        switch (valueType.getDisplayName()) {
            case StandardTypes.DOUBLE:
                inputFunction = INPUT_DOUBLE;
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported type %s supplied", valueType.getDisplayName()));
        }

        switch (arity) {
            case 1:
                // weight and compression unspecified
                return insertArguments(inputFunction, 2, DEFAULT_WEIGHT, DEFAULT_COMPRESSION);
            case 2:
                // weight specified, compression unspecified
                return insertArguments(inputFunction, 3, DEFAULT_COMPRESSION);
            case 3:
                // weight and compression specified
                return inputFunction;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported number of arguments: %s", arity));
        }
    }

    private static List<ParameterMetadata> createInputParameterMetadata(List<Type> valueTypes)
    {
        return ImmutableList.<ParameterMetadata>builder()
                .add(new ParameterMetadata(STATE))
                .addAll(valueTypes.stream().map(valueType -> new ParameterMetadata(INPUT_CHANNEL, valueType)).collect(Collectors.toList()))
                .build();
    }

    public static void inputDouble(TDigestState state, double value, long weight, double compression)
    {
        TDigest tdigest = getOrCreateTDigest(state, verifyTDigestParameter(compression));
        state.addMemoryUsage(-tdigest.estimatedInMemorySizeInBytes());
        tdigest.add(value, verifyTDigestParameter(weight));
        state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
    }

    private static TDigest getOrCreateTDigest(TDigestState state, double compression)
    {
        TDigest tdigest = state.getTDigest();
        if (tdigest == null) {
            tdigest = new TDigest(compression);
            state.setTDigest(tdigest);
            state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
        }
        return tdigest;
    }

    public static void combineState(TDigestState state, TDigestState otherState)
    {
        TDigest input = otherState.getTDigest();

        TDigest previous = state.getTDigest();
        if (previous == null) {
            state.setTDigest(input);
            state.addMemoryUsage(input.estimatedSerializedSizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedSerializedSizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedSerializedSizeInBytes());
        }
    }

    public static void evaluateFinal(TDigestStateSerializer serializer, TDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
