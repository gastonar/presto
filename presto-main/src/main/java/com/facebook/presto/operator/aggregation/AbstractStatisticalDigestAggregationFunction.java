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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.DEFAULT_ACCURACY;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.DEFAULT_WEIGHT;
import static com.facebook.presto.operator.scalar.TDigestFunctions.DEFAULT_COMPRESSION;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;

public abstract class AbstractStatisticalDigestAggregationFunction
        extends SqlAggregationFunction
{
    private final String name;
    private final String type;
    private final boolean tdigest;

    private static final MethodHandle INPUT_DOUBLE_T = methodHandle(TDigestAggregationFunction.class, "inputDouble", StatisticalDigestState.class, double.class, long.class, double.class);
    private static final MethodHandle INPUT_DOUBLE_Q = methodHandle(QuantileDigestAggregationFunction.class, "inputDouble", StatisticalDigestState.class, double.class, long.class, double.class);
    private static final MethodHandle INPUT_REAL = methodHandle(QuantileDigestAggregationFunction.class, "inputReal", StatisticalDigestState.class, long.class, long.class, double.class);
    private static final MethodHandle INPUT_BIGINT = methodHandle(QuantileDigestAggregationFunction.class, "inputBigint", StatisticalDigestState.class, long.class, long.class, double.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(QuantileDigestAggregationFunction.class, "combineState", StatisticalDigestState.class, StatisticalDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(QuantileDigestAggregationFunction.class, "evaluateFinal", StatisticalDigestStateSerializer.class, StatisticalDigestState.class, BlockBuilder.class);

    protected AbstractStatisticalDigestAggregationFunction(String name, String type, boolean tdigest, TypeSignature... typeSignatures)
    {
        super(
                name,
                ImmutableList.of(comparableTypeParameter("V")),
                ImmutableList.of(),
                parseTypeSignature(type + "(V)"),
                ImmutableList.copyOf(typeSignatures));
        this.name = name;
        this.type = type;
        this.tdigest = tdigest;
    }

    @Override
    public String getDescription()
    {
        return "Returns a digest from the set of reals, bigints or doubles";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type valueType = boundVariables.getTypeVariable("V");
        Type outputType = (Type) typeManager.getParameterizedType(
                type,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(name, valueType, outputType, arity);
    }

    private InternalAggregationFunction generateAggregation(String name, Type valueType, Type outputType, int arity)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(QuantileDigestAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = getInputTypes(valueType, arity);
        StatisticalDigestStateSerializer stateSerializer = new StatisticalDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();
        StatisticalDigestStateFactory statisticalFactory;
        if (tdigest) {
            statisticalFactory = StatisticalDigestStateFactory.createTDigestFactory();
        }
        else {
            statisticalFactory = StatisticalDigestStateFactory.createQuantileDigestFactory();
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputTypes),
                getMethodHandle(valueType, arity),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        StatisticalDigestState.class,
                        stateSerializer,
                        statisticalFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<Type> getInputTypes(Type valueType, int arity)
    {
        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                return ImmutableList.of(valueType);
            case 2:
                // weight specified, accuracy unspecified
                return ImmutableList.of(valueType, BIGINT);
            case 3:
                // weight and accuracy specified
                return ImmutableList.of(valueType, BIGINT, DOUBLE);
            default:
                throw new IllegalArgumentException(format("Unsupported number of arguments: %s", arity));
        }
    }

    private MethodHandle getMethodHandle(Type valueType, int arity)
    {
        final MethodHandle inputFunction;
        switch (valueType.getDisplayName()) {
            case StandardTypes.DOUBLE:
                if (tdigest) {
                    inputFunction = INPUT_DOUBLE_T;
                }
                else {
                    inputFunction = INPUT_DOUBLE_Q;
                }
                break;
            case StandardTypes.REAL:
                if (tdigest) {
                    throw new UnsupportedOperationException();
                }
                inputFunction = INPUT_REAL;
                break;
            case StandardTypes.BIGINT:
                if (tdigest) {
                    throw new UnsupportedOperationException();
                }
                inputFunction = INPUT_BIGINT;
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported type %s supplied", valueType.getDisplayName()));
        }

        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                if (tdigest) {
                    return insertArguments(inputFunction, 2, DEFAULT_WEIGHT, DEFAULT_COMPRESSION);
                }
                return insertArguments(inputFunction, 2, DEFAULT_WEIGHT, DEFAULT_ACCURACY);
            case 2:
                // weight specified, accuracy unspecified
                if (tdigest) {
                    return insertArguments(inputFunction, 3, DEFAULT_COMPRESSION);
                }
                return insertArguments(inputFunction, 3, DEFAULT_ACCURACY);
            case 3:
                // weight and accuracy specified
                return inputFunction;
            default:
                throw new IllegalArgumentException(format("Unsupported number of arguments: %s", arity));
        }
    }

    private static List<ParameterMetadata> createInputParameterMetadata(List<Type> valueTypes)
    {
        return ImmutableList.<ParameterMetadata>builder()
                .add(new ParameterMetadata(STATE))
                .addAll(valueTypes.stream().map(valueType -> new ParameterMetadata(INPUT_CHANNEL, valueType)).collect(Collectors.toList()))
                .build();
    }

    public static void combineState(StatisticalDigestState state, StatisticalDigestState otherState)
    {
        StatisticalDigest input = otherState.getStatisticalDigest();

        StatisticalDigest previous = state.getStatisticalDigest();
        if (previous == null) {
            state.setStatisticalDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void evaluateFinal(StatisticalDigestStateSerializer serializer, StatisticalDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
