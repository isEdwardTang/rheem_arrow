package org.qcri.rheem.spark.operators;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.Validate;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.arrow.utils.FlightClientFactory;
import org.qcri.rheem.core.arrow.utils.VectorUtils;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.spark.arrow.RddArrowTypeMapping;
import org.qcri.rheem.spark.arrow.RddToCollectionByFlight;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.*;


/**
 * 实现 从 spark 到 javastream的直接转换，而不是使用collection作为中介
 * @param <Type>
 */
public class SparkToStreamOperator<Type>
        extends UnaryToUnaryOperator<Type, Type>
        implements SparkExecutionOperator {

    public SparkToStreamOperator(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        // 直接将 rdd 的数据 通过 arrow 传给java stream
        JavaRDD<Type> inputRdd = input.provideRdd();
        List<?> collectedRdd = RddToCollectionByFlight.convertRddToCollection(inputRdd);
        output.accept(collectedRdd);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.stream.load";
    }
}
