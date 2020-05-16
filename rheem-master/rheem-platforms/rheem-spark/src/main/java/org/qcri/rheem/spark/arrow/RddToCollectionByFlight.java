package org.qcri.rheem.spark.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.arrow.utils.FlightClientFactory;
import org.qcri.rheem.core.arrow.utils.FlightServerFactory;
import org.qcri.rheem.core.arrow.utils.FlightUtils;
import org.qcri.rheem.core.arrow.utils.VectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 将rdd转换成List
 */
public class RddToCollectionByFlight {

    private static final Logger logger = LoggerFactory.getLogger(RddToCollectionByFlight.class);

    public static List<Object> convertRddToCollection(JavaRDD<?> inputRdd) {
        // 构建server
        try (FlightServerFactory serverFactory = new FlightServerFactory();
             FlightServer server = serverFactory.newDefaultServer()) {
            // 启动server
            server.start();
            logger.info("driver start flight server");

            inputRdd.foreachPartition(iterator -> {
                // worker端，发送数据
                int partitionId = TaskContext.getPartitionId();
                logger.info("worker begin handler partition for :" + partitionId);

                // 构建client
                try (FlightClientFactory clientFactory = new FlightClientFactory();
                     FlightClient client = clientFactory.newDefaultClient()) {

                    // 标志每一个partition上的数据
//                    FlightDescriptor partitionDescriptor = FlightUtils.getDefaultPartitionDescriptor(partitionId);
                    FlightDescriptor partitionDescriptor = FlightUtils.getDefaultDescriptor();
                    // 实际存储的数据，暂时只考虑一维情况，也就是Type是个基本类型，不考虑嵌套
                    BufferAllocator fieldAllocator = new RootAllocator(Long.MAX_VALUE);
                    FieldVector partitionDataVector = VectorUtils.creativeVectorByIterator(fieldAllocator,
                            new RddArrowTypeMapping(), iterator);
                    VectorSchemaRoot partitionDataRoot = VectorSchemaRoot.of(partitionDataVector);
                    FlightClient.ClientStreamListener clientStreamListener = client.startPut(partitionDescriptor,
                            partitionDataRoot, new SyncPutListener());
                    // 发送数据
                    clientStreamListener.putNext();
                    clientStreamListener.completed();
                    // 等待数据上传完毕
                    clientStreamListener.getResult();
                }
            });

            // driver端，接收数据
            // 目前仅考虑一维list的传输，因此只有一个list
            final List<Object> collectedRdd = new ArrayList<>();
            try (FlightClientFactory clientFactory = new FlightClientFactory();
                 FlightClient client = clientFactory.newDefaultClient()) {
                // 依次获取每一个partition的数据
                FlightDescriptor partitionDescriptor = FlightUtils.getDefaultDescriptor();
                client.getInfo(partitionDescriptor)
                        .getEndpoints()
                        .forEach(flightEndpoint -> {
                            try (FlightStream flightStream = client.getStream(flightEndpoint.getTicket())) {
                                flightStream.next();
                                FieldVector vector = flightStream.getRoot().getVector(0);// 先只考虑一维
                                final List<Object> partitionList = VectorUtils.convertVectorToList(vector);
//                                logger.info("driver get data from one of a partition:" + partitionList);
                                collectedRdd.addAll(partitionList);
                                vector.close();
                            } catch (Exception e) {
                                logger.warn("driver get data fail because of " + e);
                            }
                        });
//                }
            }
            return collectedRdd;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
