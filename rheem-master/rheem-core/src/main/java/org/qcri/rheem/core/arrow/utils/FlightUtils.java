package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.example.ExampleTicket;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * 用于flight的某些工具方法
 */
public class FlightUtils {

    /**
     * 获取默认的地址
     *
     * @return Location
     */
    public static Location getDefaultLocation() {
        return Location.forGrpcInsecure("localhost", 47470);
    }

    /**
     * 获取flight默认的身份验证
     *
     * @return server授权handler
     */
    public static ServerAuthHandler getDefaultAuthHandler() {
        return new ServerAuthHandler() {//身份验证部分
            @Override
            public Optional<String> isValid(byte[] token) {
                return Optional.of("xxx");
            }

            @Override
            public boolean authenticate(ServerAuthHandler.ServerAuthSender outgoing, Iterator<byte[]> incoming) {
                incoming.next();
                outgoing.send(new byte[0]);
                return true;
            }
        };
    }

//    /**
//     * 获取指定partitionId对应的描述符，用于存储
//     *
//     * @param partitionId partitionId
//     * @return descriptor
//     */
//    public static FlightDescriptor getDefaultPartitionDescriptor(int partitionId) {
//        // 统一的标识这部分的数据
//        ExampleTicket ticket = FlightUtils.getDefaultPartitionExampleTicket(partitionId);
//        return FlightDescriptor.path(ticket.getPath());
//    }

    /**
     * 获取默认的访问描述符
     * @return descriptor
     */
    public static FlightDescriptor getDefaultDescriptor() {
        List<String> dataPath = Collections.singletonList("data_from_worker");
        return FlightDescriptor.path(dataPath);
    }

//    /**
//     * 获取指定partitionId对应的描述符，用于读取
//     * @param partitionId partitionId
//     * @return Ticket
//     */
//    public static Ticket getDefaultPartitionTicket(int partitionId) {
//        ExampleTicket ticket = FlightUtils.getDefaultPartitionExampleTicket(partitionId);
//        return ticket.toTicket();
//    }

    /**
     * 获取指定partitionId的对应访问标识
     *
     * @param partitionId partitionId
     * @return Ticket
     */
    private static ExampleTicket getDefaultPartitionExampleTicket(int partitionId) {
        // 统一的标识这部分的数据
        List<String> dataPath = Collections.singletonList("data_from_worker");
        return new ExampleTicket(dataPath, partitionId, "data_from_worker" + partitionId);
    }

}
