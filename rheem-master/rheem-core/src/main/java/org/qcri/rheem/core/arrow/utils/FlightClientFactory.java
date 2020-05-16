package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * 用于创建flight client的工厂类
 */
public class FlightClientFactory implements AutoCloseable {

    private final BufferAllocator allocator = new RootAllocator();

    /**
     * 创建默认的server
     *
     * @return flight server
     */
    public FlightClient newDefaultClient() {
        return FlightClient.builder()
                .allocator(allocator)
                .location(FlightUtils.getDefaultLocation())
                .build();
    }

    @Override
    public void close() {
        allocator.close();
    }
}
