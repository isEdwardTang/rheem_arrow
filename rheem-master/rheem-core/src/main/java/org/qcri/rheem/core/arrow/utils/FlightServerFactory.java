package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.example.InMemoryStore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * 用于创建flight server的工厂类
 */
public class FlightServerFactory implements AutoCloseable {

    private final BufferAllocator allocator = new RootAllocator();

    /**
     * 创建默认的server
     *
     * @return flight server
     */
    public FlightServer newDefaultServer() {
        Location location = FlightUtils.getDefaultLocation();
        return FlightServer.builder()
                .allocator(allocator)
                .location(location)
                .authHandler(FlightUtils.getDefaultAuthHandler())
                .producer(new InMemoryStore(allocator, location))
                .build();
    }

    @Override
    public void close() {
//        allocator.close();
    }
}
