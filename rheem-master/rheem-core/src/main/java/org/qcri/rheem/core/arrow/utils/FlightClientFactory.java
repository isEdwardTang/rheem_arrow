package org.qcri.rheem.core.arrow.utils;

import java.util.Iterator;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class FlightClientFactory implements AutoCloseable {
    private final BufferAllocator allocator = new RootAllocator();
    private final Location defaultLocation;
    private final String username;
    private final String password;
    private final boolean parallel;

    public FlightClientFactory(Location defaultLocation, String username,
                               String password, boolean parallel) {
        this.defaultLocation = defaultLocation;
        this.username = username;
        this.password = password.equals("$NULL$") ? null : password;
        this.parallel = parallel;
    }

    /**
     * 用于创建一个flight的client
     *
     * @return FlightClient
     */
    public FlightClient apply() {
        FlightClient client = FlightClient.builder(allocator, defaultLocation).build();
        client.authenticateBasic(username, password);
        if (parallel) {
            Iterator<Result> res = client.doAction(new Action("PARALLEL"));
            res.forEachRemaining(Object::toString);
        }
        return client;

    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isParallel() {
        return parallel;
    }

    @Override
    public void close() throws Exception {
        allocator.close();
    }
}

