package org.jgroups.protocols.mongo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jgroups.Address;
import org.jgroups.protocols.PingData;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests that failures of the MongoDB driver, which are unchecked, are handled the same way as failures of a JDBC
 * driver are by the parent. Uses a failing collection, so no MongoDB instance is required.
 *
 * @author Radoslav Husar
 */
public class MONGO_PINGFailureTestCase {

    private static final String CLUSTER_NAME = "failure";
    private static final MongoException FAILURE = new MongoException("expected failure");

    private final AtomicInteger invocations = new AtomicInteger();
    private final FailingMongoPing ping = new FailingMongoPing();
    private final Address address = UUID.randomUUID();
    private final PingData data = new PingData(address, true, "A", new IpAddress(InetAddress.getLoopbackAddress(), 7800));

    @Test
    public void testWriteDoesNotThrow() {
        assertDoesNotThrow(() -> ping.write(List.of(data), CLUSTER_NAME));
        assertEquals(1, invocations.get(), "collection invocations");
    }

    @Test
    public void testRemoveDoesNotThrow() {
        assertDoesNotThrow(() -> ping.remove(CLUSTER_NAME, address));
        assertEquals(1, invocations.get(), "collection invocations");
    }

    @Test
    public void testFailuresAreRethrownAsSQLException() {
        assertCause(assertThrows(SQLException.class, () -> ping.writeToDB(data, CLUSTER_NAME)));
        assertCause(assertThrows(SQLException.class, () -> ping.delete(CLUSTER_NAME, address)));
        assertCause(assertThrows(SQLException.class, () -> ping.delete(null, CLUSTER_NAME, address)));
    }

    private static void assertCause(SQLException e) {
        assertSame(FAILURE, assertInstanceOf(MongoException.class, e.getCause()), "cause");
    }

    /**
     * Uses a failing collection and makes the methods of the parent, which are protected, accessible to the test.
     */
    private class FailingMongoPing extends MONGO_PING {
        @Override
        protected MongoCollection<Document> getCollection() {
            return failingCollection();
        }

        @Override
        protected void write(List<PingData> list, String clustername) {
            super.write(list, clustername);
        }

        @Override
        protected void remove(String clustername, Address addr) {
            super.remove(clustername, addr);
        }
    }

    @SuppressWarnings("unchecked")
    private MongoCollection<Document> failingCollection() {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(), new Class<?>[] { MongoCollection.class }, (proxy, method, args) -> {
            invocations.incrementAndGet();
            throw FAILURE;
        });
    }
}
