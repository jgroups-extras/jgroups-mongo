package org.jgroups.protocols.mongo;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.codecs.StringCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Tests against a containerized MongoDB database instance.
 *
 * @author Radoslav Husar
 */
public class MONGO_PINGDiscoveryTestCase {

    private static MongoDBContainer mongoDBContainer;

    @BeforeAll
    public static void setUp() {
        assumeTrue(isDockerAvailable(), "Podman/Docker environment is not available - skipping tests against MongoDB container.");

        mongoDBContainer = new MongoDBContainer("mongo:8.0")
                // Workaround for MongoDB 8.x refusing to start on Linux kernel 6.19+ (e.g. GitHub Actions runners on 7.0.0-*-azure).
                // See https://jira.mongodb.org/browse/SERVER-121912 and https://jira.mongodb.org/browse/SERVER-131779
                // TODO Remove once a fixed MongoDB release (8.0.35+) is used.
                .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");
        mongoDBContainer.start();

        // Configure the protocol with the container's connection string
        System.setProperty("jgroups.mongo.connection_url", mongoDBContainer.getConnectionString() + "/jgroups");
    }

    @AfterAll
    public static void cleanup() {
        if (mongoDBContainer != null) {
            mongoDBContainer.stop();
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }

    public static final int CHANNEL_COUNT = 5;
    private static final long TIMEOUT_MS = 10_000;
    private static final long INTERVAL_MS = 100;

    // The cluster names need to randomized so that multiple test runs can be run in parallel.
    public static final String RANDOM_CLUSTER_NAME = UUID.randomUUID().toString();

    @FunctionalInterface
    interface ChannelCreator {
        JChannel create() throws Exception;
    }

    @Test
    public void testDiscovery() throws Exception {
        discover(RANDOM_CLUSTER_NAME, this::createFromXml);
    }

    @Test
    public void testDiscoveryObscureClusterName() throws Exception {
        String obscureClusterName = "``\\//--+ěščřžýáíé==''!@#$%^&*()_{}<>?";
        discover(obscureClusterName + RANDOM_CLUSTER_NAME, this::createFromXml);
    }

    @Test
    public void testDiscoveryProgrammaticConfiguration() throws Exception {
        discover("programmatic-" + RANDOM_CLUSTER_NAME, this::createProgrammatically);
    }

    /**
     * Disconnecting and reconnecting a channel calls stop() and start() on the protocol without init() and destroy(),
     * so the MongoDB client must survive stop().
     */
    @Test
    public void testDisconnectAndReconnect() throws Exception {
        String clusterName = "reconnect-" + RANDOM_CLUSTER_NAME;

        try (JChannel first = createFromXml(); JChannel second = createFromXml()) {
            first.connect(clusterName);
            second.connect(clusterName);
            Util.waitUntilAllChannelsHaveSameView(TIMEOUT_MS, INTERVAL_MS, first, second);

            second.disconnect();
            Util.waitUntil(TIMEOUT_MS, INTERVAL_MS, () -> first.getView().size() == 1, () -> "first channel view: " + first.getView());

            second.connect(clusterName);

            // stop() must not have closed the client, otherwise this fails with "state should be: open"
            MONGO_PING ping = second.getProtocolStack().findProtocol(MONGO_PING.class);
            assertDoesNotThrow(() -> ping.getCollection().countDocuments(), "MongoDB client is not usable after reconnect");

            Util.waitUntilAllChannelsHaveSameView(TIMEOUT_MS, INTERVAL_MS, first, second);

            printViews(List.of(first, second));
            assertEquals(2, first.getView().size(), "member count");
            assertEquals(2, second.getView().size(), "member count");
        }
    }

    /**
     * The client survives stop() (see testDisconnectAndReconnect), so closing the channel must close it in destroy().
     */
    @Test
    public void testCloseClosesClient() throws Exception {
        // Closing an already closed channel is a no-op, so this only matters if the test fails before close()
        try (JChannel channel = createFromXml()) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            channel.connect("close-" + RANDOM_CLUSTER_NAME);
            var collection = ping.getCollection();

            channel.close();

            assertNull(ping.getCollection(), "collection after close");
            assertThrows(IllegalStateException.class, collection::countDocuments, "client must be closed");
        }
    }

    @Test
    public void testDiscoveryWithSharedDatabase() throws Exception {
        try (MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            discover("shared-" + RANDOM_CLUSTER_NAME, () -> createWithSharedDatabase(sharedClient));

            // Closed channels must not have closed the shared client
            assertDoesNotThrow(() -> sharedClient.getDatabase("jgroups").runCommand(new Document("ping", 1)));
        }
    }

    /**
     * connection_url is optional when a shared database is set.
     */
    @Test
    public void testDiscoveryWithSharedDatabaseWithoutConnectionUrl() throws Exception {
        try (MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            discover("shared-no-url-" + RANDOM_CLUSTER_NAME, () -> {
                JChannel channel = createProgrammatically(null);
                MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
                ping.setMongoDatabase(sharedClient.getDatabase("jgroups"));
                return channel;
            });
        }
    }

    /**
     * Reads from secondaries could be stale, so the read preference of the shared database must not be inherited.
     */
    @Test
    public void testSharedDatabaseReadPreferenceIsNotInherited() throws Exception {
        try (MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString());
             JChannel channel = createFromXml()) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            ping.setMongoDatabase(sharedClient.getDatabase("jgroups").withReadPreference(ReadPreference.secondaryPreferred()));
            channel.connect("read-preference-" + RANDOM_CLUSTER_NAME);

            assertEquals(ReadPreference.primary(), ping.getCollection().getReadPreference());
        }
    }

    @Test
    public void testSetMongoDatabaseAfterConnectIsRejected() throws Exception {
        try (JChannel channel = createFromXml();
             MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            channel.connect("late-database-" + RANDOM_CLUSTER_NAME);

            assertThrows(IllegalStateException.class, () -> ping.setMongoDatabase(sharedClient.getDatabase("jgroups")));

            // A disconnected channel keeps its collection for the next connect, so the database still cannot be replaced
            channel.disconnect();
            assertThrows(IllegalStateException.class, () -> ping.setMongoDatabase(sharedClient.getDatabase("jgroups")));
        }
    }

    /**
     * A custom codec registry might not be able to encode Documents, so the one of the shared database must not be inherited.
     */
    @Test
    public void testSharedDatabaseCodecRegistryIsNotInherited() throws Exception {
        try (MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString());
             JChannel channel = createFromXml()) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            // A registry that cannot encode Documents
            ping.setMongoDatabase(sharedClient.getDatabase("jgroups").withCodecRegistry(CodecRegistries.fromCodecs(new StringCodec())));
            channel.connect("codec-registry-" + RANDOM_CLUSTER_NAME);

            // The driver wraps the registry, so check what it can do rather than compare it to the default one
            assertDoesNotThrow(() -> ping.getCollection().getCodecRegistry().get(Document.class));
            // The local member's entry was written and can be read
            assertEquals(1, ping.readFromDB(channel.getClusterName()).size(), "entries");
        }
    }

    /**
     * Disconnecting and reconnecting must keep using the shared database, and neither must close the shared client.
     */
    @Test
    public void testDisconnectAndReconnectWithSharedDatabase() throws Exception {
        String clusterName = "shared-reconnect-" + RANDOM_CLUSTER_NAME;

        try (MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            try (JChannel channel = createWithSharedDatabase(sharedClient)) {
                MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
                channel.connect(clusterName);
                var collection = ping.getCollection();

                channel.disconnect();
                channel.connect(clusterName);

                assertSame(collection, ping.getCollection(), "collection after reconnect");
                assertEquals(1, ping.readFromDB(clusterName).size(), "entries");
            }

            assertDoesNotThrow(() -> sharedClient.getDatabase("jgroups").runCommand(new Document("ping", 1)));
        }
    }

    /**
     * connection_url is only needed on connect, so it can still be set after the channel was created.
     */
    @Test
    public void testConnectionUrlSetAfterChannelCreation() throws Exception {
        try (JChannel channel = createProgrammatically(null)) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            ping.setConnectionUrl(mongoDBContainer.getConnectionString() + "/jgroups");

            channel.connect("late-url-" + RANDOM_CLUSTER_NAME);

            assertNotNull(ping.getCollection(), "collection after connect");
        }
    }

    /**
     * The owner of a shared client might close it before the channel (e.g., from a JVM shutdown hook); removing the
     * local entry must then not throw, as that would abort the shutdown of the protocol stack.
     */
    @Test
    public void testRemoveWithClosedSharedClient() throws Exception {
        MongoClient sharedClient = MongoClients.create(mongoDBContainer.getConnectionString());
        try (sharedClient; JChannel channel = createWithSharedDatabase(sharedClient)) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            String clusterName = "closed-shared-client-" + RANDOM_CLUSTER_NAME;
            channel.connect(clusterName);

            sharedClient.close();

            assertDoesNotThrow(() -> ping.remove(clusterName, channel.getAddress()));
        }
    }

    /**
     * The collection only exists once the channel is connected; operations before that fail with a descriptive error.
     */
    @Test
    public void testDumpBeforeConnectIsRejected() throws Exception {
        try (JChannel channel = createFromXml()) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);

            assertThrows(IllegalStateException.class, () -> ping.dump("not-connected-" + RANDOM_CLUSTER_NAME));
        }
    }

    /**
     * The own client is created in start(), so that a shared database can be set on a channel created from XML
     * (JChannel calls init() from its constructor).
     */
    @Test
    public void testCollectionIsResolvedOnConnect() throws Exception {
        try (JChannel channel = createFromXml()) {
            MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
            assertNull(ping.getCollection(), "collection before connect");

            channel.connect("resolve-on-connect-" + RANDOM_CLUSTER_NAME);

            assertNotNull(ping.getCollection(), "collection after connect");
        }
    }

    @Test
    public void testConnectWithoutConnectionUrlOrSharedDatabaseIsRejected() throws Exception {
        try (JChannel channel = createProgrammatically(null)) {
            assertThrows(IllegalStateException.class, () -> channel.connect("no-database-" + RANDOM_CLUSTER_NAME));
        }
    }

    private void discover(String clusterName, ChannelCreator creator) throws Exception {
        List<JChannel> channels = new LinkedList<>();

        for (int i = 0; i < CHANNEL_COUNT; i++) {
            JChannel channel = creator.create();
            channel.connect(clusterName);
            if (i == 0) {
                // Let's be clear about the coordinator
                Util.sleep(1000);
            }
            channels.add(channel);
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        printViews(channels);

        // Asserts the views are there
        for (JChannel channel : channels) {
            assertEquals(CHANNEL_COUNT, channel.getView().getMembers().size(), "member count");
        }

        // Stop all channels
        // n.b. all channels must be closed, only disconnecting all concurrently can leave stale data
        for (JChannel channel : channels) {
            channel.close();
        }
    }

    private JChannel createFromXml() throws Exception {
        return new JChannel("org/jgroups/protocols/mongo/tcp-MONGO_PING.xml");
    }

    private JChannel createWithSharedDatabase(MongoClient sharedClient) throws Exception {
        JChannel channel = createFromXml();
        MONGO_PING ping = channel.getProtocolStack().findProtocol(MONGO_PING.class);
        ping.setMongoDatabase(sharedClient.getDatabase("jgroups"));
        return channel;
    }

    private JChannel createProgrammatically() throws Exception {
        return createProgrammatically(mongoDBContainer.getConnectionString() + "/jgroups");
    }

    private JChannel createProgrammatically(String connectionUrl) throws Exception {
        String customCollectionName = "custom-ping-collection";

        var channel = new JChannel(
                new TCP(),
                new MONGO_PING()
                        .setConnectionUrl(connectionUrl)
                        .setCollectionName(customCollectionName),
                new MERGE3(),
                new FD_SOCK2(),
                new FD_ALL3(),
                new VERIFY_SUSPECT2(),
                new BARRIER(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new MFC(),
                new UFC(),
                new FRAG2(),
                new STATE_TRANSFER()
        );

        // Verify and call the getter
        MONGO_PING mongoPing = channel.getProtocolStack().findProtocol(MONGO_PING.class);
        assertEquals(customCollectionName, mongoPing.getCollectionName());

        return channel;
    }

    protected static void printViews(List<JChannel> channels) {
        for (JChannel ch : channels) {
            System.out.println("Channel " + ch.getName() + " has view " + ch.getView());
        }
    }
}
