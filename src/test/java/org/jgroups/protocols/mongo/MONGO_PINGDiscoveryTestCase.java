package org.jgroups.protocols.mongo;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.jgroups.JChannel;
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

        mongoDBContainer = new MongoDBContainer("mongo:8.0");
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

    // The cluster names need to randomized so that multiple test runs can be run in parallel.
    public static final String RANDOM_CLUSTER_NAME = UUID.randomUUID().toString();

    @Test
    public void testDiscovery() throws Exception {
        discover(RANDOM_CLUSTER_NAME);
    }

    @Test
    public void testDiscoveryObscureClusterName() throws Exception {
        String obscureClusterName = "``\\//--+ěščřžýáíé==''!@#$%^&*()_{}<>?";
        discover(obscureClusterName + RANDOM_CLUSTER_NAME);
    }

    private void discover(String clusterName) throws Exception {
        List<JChannel> channels = create(clusterName);

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

    private List<JChannel> create(String clusterName) throws Exception {
        List<JChannel> result = new LinkedList<>();
        for (int i = 0; i < CHANNEL_COUNT; i++) {
            JChannel channel = new JChannel("org/jgroups/protocols/mongo/tcp-MONGO_PING.xml");

            channel.connect(clusterName);
            if (i == 0) {
                // Let's be clear about the coordinator
                Util.sleep(1000);
            }
            result.add(channel);
        }
        return result;
    }

    protected static void printViews(List<JChannel> channels) {
        for (JChannel ch : channels) {
            System.out.println("Channel " + ch.getName() + " has view " + ch.getView());
        }
    }
}
