package org.jgroups.protocols.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.LinkedList;
import java.util.List;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.JDBC_PING2;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

/**
 * Discovery protocol using MongoDB as a shared store for cluster member information.
 * This protocol stores node discovery data (address, name, IP, coordinator status) in a MongoDB collection.
 * <p>
 * Configuration example:
 * <pre>{@code
 * <mongo.MONGO_PING connection_url="mongodb://localhost:27017/jgroups"
 *                   collection_name="jgroups-ping"
 *                   remove_all_data_on_view_change="true"/>
 * }</pre>
 * <p>
 * The connection URL must include the database name (e.g., {@code mongodb://host:port/database}).
 *
 * @author rsobies
 * @author Radoslav Husar
 */
public class MONGO_PING extends JDBC_PING2 {

    // Constants
    protected static final short MONGO_PING_DEFAULT_PROTOCOL_ID = 531;
    private static final String CLUSTERNAME_KEY = "clustername";
    private static final String NAME_KEY = "name";
    private static final String IP_KEY = "ip";
    private static final String ISCOORD_KEY = "isCoord";

    static {
        short protocolId = ClassConfigurator.getProtocolId(MONGO_PING.class);
        // Since JGroups 5.5.3 we can use ClassConfigurator.getProtocolId which manages the ID; until then we need to provide an ID ourselves
        ClassConfigurator.addProtocol(protocolId != 0 ? protocolId : MONGO_PING_DEFAULT_PROTOCOL_ID, MONGO_PING.class);
    }

    @Property(description = "Name of the MongoDB collection used to store cluster member information")
    protected String collection_name = "jgroups-ping";

    protected MongoClient mongoClient;

    @Override
    public void init() throws Exception {
        super.init();

        // Create shared client for runtime operations
        mongoClient = MongoClients.create(connectionString);
    }

    @Override
    public void stop() {
        // Superclass stop() uses the client for remove()
        super.stop();

        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    @Override
    public MONGO_PING setConnectionUrl(String connectionUrl) {
        this.connection_url = connectionUrl;
        return this;
    }

    public String getCollectionName() {
        return collection_name;
    }

    public MONGO_PING setCollectionName(String collectionName) {
        this.collection_name = collectionName;
        return this;
    }

    protected MongoClient getMongoClient() {
        return mongoClient;
    }

    protected MongoCollection<Document> getCollection(MongoClient client) {
        var connString = new ConnectionString(connection_url);
        assert connString.getDatabase() != null;
        var db = client.getDatabase(connString.getDatabase());
        return db.getCollection(collection_name);
    }

    protected ConnectionString connectionString;

    @Override
    protected void removeAllNotInCurrentView() {
        View local_view = view;
        if (local_view == null) {
            return;
        }
        String cluster_name = getClusterName();
        var collection = getCollection(getMongoClient());
        try {
            List<PingData> list = readFromDB(getMongoClient(), getClusterName());
            for (PingData data : list) {
                Address addr = data.getAddress();
                if (!local_view.containsMember(addr)) {
                    addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
                    delete(collection, cluster_name, addr);
                }
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
    }

    @Override
    protected void loadDriver() {
        //do nothing
    }

    @Override
    protected void clearTable(String clustername) {
        var collection = getCollection(getMongoClient());
        collection.deleteMany(eq(CLUSTERNAME_KEY, clustername));
    }

    @Override
    protected void writeToDB(PingData data, String clustername) {
        lock.lock();
        try {
            var collection = getCollection(getMongoClient());
            Address address = data.getAddress();
            String addr = Util.addressToString(address);

            // Delete existing entry
            collection.deleteOne(and(eq("_id", addr), eq(CLUSTERNAME_KEY, clustername)));

            // Insert new entry
            String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
            PhysicalAddress ip_addr = data.getPhysicalAddr();
            String ip = ip_addr.toString();
            collection.insertOne(new Document("_id", addr)
                    .append(NAME_KEY, name)
                    .append(CLUSTERNAME_KEY, clustername)
                    .append(IP_KEY, ip)
                    .append(ISCOORD_KEY, data.isCoord())
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void createSchema() {
        connectionString = new ConnectionString(connection_url);
        // Use a temporary client here since this is called during init() before the shared client is created
        try (var client = MongoClients.create(connectionString)) {
            var db = client.getDatabase(connectionString.getDatabase());
            db.createCollection(collection_name);
        }
    }

    @Override
    protected void createInsertStoredProcedure() {
        //do nothing
    }

    protected List<PingData> readFromDB(MongoClient mongoClient, String cluster) throws Exception {
        var collection = getCollection(mongoClient);
        try (var iterator = collection.find(eq(CLUSTERNAME_KEY, cluster)).iterator()) {
            reads++;
            List<PingData> retval = new LinkedList<>();

            while (iterator.hasNext()) {
                var doc = iterator.next();
                String uuid = doc.get("_id", String.class);
                Address addr = Util.addressFromString(uuid);
                String name = doc.get(NAME_KEY, String.class);
                String ip = doc.get(IP_KEY, String.class);
                IpAddress ip_addr = new IpAddress(ip);
                boolean coord = Boolean.TRUE.equals(doc.get(ISCOORD_KEY, Boolean.class));
                PingData data = new PingData(addr, true, name, ip_addr).coord(coord);
                retval.add(data);
            }

            return retval;
        }
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        return readFromDB(getMongoClient(), cluster);
    }

    protected void delete(MongoCollection<Document> collection, String clustername, Address addressToDelete) {
        lock.lock();
        try {
            String addr = Util.addressToString(addressToDelete);
            collection.deleteOne(and(eq("_id", addr), eq(CLUSTERNAME_KEY, clustername)));
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        var collection = getCollection(getMongoClient());
        delete(collection, clustername, addressToDelete);
    }
}
