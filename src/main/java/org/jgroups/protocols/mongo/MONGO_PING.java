package org.jgroups.protocols.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.LinkedList;
import java.util.List;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
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

    // Builder-like methods and method overrides

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

    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @Override
    public void init() throws Exception {
        var connectionString = new ConnectionString(connection_url);
        if (connectionString.getDatabase() == null) {
            throw new IllegalStateException("Database name must be specified in connection_url");
        }
        mongoClient = MongoClients.create(connectionString);
        collection = mongoClient.getDatabase(connectionString.getDatabase()).getCollection(collection_name);
        super.init();
    }

    protected MongoCollection<Document> getCollection() {
        return this.collection;
    }

    @Override
    public void stop() {
        // Postpone closing the client post JDBC_PING2.stop() which uses the client for remove()
        super.stop();

        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    // No-op overrides otherwise inherited and called from JDBC_PING2.init()

    @Override
    protected void loadDriver() {
        // No-op.
    }

    @Override
    protected void createInsertStoredProcedure() {
        // No-op.
    }

    @Override
    protected void removeAllNotInCurrentView() {
        View local_view = view;
        if (local_view == null) {
            return;
        }
        try {
            String cluster_name = getClusterName();
            List<PingData> list = readAll(cluster_name);
            for (PingData data : list) {
                Address addr = data.getAddress();
                if (!local_view.containsMember(addr)) {
                    addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
                    doDelete(cluster_name, addr);
                }
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
    }

    @Override
    protected void clearTable(String clustername) {
        getCollection().deleteMany(eq(CLUSTERNAME_KEY, clustername));
    }

    @Override
    protected void writeToDB(PingData data, String clustername) {
        Address address = data.getAddress();
        String addr = Util.addressToString(address);
        String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
        PhysicalAddress ip_addr = data.getPhysicalAddr();
        String ip = ip_addr.toString();

        var filter = and(eq("_id", addr), eq(CLUSTERNAME_KEY, clustername));
        var document = new Document("_id", addr)
                .append(NAME_KEY, name)
                .append(CLUSTERNAME_KEY, clustername)
                .append(IP_KEY, ip)
                .append(ISCOORD_KEY, data.isCoord());
        getCollection().replaceOne(filter, document, new ReplaceOptions().upsert(true));
    }

    @Override
    protected void createSchema() {
        try {
            var db = mongoClient.getDatabase(collection.getNamespace().getDatabaseName());
            db.createCollection(collection_name);
        } catch (MongoCommandException mex) {
            // Ignore "collection already exists" error (code 48)
            if (mex.getErrorCode() != 48) {
                throw mex;
            }
        }
    }

    /**
     * Reads all ping data from the collection for the given cluster.
     */
    private List<PingData> readAll(String cluster) throws Exception {
        try (var iterator = getCollection().find(eq(CLUSTERNAME_KEY, cluster)).iterator()) {
            // Lock only for the shared counter; MongoClient is thread-safe
            lock.lock();
            try {
                reads++;
            } finally {
                lock.unlock();
            }
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
        return readAll(cluster);
    }

    /**
     * Deletes a single entry from the collection.
     */
    private void doDelete(String clustername, Address addressToDelete) {
        String address = Util.addressToString(addressToDelete);
        getCollection().deleteOne(and(eq("_id", address), eq(CLUSTERNAME_KEY, clustername)));
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        doDelete(clustername, addressToDelete);
    }
}
