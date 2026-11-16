package org.jgroups.protocols.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
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
 * <p>
 * By default the protocol creates its own {@link MongoClient} from the connection URL. An application that already has
 * a client (e.g., one managed by Spring) can share it by passing one of its databases to
 * {@link #setMongoDatabase(MongoDatabase)} before the channel is connected; the connection URL is then optional and not
 * used, and the shared client is never closed by this protocol.
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

    @Property(description = "Name of the MongoDB collection used to store cluster member discovery information. Defaults to 'jgroups-ping' collection.", writable = false)
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

    // Database of a client shared by the application, set via setMongoDatabase()
    private MongoDatabase sharedDatabase;
    // Client created by this protocol, null when using a shared database; closed by this protocol
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    /**
     * Uses a database of a client shared by the application instead of creating a client from {@code connection_url},
     * which is then optional and not used. Must be called after the channel is created and before it is first
     * connected; a disconnected channel keeps using the same database when reconnected. The shared client is never
     * closed by this protocol; its owner closes it after the channel.
     * <p>
     * The read preference is always primary and the default codec registry is used, regardless of the database's
     * settings; other settings (e.g., write concern) are inherited from the database.
     *
     * @throws IllegalStateException if the channel has already been connected (even if since disconnected)
     */
    public MONGO_PING setMongoDatabase(MongoDatabase database) {
        Objects.requireNonNull(database, "database");
        if (collection != null) {
            throw new IllegalStateException("MongoDB database must be set before the channel is connected");
        }
        this.sharedDatabase = database;
        return this;
    }

    /**
     * Only validates {@code connection_url} if it is set, to fail early. No client is created here: JChannel calls
     * init() from its constructor, before an application could call {@link #setMongoDatabase(MongoDatabase)}.
     */
    @Override
    public void init() throws Exception {
        if (connection_url != null) {
            parseConnectionUrl();
        }
        super.init();
    }

    private ConnectionString parseConnectionUrl() {
        var connectionString = new ConnectionString(connection_url);
        if (connectionString.getDatabase() == null) {
            throw new IllegalStateException("Database name must be specified in connection_url");
        }
        return connectionString;
    }

    /**
     * Resolves the collection on the first start, from the shared database or from a client created by this protocol.
     * Disconnecting and reconnecting a channel calls stop() and start() again, and keeps using the same collection.
     */
    @Override
    public void start() throws Exception {
        if (collection == null) {
            MongoDatabase database = sharedDatabase;
            if (database == null) {
                if (connection_url == null) {
                    throw new IllegalStateException("Either connection_url or a shared MongoDB database must be set");
                }
                // Parsed here rather than in init(), so that a connection_url set after the channel was created is used
                var connectionString = parseConnectionUrl();
                mongoClient = MongoClients.create(connectionString);
                database = mongoClient.getDatabase(connectionString.getDatabase());
            }
            try {
                createCollection(database);
                collection = database.getCollection(collection_name)
                        // Don't inherit settings of a shared client that would break discovery: reads from secondaries
                        // can be stale, and a custom codec registry might not be able to encode Documents
                        .withReadPreference(ReadPreference.primary())
                        .withCodecRegistry(MongoClientSettings.getDefaultCodecRegistry());
            } catch (Throwable t) {
                // Close the own client right away instead of relying on the stack calling destroy()
                closeClient();
                throw t;
            }
        }
        super.start();
    }

    protected MongoCollection<Document> getCollection() {
        return this.collection;
    }

    /**
     * Returns the collection, which only exists between the first connect and the close of the channel.
     */
    private MongoCollection<Document> requireCollection() {
        var collection = getCollection();
        if (collection == null) {
            throw new IllegalStateException("MongoDB collection is not available: the channel has not been connected yet or is closed");
        }
        return collection;
    }

    /**
     * Closes the client in destroy() rather than stop(): disconnecting and reconnecting a channel calls stop() and
     * start() without init() and destroy(), and FILE_PING.stop() still uses the client to remove the local entry.
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            closeClient();
        }
    }

    /**
     * Closes the client only if this protocol created it; a shared client stays open. Either way the collection is
     * dropped, so that a closed client is not handed out and a shared one is not kept reachable.
     */
    private void closeClient() {
        collection = null;
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
    protected void clearTable(String clustername) {
        requireCollection().deleteMany(eq(CLUSTERNAME_KEY, clustername));
    }

    /**
     * Returns null as MongoDB does not use JDBC connections. The parent class methods that receive
     * a Connection parameter (e.g., {@link #delete(Connection, String, Address)}) are overridden
     * to ignore it and use the cached {@link MongoCollection} instead.
     */
    @Override
    protected Connection getConnection() {
        return null;
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
        requireCollection().replaceOne(filter, document, new ReplaceOptions().upsert(true));
    }

    /**
     * Called by JDBC_PING2.init(), before a shared database can be set; the collection is created in {@link #start()}.
     */
    @Override
    protected void createSchema() {
        // No-op.
    }

    private void createCollection(MongoDatabase database) {
        try {
            database.createCollection(collection_name);
        } catch (MongoCommandException mex) {
            // Ignore "collection already exists" error (code 48)
            if (mex.getErrorCode() != 48) {
                throw mex;
            }
        }
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (var iterator = requireCollection().find(eq(CLUSTERNAME_KEY, cluster)).iterator()) {
            // Lock only for the shared counter to work around its non-volatility
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
                Address address = Util.addressFromString(uuid);
                String name = doc.get(NAME_KEY, String.class);
                String ip = doc.get(IP_KEY, String.class);
                IpAddress ipAddress = new IpAddress(ip);
                boolean isCoord = Boolean.TRUE.equals(doc.get(ISCOORD_KEY, Boolean.class));
                PingData pingData = new PingData(address, true, name, ipAddress).coord(isCoord);
                retval.add(pingData);
            }

            return retval;
        }
    }

    @Override
    protected void delete(Connection conn, String clustername, Address addressToDelete) {
        // Ignore conn - it's null anyway
        this.delete(clustername, addressToDelete);
    }

    /**
     * The parent only handles SQLException, while the MongoDB driver throws unchecked exceptions. Log them instead, so
     * that a failed removal (e.g., MongoDB is not reachable, or a shared client was already closed by its owner) aborts
     * neither the stack shutdown in FILE_PING.stop() nor the handling of a view change.
     */
    @Override
    protected void remove(String clustername, Address addr) {
        try {
            delete(clustername, addr);
        } catch (RuntimeException e) {
            log.error(String.format("%s: failed deleting %s from the collection", local_addr, addr), e);
        }
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        var collection = getCollection();
        if (collection == null) {
            // start() failed, so the local entry was never written; FILE_PING.stop() still tries to remove it
            return;
        }
        String address = Util.addressToString(addressToDelete);
        collection.deleteOne(and(eq("_id", address), eq(CLUSTERNAME_KEY, clustername)));
    }
}
