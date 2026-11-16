module org.jgroups.mongo {
    requires java.sql; // java.sql.Connection used in JDBC_PING2 method signatures which this protocol currently extends
    requires org.jgroups;
    requires org.mongodb.bson;
    requires org.mongodb.driver.core;
    requires transitive org.mongodb.driver.sync.client; // MongoDatabase is used in the public API

    exports org.jgroups.protocols.mongo;

    opens org.jgroups.protocols.mongo to org.jgroups; // reflection for @Property field injection
}
