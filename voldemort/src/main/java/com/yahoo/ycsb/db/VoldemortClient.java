package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.undoTracker.SRD;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.voldemort.MsgProto;
import com.yahoo.ycsb.db.voldemort.MsgProto.Msg;
import com.yahoo.ycsb.db.voldemort.ProtoBuffIterator;


public class VoldemortClient extends
        DB {

    private StoreClient<String, Msg> storeClient;
    private SocketStoreClientFactory socketFactory;
    private String storeName;
    private final Logger logger = Logger.getLogger(VoldemortClient.class);

    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;

    /**
     * Initialize the DB layer. This accepts all properties allowed by the Voldemort
     * client.
     * A store maps to a table.
     * Required : bootstrap_urls
     * Additional property : store_name -> to preload once, should be same as -t
     * <table>
     * {@linktourl
     * http://project-voldemort.com/javadoc/client/voldemort/client/ClientConfig.html}
     */
    @Override
    public void init() throws DBException {
        Properties prop = getProperties();
        // set default as protocol buffers
        prop.setProperty("request_format", "pb0");
        ClientConfig clientConfig = new ClientConfig(prop);
        socketFactory = new SocketStoreClientFactory(clientConfig);

        // Retrieve store name
        storeName = getProperties().getProperty("store_name", "usertable");

        // Use store name to retrieve client
        storeClient = socketFactory.getStoreClient(storeName);
        if (storeClient == null)
            throw new DBException("Unable to instantiate store client");

    }

    @Override
    public void cleanup() throws DBException {
        socketFactory.close();
    }

    @Override
    public int delete(String table, String key) {
        if (checkStore(table) == ERROR) {
            return ERROR;
        }
        try {
            if (storeClient.delete(key, new SRD()))
                return OK;
            else
                return ERROR;
        } catch (Exception e) {
            System.err.println("delete: " + key);
            e.printStackTrace();
            return ERROR;
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (checkStore(table) == ERROR) {
            return ERROR;
        }
        try {
            storeClient.put(key, ProtoBuffIterator.getMsg(values), new SRD());
            return OK;
        } catch (Exception e) {
            System.err.println("insert: " + key + " values: " + values);
            e.printStackTrace();
            return ERROR;
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        if (checkStore(table) == ERROR) {
            return ERROR;
        }
        try {
            Versioned<Msg> versionedValue = storeClient.get(key, new SRD());

            if (versionedValue == null)
                return NOT_FOUND;

            if (fields != null) {
                for (MsgProto.Entry e : versionedValue.getValue().getEntryList()) {
                    if (fields.contains(e.getKey())) {
                        result.put(e.getKey().toStringUtf8(), new StringByteIterator(e.getValue().toStringUtf8()));
                    }
                }
            } else {
                ProtoBuffIterator.putAllAsByteIterators(result, versionedValue.getValue());
            }
            return OK;
        } catch (Exception e) {
            System.err.println("read: " + key + " fields: " + fields);
            e.printStackTrace();
            return ERROR;
        }

    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.warn("Voldemort does not support Scan semantics");
        return OK;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        if (checkStore(table) == ERROR) {
            return ERROR;
        }
        try {
            Versioned<Msg> versionedValue = storeClient.get(key, new SRD());
            MsgProto.Msg.Builder value = MsgProto.Msg.newBuilder();

            VectorClock version;
            if (versionedValue != null) {
                version = ((VectorClock) versionedValue.getVersion()).incremented(0, 1);
                Msg msg = versionedValue.getValue();

                // update the values
                for (MsgProto.Entry e : msg.getEntryList()) {
                    ByteIterator val = null;
                    if ((val = values.remove(e.getKey())) != null) {
                        value.addEntry(MsgProto.Entry.newBuilder().setKey(e.getKey()).setValue(ByteString.copyFrom(val.toArray())));
                    }
                }
                // add new
                for (Entry<String, ByteIterator> entry : values.entrySet()) {
                    value.addEntry(MsgProto.Entry.newBuilder()
                                                 .setKey(ByteString.copyFromUtf8(entry.getKey()))
                                                 .setValue(ByteString.copyFrom(entry.getValue().toArray())));
                }

            } else {
                version = new VectorClock();
                ProtoBuffIterator.putAllAsStrings(value, values);
            }

            // colocar a mensagem
            storeClient.put(key, Versioned.value(value.build(), version), new SRD());
            return OK;
        } catch (Exception e) {
            System.err.println("update: " + key + " fields: " + values);
            e.printStackTrace();
            return ERROR;
        }
    }

    private int checkStore(String table) {
        if (table.compareTo(storeName) != 0) {
            try {
                storeClient = socketFactory.getStoreClient(table);
                if (storeClient == null) {
                    logger.error("Could not instantiate storeclient for " + table);
                    return ERROR;
                }
                storeName = table;
            } catch (Exception e) {
                System.err.println("check strore: " + table);
                e.printStackTrace();
                return ERROR;
            }
        }
        return OK;
    }

}
