package com.yahoo.ycsb.db.voldemort;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.voldemort.MsgProto.Entry;
import com.yahoo.ycsb.db.voldemort.MsgProto.Msg;

public class ProtoBuffIterator extends
        StringByteIterator {

    public ProtoBuffIterator(String s) {
        super(s);
    }



    /**
     * Create a copy of a map, converting the values from
     * StringByteIterators to protobuff.
     */
    public static Msg getMsg(Map<String, ByteIterator> m) {
        Msg.Builder proto = Msg.newBuilder();
        for (String s : m.keySet()) {
            proto.addEntry(MsgProto.Entry.newBuilder().setKey(ByteString.copyFromUtf8(s)).setValue(ByteString.copyFrom(m.get(s).toArray())));
        }
        return proto.build();
    }

    /**
     * Put all of the entries of one map into the other, converting
     * String values into ByteIterators.
     */
    public static void putAllAsByteIterators(Map<String, ByteIterator> out, Msg msg) {
        for (Entry s : msg.getEntryList()) {
            out.put(s.getKey().toStringUtf8(), new StringByteIterator(s.getValue().toStringUtf8()));
        }
    }

    public static void putAllAsStrings(Msg.Builder msg, HashMap<String, ByteIterator> values) {
        for (String s : values.keySet()) {
            msg.addEntry(MsgProto.Entry.newBuilder()
                                       .setKey(ByteString.copyFromUtf8(s))
                                       .setValue(ByteString.copyFrom(values.get(s).toArray())));
        }
    }
}
