package com.alex.flink.sources.jdbc;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JDBCSimpleVersionedSerializer implements SimpleVersionedSerializer<JDBCSourceSplit> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(JDBCSourceSplit split) throws IOException {
        return split.splitId().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public JDBCSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return new JDBCSourceSplit(new String(serialized, StandardCharsets.UTF_8));
        }
        throw new IOException("Unknown version: " + version);
    }
}
