package com.alex.flink.sources.jdbc;

import org.apache.flink.api.connector.source.SourceSplit;

public class JDBCSourceSplit implements SourceSplit {
    private final String splitId;

    public JDBCSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
