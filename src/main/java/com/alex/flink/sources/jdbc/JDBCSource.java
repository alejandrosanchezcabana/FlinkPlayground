package com.alex.flink.sources.jdbc;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;

public class JDBCSource implements Source<List<Object>, JDBCSourceSplit, Void> {
    private final String url;
    private final String username;
    private final String password;
    private final String query;

    public JDBCSource(String url, String username, String password, String query) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.query = query;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<List<Object>, JDBCSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new JDBCSourceReader(url, username, password, query);
    }

    @Override
    public SplitEnumerator<JDBCSourceSplit, Void> createEnumerator(SplitEnumeratorContext<JDBCSourceSplit> enumContext)
            throws Exception {
        return new JDBCSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<JDBCSourceSplit, Void> restoreEnumerator(SplitEnumeratorContext<JDBCSourceSplit> enumContext,
            Void checkpoint) throws Exception {
        return new JDBCSplitEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<JDBCSourceSplit> getSplitSerializer() {
        return new JDBCSimpleVersionedSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
