package com.alex.flink.sources.jdbc;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class JDBCSourceReader implements SourceReader<List<Object>, JDBCSourceSplit> {
    private final String url;
    private final String username;
    private final String password;
    private final String query;

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private boolean hasSplit = false;
    private boolean finished = false;

    public JDBCSourceReader(String url, String username, String password, String query) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.query = query;
    }

    @Override
    public void start() {
        // No-op, connection is opened when processing split
    }

    @Override
    public InputStatus pollNext(ReaderOutput<List<Object>> output) throws Exception {
        if (finished) {
            return InputStatus.END_OF_INPUT;
        }

        if (!hasSplit) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (connection == null) {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
        }

        if (resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));
            }
            output.collect(row);
            return InputStatus.MORE_AVAILABLE;
        } else {
            finished = true;
            return InputStatus.END_OF_INPUT;
        }
    }

    @Override
    public List<JDBCSourceSplit> snapshotState(long checkpointId) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<JDBCSourceSplit> splits) {
        if (!splits.isEmpty()) {
            hasSplit = true;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // No-op
    }

    @Override
    public void close() throws Exception {
        if (resultSet != null)
            resultSet.close();
        if (statement != null)
            statement.close();
        if (connection != null)
            connection.close();
    }
}
