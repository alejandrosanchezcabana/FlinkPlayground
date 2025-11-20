package com.alex.flink.sources.jdbc;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class JDBCSplitEnumerator implements SplitEnumerator<JDBCSourceSplit, Void> {
    private final SplitEnumeratorContext<JDBCSourceSplit> context;
    private final Queue<JDBCSourceSplit> remainingSplits;

    public JDBCSplitEnumerator(SplitEnumeratorContext<JDBCSourceSplit> context) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>();
        this.remainingSplits.add(new JDBCSourceSplit("jdbc-split-0"));
    }

    @Override
    public void start() {
        // No-op
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!remainingSplits.isEmpty()) {
            JDBCSourceSplit split = remainingSplits.poll();
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<JDBCSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // If we have splits, assign them to the new reader
        if (!remainingSplits.isEmpty()) {
            JDBCSourceSplit split = remainingSplits.poll();
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
        // No-op
    }
}
