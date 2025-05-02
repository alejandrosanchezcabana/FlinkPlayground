package com.alex.flink.sources.disk;

import org.apache.flink.api.connector.source.SourceSplit;

public record DiskSourceSplit(String splitId) implements SourceSplit {
}