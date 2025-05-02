package com.alex.flink.sources.s3;

import org.apache.flink.api.connector.source.SourceSplit;

public record S3SourceSplit(String splitId) implements SourceSplit {
}