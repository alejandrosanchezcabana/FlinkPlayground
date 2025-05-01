package com.alex.flink.sources;

import org.apache.flink.api.connector.source.SourceSplit;

public class S3SourceSplit implements SourceSplit {
  private final String splitId;

  public S3SourceSplit(String splitId) {
    this.splitId = splitId;
  }

  @Override
  public String splitId() {
    return splitId;
  }
}