package com.alex.flink.sources.s3;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class S3SplitEnumerator implements SplitEnumerator<S3SourceSplit, Void> {
  public S3SplitEnumerator(SplitEnumeratorContext<S3SourceSplit> splitEnumeratorContext) {
  }

  @Override
  public void start() {

  }

  @Override
  public void handleSplitRequest(int i, @Nullable String s) {

  }

  @Override
  public void addSplitsBack(List<S3SourceSplit> list, int i) {

  }

  @Override
  public void addReader(int i) {

  }

  @Override
  public Void snapshotState(long l) throws Exception {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
