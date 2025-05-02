package com.alex.flink.sources.disk;

import com.alex.flink.sources.s3.S3SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class DiskSplitEnumerator implements SplitEnumerator<DiskSourceSplit, Void> {
  public DiskSplitEnumerator(SplitEnumeratorContext<DiskSourceSplit> splitEnumeratorContext) {
  }

  @Override
  public void start() {

  }

  @Override
  public void handleSplitRequest(int i, @Nullable String s) {

  }

  @Override
  public void addSplitsBack(List<DiskSourceSplit> list, int i) {

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
