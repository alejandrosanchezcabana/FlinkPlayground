package com.alex.flink.sources.disk;

import com.alex.flink.sources.s3.S3SimpleVersionedSerializer;
import com.alex.flink.sources.s3.S3SourceReader;
import com.alex.flink.sources.s3.S3SourceSplit;
import com.alex.flink.sources.s3.S3SplitEnumerator;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.regex.Pattern;

public class DiskSource implements Source<List<Object>, DiskSourceSplit, Void> {
  private final String inputPath;
  private final String pattern;

  public DiskSource(String inputPath, String pattern) {
    this.inputPath = inputPath;
    this.pattern = pattern;
    System.out.println("Initializing DiskSource with inputPath: " + inputPath + " and pattern: " + pattern);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<DiskSourceSplit, Void> createEnumerator(SplitEnumeratorContext<DiskSourceSplit> splitEnumeratorContext) throws Exception {
    return new DiskSplitEnumerator(splitEnumeratorContext);
  }

  @Override
  public SplitEnumerator<DiskSourceSplit, Void> restoreEnumerator(SplitEnumeratorContext<DiskSourceSplit> splitEnumeratorContext, Void unused) throws Exception {
    return new DiskSplitEnumerator(splitEnumeratorContext);
  }

  @Override
  public SimpleVersionedSerializer<DiskSourceSplit> getSplitSerializer() {
    return new DiskSimpleVersionedSerializer();
  }

  @Override
  public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<List<Object>, DiskSourceSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
    return new DiskSourceReader(inputPath, pattern);
  }
}

