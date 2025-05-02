package com.alex.flink.sources.s3;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import software.amazon.awssdk.regions.Region;

import java.util.List;

public class S3Source implements Source<List<Object>, S3SourceSplit, Void> {
  private final String bucketName;
  private final Region bucketRegion;
  private final String prefix;
  private final String AWSAccessKey;
  private final String AWSSecretKey;

  public S3Source(String bucketName, String bucketRegion, String prefix, String AWSAccessKey, String AWSSecretKey) {
    this.bucketName = bucketName;
    this.prefix = prefix;
    this.AWSAccessKey = AWSAccessKey;
    this.AWSSecretKey = AWSSecretKey;
    this.bucketRegion = Region.of(bucketRegion);
    System.out.println("Initializing S3Source with bucket: " + bucketName + " and prefix: " + prefix);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<S3SourceSplit, Void> createEnumerator(SplitEnumeratorContext<S3SourceSplit> splitEnumeratorContext) throws Exception {
    return new S3SplitEnumerator(splitEnumeratorContext);
  }

  @Override
  public SplitEnumerator<S3SourceSplit, Void> restoreEnumerator(SplitEnumeratorContext<S3SourceSplit> splitEnumeratorContext, Void unused) throws Exception {
    return new S3SplitEnumerator(splitEnumeratorContext);
  }

  @Override
  public SimpleVersionedSerializer<S3SourceSplit> getSplitSerializer() {
    return new S3SimpleVersionedSerializer();
  }

  @Override
  public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<List<Object>, S3SourceSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
    System.out.println("Creating S3SourceReader with bucket: " + bucketName + " and prefix: " + prefix);
    return new S3SourceReader(bucketName, bucketRegion, prefix, AWSAccessKey, AWSSecretKey);
  }
}

