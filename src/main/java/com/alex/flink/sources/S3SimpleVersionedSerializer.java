package com.alex.flink.sources;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

public class S3SimpleVersionedSerializer implements SimpleVersionedSerializer<S3SourceSplit> {
  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(S3SourceSplit s3SourceSplit) throws IOException {
    return s3SourceSplit.splitId().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public S3SourceSplit deserialize(int i, byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return new S3SourceSplit(ois.readUTF());
  }
}
