package com.alex.flink.sources.disk;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

public class DiskSimpleVersionedSerializer implements SimpleVersionedSerializer<DiskSourceSplit> {
  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(DiskSourceSplit diskSourceSplit) throws IOException {
    return diskSourceSplit.splitId().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public DiskSourceSplit deserialize(int i, byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return new DiskSourceSplit(ois.readUTF());
  }
}
