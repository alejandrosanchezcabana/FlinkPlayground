package com.alex.flink.sinks.disk;

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class DiskSinkWriter implements SinkWriter<List<Object>> {
  private final String outputPath;
  DiskSinkWriter(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void write(List<Object> value, Context context) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("%s/%s.json", outputPath, System.currentTimeMillis()), true))) {
      for (Object content : value) {
        writer.write(content.toString());
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing to disk", e);
    }
  }

  @Override
  public void flush(boolean b) {

  }

  @Override
  public void close() throws Exception {
  }
}
