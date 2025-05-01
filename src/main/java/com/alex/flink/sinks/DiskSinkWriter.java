package com.alex.flink.sinks;

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class DiskSinkWriter implements SinkWriter<String> {
  private final String outputPath;
  DiskSinkWriter(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void write(String value, Context context) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, true))) {
      writer.write(value);
      writer.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush(boolean b) {

  }

  @Override
  public void close() throws Exception {
  }
}
