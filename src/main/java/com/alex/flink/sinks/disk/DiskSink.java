package com.alex.flink.sinks.disk;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.List;

public class DiskSink implements Sink<List<Object>> {

  private final String outputPath;

  public DiskSink(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public SinkWriter<List<Object>> createWriter(WriterInitContext context) throws IOException {
    return new DiskSinkWriter(outputPath);
  }
}