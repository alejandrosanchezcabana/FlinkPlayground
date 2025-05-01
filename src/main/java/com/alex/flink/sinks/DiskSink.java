package com.alex.flink.sinks;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

public class DiskSink implements Sink<String> {

    private final String outputPath;

    public DiskSink(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public SinkWriter<String> createWriter(InitContext initContext) throws IOException {
        return new DiskSinkWriter(outputPath);
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new DiskSinkWriter(outputPath);
    }
}