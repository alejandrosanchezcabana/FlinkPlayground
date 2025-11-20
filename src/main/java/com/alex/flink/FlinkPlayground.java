package com.alex.flink;

import com.alex.flink.mapper.FieldRemoverMapper;
import com.alex.flink.sinks.disk.DiskSink;
import com.alex.flink.sources.disk.DiskSource;
import com.alex.flink.sources.s3.S3Source;
import com.alex.flink.utils.MarkdownProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public class FlinkPlayground {
  private static final MarkdownProperties properties = new MarkdownProperties();
  private static DataStreamSource<List<Object>> dataStream;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    loadProperties();

    addSource(env, DiskSource.class);

    String fieldToRemove = properties.getProperty("field.remover.fieldToRemove");
    SingleOutputStreamOperator<List<Object>> stream = dataStream.map(new FieldRemoverMapper(fieldToRemove));

    addSink(stream, DiskSink.class);

    env.execute("Flink Job");
  }

  private static void addSource(StreamExecutionEnvironment env,
      Class<? extends Source<List<Object>, ? extends SourceSplit, Void>> source) {
    if (source.equals(S3Source.class)) {
      String bucketName = properties.getProperty("s3.bucket.name");
      String bucketRegion = properties.getProperty("s3.bucket.region", "eu-west-1");
      String filePath = properties.getProperty("s3.file.path");
      String s3AccessKey = properties.getProperty("aws.access.key");
      String s3SecretKey = properties.getProperty("aws.secret.key");
      dataStream = env.fromSource(new S3Source(bucketName, bucketRegion, filePath, s3AccessKey, s3SecretKey),
          WatermarkStrategy.forMonotonousTimestamps(), "S3 Source");
    } else if (source.equals(DiskSource.class)) {
      String inputPath = properties.getProperty("local.input.dir");
      String inputPattern = properties.getProperty("local.input.pattern");
      dataStream = env.fromSource(new DiskSource(inputPath, inputPattern), WatermarkStrategy.forMonotonousTimestamps(),
          "Disk Source");
    }
  }

  private static void addSink(SingleOutputStreamOperator<List<Object>> stream,
      Class<? extends Sink<List<Object>>> sink) {
    if (sink.equals(DiskSink.class)) {
      String outputPath = properties.getProperty("local.output.dir");
      stream.sinkTo(new DiskSink(outputPath));
    }
  }

  private static void loadProperties() {
    URL resource = Thread.currentThread().getContextClassLoader().getResource("");
    String appConfigPath;
    if (resource != null) {
      appConfigPath = resource.getPath();
    } else {
      throw new RuntimeException();
    }

    try {
      properties.load(new FileInputStream(appConfigPath + "applicationd.md"));
    } catch (IOException e) {
      System.out.println("Failed to locate application.md");
      System.out.println(
          "Please make sure /src/main/resources/application.md exists, if it doesn't you can create by copying the default_application.md file");
      throw new RuntimeException();
    }
  }
}
