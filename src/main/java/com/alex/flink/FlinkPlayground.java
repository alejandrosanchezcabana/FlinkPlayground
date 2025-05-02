package com.alex.flink;

import com.alex.flink.sinks.disk.DiskSink;
import com.alex.flink.sources.s3.S3Source;
import com.alex.flink.utils.MarkdownProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

public class FlinkPlayground {
  private static final MarkdownProperties properties = new MarkdownProperties();
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Enable checkpointing
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    System.out.println("Begin preparation for job");

    loadProperties();

    String bucketName = properties.getProperty("s3.bucket.name");
    String bucketRegion = properties.getProperty("s3.bucket.region", "eu-west-1");
    String filePath = properties.getProperty("s3.file.path");
    String s3AccessKey = properties.getProperty("aws.access.key");
    String s3SecretKey = properties.getProperty("aws.secret.key");


    String outputPath = properties.getProperty("local.output.dir");

    System.out.println("Adding S3Source");

    DataStream<String> s3DataStream = env.fromSource(new S3Source(bucketName, bucketRegion, filePath, s3AccessKey, s3SecretKey), WatermarkStrategy.forMonotonousTimestamps(), "S3 Source");

    // Process the data (if needed)
    SingleOutputStreamOperator<String> processedStream = s3DataStream.map((MapFunction<String, String>) value -> {
      // Add any processing logic here
      return value;
    });

    System.out.println("Adding DiskSink");
    // Create FileSink
    processedStream.sinkTo(new DiskSink(outputPath));

    // Execute the job
    env.execute("Flink S3 to Disk Job");
  }

  private static void loadProperties() {
    URL resource = Thread.currentThread().getContextClassLoader().getResource("");
    String appConfigPath;
    if (resource != null) {
      appConfigPath = resource.getPath();
    } else {
      throw new RuntimeException("Resource not found");
    }

    try {
      properties.load(new FileInputStream(appConfigPath + "application.md"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
