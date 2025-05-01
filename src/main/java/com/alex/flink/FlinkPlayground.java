package com.alex.flink;

import com.alex.flink.sinks.DiskSink;
import com.alex.flink.sources.S3Source;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.util.Properties;

public class FlinkPlayground {

  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Enable checkpointing
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    System.out.println("Begin preparation for job");

    Properties prop = new Properties();
    String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    String appConfigPath = rootPath + "application.properties";
    //load a properties file from class path, inside static method
    prop.load(new FileInputStream(appConfigPath));
    String bucketName = prop.getProperty("s3.bucket.name");
    String filePath = prop.getProperty("s3.file.path");
    String s3AccessKey = prop.getProperty("aws.access.key");
    String s3SecretKey = prop.getProperty("aws.secret.key");


    String outputPath = prop.getProperty("local.output.dir");

    System.out.println("Adding S3Source");
    // Read from S3 using S3Source with WatermarkStrategy
    DataStream<String> s3DataStream = env.fromSource(
        new S3Source(bucketName, filePath, s3AccessKey, s3SecretKey),
        WatermarkStrategy.forMonotonousTimestamps(),
        "S3 Source"
    );

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
}
