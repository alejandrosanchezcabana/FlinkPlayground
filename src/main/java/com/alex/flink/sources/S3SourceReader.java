package com.alex.flink.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class S3SourceReader implements SourceReader<String, S3SourceSplit> {
  private final String bucketName;
  private final String prefix;
  private final S3Client s3Client;
  private Iterator<S3Object> objectIterator;
  private final ObjectMapper objectMapper = new ObjectMapper(); // JSON parser

  public S3SourceReader(String bucketName, Region bucketRegion, String prefix, String AWSAccessKey, String AWSSecretKey) {
    System.out.println("Initializing S3SourceReader");
    this.bucketName = bucketName;
    this.prefix = prefix;

    AwsCredentials credentials;
    if (AWSAccessKey != null && AWSSecretKey != null) {
      credentials = AwsBasicCredentials.create(AWSAccessKey, AWSSecretKey);
    } else {
      credentials = AnonymousCredentialsProvider.create().resolveCredentials();
    }

    this.s3Client = S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials)).region(bucketRegion).endpointOverride(URI.create(String.format("https://s3.%S.amazonaws.com", bucketRegion.id())))
        .build();
    initializeObjectIterator();
  }

  private void initializeObjectIterator() {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .prefix(prefix)
        .build();
    ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
    List<S3Object> objects = listResponse.contents();
    this.objectIterator = objects.iterator();
  }

  @Override
  public void start() {
    System.out.println("S3SourceReader started");
  }

  @Override
  public InputStatus pollNext(ReaderOutput readerOutput) throws Exception {
    System.out.println("Polling next object from S3");
    if (objectIterator.hasNext()) {
      S3Object s3Object = objectIterator.next();
      GetObjectRequest getRequest = GetObjectRequest.builder()
          .bucket(bucketName)
          .key(s3Object.key())
          .build();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          s3Client.getObject(getRequest), StandardCharsets.UTF_8))) {
        System.out.println("Reading object: " + s3Object.key());
        StringBuilder jsonContent = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          jsonContent.append(line);
        }
        // Parse JSON and emit each object as a string
        JsonNode rootNode = objectMapper.readTree(jsonContent.toString());
        if (rootNode.isArray()) {
          for (JsonNode node : rootNode) {
            readerOutput.collect(node.toString());
          }
        } else {
          readerOutput.collect(rootNode.toString());
        }
      }
      return InputStatus.MORE_AVAILABLE;
    } else {
      return InputStatus.END_OF_INPUT;
    }
  }

  @Override
  public List snapshotState(long l) {
    return List.of();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return null;
  }

  @Override
  public void addSplits(List list) {

  }

  @Override
  public void notifyNoMoreSplits() {

  }

  @Override
  public void close() throws Exception {

  }
}
