package com.alex.flink.sources.disk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskSourceReader implements SourceReader<List<Object>, DiskSourceSplit> {
  private final Path directoryPath;
  private final String filePattern;
  private final ObjectMapper objectMapper = new ObjectMapper(); // JSON parser
  private Iterator<File> fileIterator;

  public DiskSourceReader(String directoryPath, String filePattern) {
    System.out.println("Initializing DiskSourceReader");
    this.directoryPath = Paths.get(directoryPath);
    this.filePattern = filePattern;
    initializeFileIterator();
  }

  private void initializeFileIterator() {
    try (Stream<Path> paths = Files.walk(directoryPath)) {
      List<File> files = paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().matches(filePattern))
          .map(Path::toFile)
          .collect(Collectors.toList());
      this.fileIterator = files.iterator();
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize file iterator", e);
    }
  }

  @Override
  public void start() {
    System.out.println("DiskSourceReader started");
  }

  @Override
  public InputStatus pollNext(ReaderOutput<List<Object>> readerOutput) throws Exception {
    System.out.println("Polling next file from disk");
    if (fileIterator.hasNext()) {
      File file = fileIterator.next();
      processFile(readerOutput, file);
      return InputStatus.MORE_AVAILABLE;
    }
    return InputStatus.END_OF_INPUT;
  }

  private void processFile(ReaderOutput<List<Object>> readerOutput, File file) {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      System.out.println("Reading file: " + file.getName());
      List<Object> listOfLines = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        listOfLines.add(objectMapper.readTree(line).toString());
      }
      readerOutput.collect(listOfLines);
    } catch (IOException e) {
      throw new RuntimeException("Failed to process file: " + file.getName(), e);
    }
  }

  @Override
  public List snapshotState(long l) {
    return List.of();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void addSplits(List<DiskSourceSplit> splits) {
    // No-op for this implementation
  }

  @Override
  public void notifyNoMoreSplits() {
    // No-op for this implementation
  }

  @Override
  public void close() throws Exception {
    System.out.println("Closing DiskSourceReader");
    // No-op for this implementation
  }
}
