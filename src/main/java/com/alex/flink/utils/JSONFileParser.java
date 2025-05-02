package com.alex.flink.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.source.ReaderOutput;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JSONFileParser {

  public static void processJsonFile(ReaderOutput<List<Object>> readerOutput, BufferedReader reader, ObjectMapper objectMapper) throws IOException {
    List<Object> listOfLines = new ArrayList<>();
    String line;
    while ((line = reader.readLine()) != null) {
      listOfLines.add(objectMapper.readTree(line).toString());
    }
    readerOutput.collect(listOfLines);
  }
}
