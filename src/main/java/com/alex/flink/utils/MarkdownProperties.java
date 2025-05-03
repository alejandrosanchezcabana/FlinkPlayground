package com.alex.flink.utils;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MarkdownProperties extends Properties {

  private static final Pattern INLINE_PROPERTY_PATTERN = Pattern.compile("^([a-zA-Z0-9_.]+)=(.+)$");

  public MarkdownProperties() {
    super();
  }

  @Override
  public void load(InputStream inputStream) {
    Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8);
    readProperties(scanner);
    scanner.close();
  }

  @Override
  public void load(Reader reader) {
    Scanner scanner = new Scanner(reader);
    readProperties(scanner);
    scanner.close();
  }

  @Override
  public void loadFromXML(InputStream stream) {
    throw new RuntimeException("The method is not supported by this class");
  }

  private void readProperties(Scanner scanner) {
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine().trim();

      // Match inline properties
      Matcher inlineMatcher = INLINE_PROPERTY_PATTERN.matcher(line);
      if (inlineMatcher.matches()) {
        String key = inlineMatcher.group(1).trim();
        String value = inlineMatcher.group(2).trim();
        this.setProperty(key, value);
      }
    }
  }
}
