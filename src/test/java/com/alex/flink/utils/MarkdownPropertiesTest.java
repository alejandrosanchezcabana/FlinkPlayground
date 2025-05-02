package com.alex.flink.utils;

import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class MarkdownPropertiesTest {

  @Test
  public void testLoadInlineProperties() throws Exception {
    String inlineProperties = """
        this.is.property.name=this.is.property.value
        another.property.key=another.property.value
        """;

    MarkdownProperties properties = new MarkdownProperties();
    properties.load(new StringReader(inlineProperties));

    assertEquals("this.is.property.value", properties.getProperty("this.is.property.name"));
    assertEquals("another.property.value", properties.getProperty("another.property.key"));
  }
}
