package com.alex.flink.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class FieldRemoverMapper implements MapFunction<List<Object>, List<Object>> {
  private final String elementToRemove;

  public FieldRemoverMapper(String elementToRemove) {
    this.elementToRemove = elementToRemove;
  }

  @Override
  public List<Object> map(List<Object> objects) throws Exception {
    List<Object> resultList = new ArrayList<>();
    for (Object jsonObject : objects) {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(jsonObject.toString());

      if (jsonNode.has(elementToRemove)) {
        ((ObjectNode) jsonNode).remove(elementToRemove);
      }
      resultList.add(objectMapper.writeValueAsString(jsonNode));
    }
    return resultList;
  }
}
