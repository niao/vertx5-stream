package com.example;

import io.vertx.core.json.JsonObject;


public record ItemDto(
  Long id,
  String name,
  Integer sequenceNumber
  // description намеренно исключён из DTO
) {


  public JsonObject toJson() {
    return new JsonObject()
      .put("id", id)
      .put("name", name)
      .put("sequenceNumber", sequenceNumber);
  }

}
