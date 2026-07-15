package com.example.util;

import io.vertx.core.buffer.Buffer;

public class ProtobufUtils {
  public static Buffer encodeVarint(int value) {
    byte[] bytes = new byte[5];
    int idx = 0;
    while (value > 0x7F) {
      bytes[idx++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    bytes[idx++] = (byte) (value & 0x7F);
    return Buffer.buffer(bytes).slice(0, idx);
  }
}
