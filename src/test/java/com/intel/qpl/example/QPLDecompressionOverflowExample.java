/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl.example;

import com.intel.qpl.QPLCompressor;
import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class QPLDecompressionOverflowExample {
  public static void main(String[] args) {
    compressDecompress();
  }

  private static void compressDecompress() {

    byte[] uncompressedBytes = "12345345234572".getBytes(StandardCharsets.UTF_8);
    int compressedLength = QPLJob.maxCompressedLength(uncompressedBytes.length);
    byte[] compressedBytes = new byte[compressedLength];

    // Create  QPLCompressor for compression
    QPLCompressor compressor =
        new QPLCompressor(
            QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE,
            QPLUtils.DEFAULT_COMPRESSION_LEVEL,
            QPLUtils.DEFAULT_RETRY_COUNT);
    int compressedLen = compressor.compress(uncompressedBytes, compressedBytes);

    // passing insufficient uncompressedBytes length
    byte[] uncompressedResult = new byte[uncompressedBytes.length / 2];
    int compressedOffset = 0;
    int uncompressOffest = 0;
    byte[] mergedBytes = new byte[uncompressedBytes.length];

    compressor.decompress(
        compressedBytes, 0, compressedLen, uncompressedResult, 0, uncompressedResult.length);
    while (compressor.isOutputInsufficient()) {
      System.arraycopy(
          uncompressedResult, 0, mergedBytes, uncompressOffest, compressor.getBytesWritten());
      compressedOffset += compressor.getBytesRead();
      uncompressOffest += compressor.getBytesWritten();
      compressor.decompress(
          compressedBytes,
          compressedOffset,
          compressedLen - compressedOffset,
          uncompressedResult,
          0,
          uncompressedResult.length);
    }
    System.arraycopy(
        uncompressedResult, 0, mergedBytes, uncompressOffest, compressor.getBytesWritten());

    if (Arrays.equals(uncompressedBytes, mergedBytes)) {
      System.out.println("**************************************************");
      System.out.println("Compress/Decompress succeeded");
      System.out.println("**************************************************");
    } else {
      System.out.println("**************************************************");
      System.out.println("Compress/Decompress failed");
      System.out.println("**************************************************");
    }
  }
}
