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

public class QPLCompressorExample {
  public static void main(String[] args) {
    compressDecompress();
    compressDecompressWithOptions();
  }

  private static void compressDecompress() {

    byte[] uncompressedBytes = "12345345234572".getBytes(StandardCharsets.UTF_8);
    int compressedLength = QPLJob.maxCompressedLength(uncompressedBytes.length);
    byte[] compressedBytes = new byte[compressedLength];

    // Create  QPLCompressor for compression
    QPLCompressor comp = new QPLCompressor();
    comp.compress(uncompressedBytes, compressedBytes);
    comp.doClear();

    // Create  QPLDecompressJob for decompression
    byte[] uncompressedResult = new byte[uncompressedBytes.length];
    QPLCompressor decomp = new QPLCompressor();
    decomp.decompress(compressedBytes, uncompressedResult);
    decomp.doClear();

    if (Arrays.equals(uncompressedBytes, uncompressedResult)) {
      System.out.println("**************************************************");
      System.out.println("Compress/Decompress succeeded");
      System.out.println("**************************************************");
    } else {
      System.out.println("**************************************************");
      System.out.println("Compress/Decompress failed");
      System.out.println("**************************************************");
    }
  }

  private static void compressDecompressWithOptions() {

    byte[] uncompressedBytes = "12345345234572".getBytes(StandardCharsets.UTF_8);
    int compressedLength = QPLJob.maxCompressedLength(uncompressedBytes.length);
    byte[] compressedBytes = new byte[compressedLength];

    // Create  QPLCompressor for compression
    QPLCompressor comp =
        new QPLCompressor(
            QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE,
            QPLUtils.DEFAULT_COMPRESSION_LEVEL,
            QPLUtils.DEFAULT_RETRY_COUNT);
    comp.compress(uncompressedBytes, compressedBytes);
    comp.doClear();

    // Create  QPLCompressor for decompression
    byte[] uncompressedResult = new byte[uncompressedBytes.length];
    QPLCompressor decomp =
        new QPLCompressor(
            QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE,
            QPLUtils.DEFAULT_COMPRESSION_LEVEL,
            QPLUtils.DEFAULT_RETRY_COUNT);
    decomp.decompress(compressedBytes, uncompressedResult);
    decomp.doClear();

    if (Arrays.equals(uncompressedBytes, uncompressedResult)) {
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
