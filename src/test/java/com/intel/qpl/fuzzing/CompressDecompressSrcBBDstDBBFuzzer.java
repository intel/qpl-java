/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl.fuzzing;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.intel.qpl.QPLException;
import com.intel.qpl.QPLJob;
import java.nio.ByteBuffer;

public class CompressDecompressSrcBBDstDBBFuzzer {
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    byte[] srcData = data.consumeRemainingAsBytes();
    int n = srcData.length;
    if (n <= 0) {
      return; // qpl does not work on zero or empty ByteBuffers
    }
    try {
      ByteBuffer srcBB = ByteBuffer.allocate(n);
      srcBB.put(srcData, 0, n);
      srcBB.flip();
      int compressedSize = QPLJob.maxCompressedLength(n);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
      qplJob.setOperationType(CompressorConstants.compress);
      qplJob.setFlags(CompressorConstants.compressionFlags);
      qplJob.execute(srcBB, compressedBB);
      compressedBB.flip();
      srcBB.flip();

      ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
      qplJob.setOperationType(CompressorConstants.decompress);
      qplJob.setFlags(CompressorConstants.decompressionFlags);
      qplJob.setRetryCount(0);
      qplJob.execute(compressedBB, resultBB);
      resultBB.flip();
      qplJob.doClear();
      assert resultBB.compareTo(srcBB) == 0 : "Source and decompressed Bytebuffer is not equal";
    } catch (QPLException ignored) {
      final String expectedErrorMessage = "Error occurred while executing job. Status code is - 319";
      if (!ignored.getMessage().equalsIgnoreCase(expectedErrorMessage)) {
        throw ignored;
      }
    }
  }
}
