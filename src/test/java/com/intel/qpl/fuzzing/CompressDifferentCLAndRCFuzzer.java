/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl.fuzzing;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.intel.qpl.QPLException;
import com.intel.qpl.QPLJob;
import java.util.Arrays;

public class CompressDifferentCLAndRCFuzzer {
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    try {
      int compressionLevel = data.consumeInt();
      int retryCount = data.consumeInt();
      byte[] src = data.consumeRemainingAsBytes();
      int size = src.length;
      int compressLength = QPLJob.maxCompressedLength(size);
      byte[] dst = new byte[compressLength];

      QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
      qplJob.setOperationType(CompressorConstants.compress);
      qplJob.setFlags(CompressorConstants.compressionFlags);
      qplJob.setCompressionLevel(compressionLevel);
      qplJob.setRetryCount(retryCount);
      qplJob.execute(src, dst);
      byte[] result = new byte[size];
      qplJob.setOperationType(CompressorConstants.decompress);
      qplJob.setFlags(CompressorConstants.decompressionFlags);
      qplJob.setRetryCount(retryCount);
      qplJob.execute(dst, result);
      qplJob.doClear();
      assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
    } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ignored) {
    } catch (QPLException ignored) {
      final String clMessage = "Error occurred while executing job. Status code is - 87";
      final String srcMessage = "Error occurred while executing job. Status code is - 319";
      if (!ignored.getMessage().equalsIgnoreCase(clMessage)
          && !ignored.getMessage().equalsIgnoreCase(srcMessage)) {
        throw ignored;
      }
    }
  }
}
