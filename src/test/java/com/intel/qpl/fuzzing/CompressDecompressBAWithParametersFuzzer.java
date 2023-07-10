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

public class CompressDecompressBAWithParametersFuzzer {
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    try {
      int srcOffset = data.consumeInt();
      int srcLength = data.consumeInt();
      byte[] src = data.consumeRemainingAsBytes();

      int compressLength = QPLJob.maxCompressedLength(srcLength);

      byte[] dst = new byte[compressLength];
      QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
      qplJob.setOperationType(CompressorConstants.compress);
      qplJob.setFlags(CompressorConstants.compressionFlags);
      qplJob.setCompressionLevel(1);
      qplJob.execute(src, srcOffset, srcLength, dst, 0);
      byte[] result = new byte[src.length];
      qplJob.setOperationType(CompressorConstants.decompress);
      qplJob.setFlags(CompressorConstants.decompressionFlags);
      qplJob.setRetryCount(0);
      qplJob.execute(dst, result);
      qplJob.doClear();
      // Extract Sub src array based on srcOffset and srcLength
      byte[] srcRang = Arrays.copyOfRange(src, srcOffset, srcLength);
      byte[] resRang = Arrays.copyOfRange(result, 0, srcLength - srcOffset);
      assert Arrays.equals(srcRang, resRang) : "Source and decompressed bytes are not equal";

    } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ignored) {
    } catch (QPLException ignored) {
      final String expectedErrorMessage = "Error occurred while executing job. Status code is - 319";
      if (!ignored.getMessage().equalsIgnoreCase(expectedErrorMessage)) {
        throw ignored;
      }
    }
  }
}
