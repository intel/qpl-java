/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class QPLUtilsTest {

  private final int compressionFlags =
      QPLUtils.Flags.QPL_FLAG_FIRST.getId()
          | QPLUtils.Flags.QPL_FLAG_LAST.getId()
          | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId();

  @Test
  public void testEnums() {
    // test ExecutionPaths enum
    assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_AUTO.getExecutionPathCode(), 0);
    assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE.getExecutionPathCode(), 1);
    assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE.getExecutionPathCode(), 2);

    // test Operations enum
    assertEquals(QPLUtils.Operations.QPL_OP_DECOMPRESS.getOperationCode(), 0);
    assertEquals(QPLUtils.Operations.QPL_OP_COMPRESS.getOperationCode(), 1);
  }

  @Test
  public void testCompressionOnBAWrongLength() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          byte[] srcData = "12345345234572".getBytes(StandardCharsets.UTF_8);
          int n = srcData.length;
          int compressLength = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressLength];
          QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(srcData, 0, -1, compressed, 0, compressLength);
        });
  }

  @Test
  public void testCompressionOnBAWrongSource() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          byte[] srcData = "234567891233".getBytes(StandardCharsets.UTF_8);
          int n = srcData.length;
          int compressLength = QPLJob.maxCompressedLength(n);
          int srcOffset = compressLength;
          byte[] compressed = new byte[compressLength];
          QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(srcData, srcOffset, srcData.length, compressed, 0, compressLength);
        });
  }

  @Test
  public void testByteArrayCompressionOnNegativeOffset() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          byte[] srcData = "234567891233".getBytes(StandardCharsets.UTF_8);
          int n = srcData.length;
          int compressLength = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressLength];
          QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(srcData, -1, srcData.length, compressed, 0, compressLength);
        });
  }
}
