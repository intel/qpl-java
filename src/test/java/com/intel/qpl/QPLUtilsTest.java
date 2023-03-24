/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl;

import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QPLUtilsTest {

    private final int compressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId() | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId();

    @Test
    public void testEnums() {
        //test ExecutionPaths enum
        Assert.assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_AUTO.getExecutionPathCode(), 0);
        Assert.assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE.getExecutionPathCode(), 1);
        Assert.assertEquals(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE.getExecutionPathCode(), 2);

        //test Operations enum
        Assert.assertEquals(QPLUtils.Operations.QPL_OP_DECOMPRESS.getOperationCode(), 0);
        Assert.assertEquals(QPLUtils.Operations.QPL_OP_COMPRESS.getOperationCode(), 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCompressionOnBAWrongLength() {
        byte[] srcData = "12345345234572".getBytes(StandardCharsets.UTF_8);
        int n = srcData.length;
        int compressLength = compressedBufferLength(n);
        byte[] compressed = new byte[compressLength];
        QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(srcData, 0, -1, compressed, 0);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testCompressionOnBAWrongSource() {
        byte[] srcData = "234567891233".getBytes(StandardCharsets.UTF_8);
        int n = srcData.length;
        int compressLength = compressedBufferLength(n);
        int srcOffset = compressLength;
        byte[] compressed = new byte[compressLength];
        QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(srcData, srcOffset, srcData.length, compressed, 0);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testByteArrayCompressionOnNegativeOffset() {
        byte[] srcData = "234567891233".getBytes(StandardCharsets.UTF_8);
        int n = srcData.length;
        int compressLength = compressedBufferLength(n);
        byte[] compressed = new byte[compressLength];
        QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(srcData, -1, srcData.length, compressed, 0);
    }

    private int compressedBufferLength(int n) {
        return n + (n >> 12) + (n >> 14) + (n >> 25) + 13;
    }
}
