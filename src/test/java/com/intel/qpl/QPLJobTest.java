/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class QPLJobTest {
    private static final Random RANDOM = new Random();
    private final int compressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId() | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId();
    private final int decompressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId();
    private final QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;

    @Test
    public void testDefaultQPLJobCreation() {
        QPLJob defaultJob = new QPLJob();
        defaultJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        defaultJob.setFlags(compressionFlags);
        assertEquals(defaultJob.getOperationType(), 1);
        assertEquals(defaultJob.getCompressionLevel(), 1);
        assertEquals(defaultJob.getRetryCount(), 0);
        assertEquals(defaultJob.getFlags(), 8195);

    }

    @Test
    public void testParameterizedQPLJobCreation() {

        QPLJob job = new QPLJob(executionPath);
        job.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        job.setFlags(compressionFlags);
        job.setCompressionLevel(3);
        job.setRetryCount(10);

        assertEquals(job.getOperationType(), 1);
        assertEquals(job.getFlags(), 8195);
        assertEquals(job.getCompressionLevel(), 3);
        assertEquals(job.getRetryCount(), 10);

        QPLJob decompressJob = new QPLJob(executionPath);
        decompressJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        decompressJob.setFlags(decompressionFlags);
        decompressJob.setCompressionLevel(3);
        decompressJob.setRetryCount(10);

        assertEquals(decompressJob.getOperationType(), 0);
        assertEquals(decompressJob.getCompressionLevel(), 3);
        assertEquals(decompressJob.getFlags(), 3);
        assertEquals(decompressJob.getRetryCount(), 10);
    }

    @Test
    public void testGetValidExecutionPath() {
        QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
        QPLUtils.ExecutionPaths validExecutionPath = QPLJob.getValidExecutionPath(executionPath);
        assertEquals(executionPath.name(), validExecutionPath.name());

        executionPath = QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE;
        QPLUtils.ExecutionPaths validExecutionPath1 = QPLJob.getValidExecutionPath(executionPath);
        Assert.assertNotNull(validExecutionPath1.name());

        executionPath = QPLUtils.ExecutionPaths.QPL_PATH_AUTO;
        QPLUtils.ExecutionPaths validExecutionPath2 = QPLJob.getValidExecutionPath(executionPath);
        Assert.assertNotNull(validExecutionPath2.name());
    }

    @Test
    public void testGetValidCompressionLevel() {
        int level = QPLUtils.DEFAULT_COMPRESSION_LEVEL;
        QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
        int validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(level, validLevel);

        level = -1;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

        level = 0;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

        level = 1;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(level, validLevel);

        level = 2;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

        level = 3;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(level, validLevel);

        level = 100;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

        level = 2147483647;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

        level = -2147483647;
        validLevel = QPLJob.getValidCompressionLevel(executionPath, level);
        assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testGetQPLJobSizeWrongExecutionPath() {
        // Execution path code 100 is not valid and valid codes are 0,1,2.
        int size = QPLJNI.getQPLJobSize(100);
        assertTrue(size > 0);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testInitQPLJobWrongExecutionPath() {
        int size = QPLJNI.getQPLJobSize(2);
        ByteBuffer bb = ByteBuffer.allocateDirect(size);
        // Execution path code 4 is not valid and valid codes are 0,1,2.
        QPLJNI.initQPLJob(4,bb);
        assertTrue(size > 0);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testQPLFinishWrongJob() {
        ByteBuffer bb = ByteBuffer.allocate(100);
        QPLJNI.finish(bb);
    }

    @Test
    public void testCompressDecompressDirectBB() {
        compressDecompressByteBuffer(executionPath, 1, 10);
    }

    @Test
    public void testCompressDecompressBB() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        executeCompress(qplJob, srcBB, compressedBB);
        compressedBB.flip();
        srcBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressSrcRODstDBB() {
        int n = 100;
        byte[] srcData = new byte[n];
        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer srcBB1 = srcBB.asReadOnlyBuffer();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        executeCompress(qplJob, srcBB1, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressRODBB() {
        int n = 100;
        byte[] srcData = new byte[n];
        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer srcBB1 = srcBB.asReadOnlyBuffer();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        executeCompress(qplJob, srcBB1, compressedBB);
        compressedBB.flip();
        ByteBuffer compressedBB1 = compressedBB.asReadOnlyBuffer();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB1, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressDuplicateSrc() {
        int n = 100;
        byte[] srcData = new byte[n];
        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer srcBB1 = srcBB.duplicate();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        executeCompress(qplJob, srcBB1, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressSliceBB() {
        int n = 100;
        byte[] srcData = new byte[n];
        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        int srcOffset = 2;
        ByteBuffer srcBB = ByteBuffer.allocate(srcOffset + n);
        srcBB.position(srcOffset);
        srcBB.put(srcData, 0, n);
        srcBB.flip().position(srcOffset);
        ByteBuffer srcBB1 = srcBB.slice().asReadOnlyBuffer();
        int compressOffset = 3;
        int compressedSize = compressOffset + compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        compressedBB.position(compressOffset);
        ByteBuffer compressedBBSlice = compressedBB.slice();
        QPLJob qplJob = new QPLJob(executionPath);
        executeCompress(qplJob, srcBB1, compressedBBSlice);
        compressedBBSlice.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBBSlice, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressSrcDBBDstBB() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBB, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        srcBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressDecompressSrcBBDstDBB() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBB, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        srcBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test(expectedExceptions = ReadOnlyBufferException.class)
    public void testCompressSrcRODstRO() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer srcBBRO = srcBB.asReadOnlyBuffer();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        ByteBuffer compressedBBRO = compressedBB.asReadOnlyBuffer();
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBBRO, compressedBBRO);
    }


    @Test
    public void testCompressDecompressSrcRODstBB() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer srcBBRO = srcBB.asReadOnlyBuffer();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBBRO, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        srcBBRO.flip();
        assertEquals(resultBB.compareTo(srcBBRO), 0);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testCompressSameSrcAndDst() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBB, srcBB);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testCompressInsufficientDstLength() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(n);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBB, compressedBB);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testUnsupportedCompressionLevel() {
        compressDecompressByteBuffer(executionPath, 100, 0);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testCompressDecompressNegativeCompressionLevel() {
        compressDecompressByteBuffer(executionPath, -1, 0);
    }

    @Test
    public void testCompressDecompressByteArray() {
        compressDecompressByteArray(executionPath, 1, 0);
        compressDecompressByteArray(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 3, 10);
    }

    @Test
    public void testCompressDecompressRandomByteArray() {
        int n = 66560;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, 0, src.length, compressed, 0);

        byte[] result = new byte[n];
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.execute(compressed, 0, compressed.length, result, 0);
        assertEquals(result, src);
    }

    @Test(expectedExceptions = QPLException.class)
    public void testCompressWrongFlag() {
        int n = 66560;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(-2147483647);
        qplJob.execute(src, 0, src.length, compressed, 0);      
    }
    @Test(expectedExceptions = QPLException.class)
    public void testCompressWrongCL() {
        int n = 66560;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setCompressionLevel(-2147483647);
        qplJob.execute(src, 0, src.length, compressed, 0);      
    }

    @Test
    public void testCompressDecompress() {
        int n = 66560;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, 0, src.length, compressed, 0);

        byte[] result = new byte[n];
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.execute(compressed, 0, compressed.length, result, 0);
        assertEquals(result, src);
    }

    @Test
    public void testCompressDecompressAtSpecifiedOffset() {
        int n = 20;
        byte[] src = new byte[n];

        for (int i = 0; i < n; i++)
            src[i] = (byte) i;

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, 0, src.length, compressed, 0);

        int resultOffset = 5;
        byte[] result = new byte[resultOffset + n];
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        int resultLength = qplJob.execute(compressed, 0, compressed.length, result, resultOffset);
        assertEquals(resultLength, n);
        assertArrayEquals(Arrays.copyOfRange(src, 0, src.length),
                Arrays.copyOfRange(result, resultOffset, resultOffset + resultLength));

    }

    @Test
    public void testCompressDecompressBBAtSpecifiedOffset() {
        int n = 20;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        int srcOffset = 2;
        ByteBuffer srcBB = ByteBuffer.allocate(srcOffset + n);
        srcBB.put(srcData, 1, n - 1);
        srcBB.flip().position(srcOffset);
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBB, compressedBB);
        compressedBB.flip();

        int resultOffset = 5;
        ByteBuffer resultBB = ByteBuffer.allocate(resultOffset + n);
        resultBB.position(resultOffset);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip().position(resultOffset);
        srcBB.flip().position(srcOffset);
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test
    public void testCompressLargeOffset() {
        int n = 1000000;
        int offset = 999999;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, offset, src.length-offset, compressed, 0);

        byte[] result = new byte[n-offset];
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.execute(compressed, 0, compressed.length, result, 0);
        byte [] srcRang = Arrays.copyOfRange(src,offset,n);
        assertEquals(result, srcRang);

    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testCompressBigLength() {
        int n = 10000000;
        int offset = 9999999;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, offset, src.length, compressed, 0);
    }

    @Test
    public void testCompressDecompressBBSlice() {
        int n = 20;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;
        int srcOffset = 2;
        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 1, n - 1);
        srcBB.flip().position(srcOffset);
        ByteBuffer srcBBSlice = srcBB.slice();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob();
        executeCompress(qplJob, srcBBSlice, compressedBB);
        compressedBB.flip();

        int resultOffset = 5;
        ByteBuffer resultBB = ByteBuffer.allocate(resultOffset + n);
        resultBB.position(resultOffset);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip().position(resultOffset);
        srcBBSlice.flip();
        assertEquals(resultBB.compareTo(srcBBSlice), 0);
    }


    @Test(expectedExceptions = QPLException.class)
    public void testCompressEmptyDBB() {
        byte[] data = new byte[0];
        int n = data.length;
        int offset = 2;
        ByteBuffer src = ByteBuffer.allocateDirect(n + offset);
        src.position(offset);
        src.put(data, 0, n);
        src.flip().position(offset);

        int outOffset = 3;
        int compressedLength = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedLength);
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        compressedBB.position(outOffset);
        qplJob.execute(src, compressedBB);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testCompressReleasedQPLResource() {
        int n = 66560;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.doClear();
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, 0, src.length, compressed, 0);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDecompressClosedQPLJob() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.doClear();
        executeCompress(qplJob, srcBB, compressedBB);
        compressedBB.flip();
        srcBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocate(n);
        executeDecompress(qplJob, compressedBB, resultBB);
        resultBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testReleaseResourcesClosedQPLJob() {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        ByteBuffer srcBB = ByteBuffer.allocate(n);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.doClear();
        qplJob.doClear();
        executeCompress(qplJob, srcBB, compressedBB);
    }

    @Test (expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testCompressEmptyBB() {
        int n = 66560;
        byte[] emptySrcArray = new byte[0];
        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(emptySrcArray, compressed);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testCompressWithNegativeOffset() {
        int n = 65536;
        //offset should be >=0
        int offset = -2147483648;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, offset, src.length, compressed, 0);
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testCompressIntegerOverflow() {
        int n = 65536;
        byte[] src = new byte[n];
        RANDOM.nextBytes(src);

        int compressedSize = compressedBufferLength(n);
        byte[] compressed = new byte[compressedSize];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(src, 2147483647, src.length, compressed, 0);
    }


    private void compressDecompressByteBuffer(QPLUtils.ExecutionPaths executionPath, int compressionLevel, int retryCount) {
        int n = 100;
        byte[] srcData = new byte[n];

        for (int i = 0; i < n; i++)
            srcData[i] = (byte) i;

        final int inOffset = 0;
        ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
        srcBB.position(inOffset);
        srcBB.put(srcData, 0, n);
        srcBB.flip();
        int compressedSize = compressedBufferLength(n);
        ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.setCompressionLevel(compressionLevel);
        qplJob.setRetryCount(retryCount);
        qplJob.execute(srcBB, compressedBB);
        compressedBB.flip();

        ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.setRetryCount(retryCount);
        qplJob.execute(compressedBB, resultBB);
        resultBB.flip();
        srcBB.flip();
        assertEquals(resultBB.compareTo(srcBB), 0);
    }

    private void compressDecompressByteArray(QPLUtils.ExecutionPaths executionPath, int compressionLevel, int retryCount) {
        byte[] src = "12345345234572".getBytes(StandardCharsets.UTF_8);
        final int n = src.length;
        int compressLength = compressedBufferLength(n);

        byte[] compressed = new byte[compressLength];
        QPLJob qplJob = new QPLJob(executionPath);
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.setCompressionLevel(compressionLevel);
        qplJob.setRetryCount(retryCount);
        qplJob.execute(src, compressed);

        byte[] result = new byte[n];
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.setCompressionLevel(compressionLevel);
        qplJob.setRetryCount(retryCount);
        qplJob.execute(compressed, result);
        assertEquals(src, result);
    }

    private int compressedBufferLength(int n) {
        return n + (n >> 12) + (n >> 14) + (n >> 25) + 13;
    }

    private void executeCompress(QPLJob qplJob, ByteBuffer srcBB, ByteBuffer compressedBB) {
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
        qplJob.setFlags(compressionFlags);
        qplJob.setCompressionLevel(1);
        qplJob.execute(srcBB, compressedBB);
    }

    private void executeDecompress(QPLJob qplJob, ByteBuffer compressedBB, ByteBuffer resultBB) {
        qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
        qplJob.setFlags(decompressionFlags);
        qplJob.setRetryCount(0);
        qplJob.execute(compressedBB, resultBB);
    }
}
