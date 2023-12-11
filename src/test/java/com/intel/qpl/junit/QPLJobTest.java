/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl.junit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.intel.qpl.QPLException;
import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLOutputOverflowException;
import com.intel.qpl.QPLUtils;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class QPLJobTest {

  private static final Random RANDOM = new Random();
  private final int compressionFlags =
      QPLUtils.Flags.QPL_FLAG_FIRST.getId()
          | QPLUtils.Flags.QPL_FLAG_LAST.getId()
          | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId()
          | QPLUtils.Flags.QPL_FLAG_OMIT_VERIFY.getId();
  private final int decompressionFlags =
      QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId();
  private final QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
  private final String FILE_PATH = "README.md";

  public static Stream<Arguments> provideUnsupportedCL() {
    return QPLTestSuite.FORCE_HARDWARE
        ? Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 10),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, -2147483647),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, -1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 2147483645),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 3),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 10),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 2147483645),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, -1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_AUTO, -1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_AUTO, 2147483645),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_AUTO, -1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_AUTO, 0))
        : Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 10),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, -2147483647),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, -1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 2147483645));
  }

  public static Stream<Arguments> provideAllParams() {
    return QPLTestSuite.FORCE_HARDWARE
        ? Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 3, 100),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 15),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_AUTO, 1, 1))
        : Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 0),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 3, 100));
  }

  public static Stream<Arguments> provideParamsLength() {
    return QPLTestSuite.FORCE_HARDWARE
        ? Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1000, getSrcArray(1000)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1024, getSrcArray(1024)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 2048, getSrcArray(2048)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 3072, getSrcArray(3072)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 4096, getSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 16384, getSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 3, 65536, getSrcArray(65536)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 524288, getSrcArray(524288)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 1048576, getSrcArray(1048576)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 3, 1048577, getSrcArray(1048577)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 2097150, getSrcArray(2097150)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 65536, getSrcArray(65536)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 1000, getSrcArray(1000)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 1024, getSrcArray(1024)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 2048, getSrcArray(2048)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 3072, getSrcArray(3072)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 4096, getSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 2, 16384, getSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 3, 65536, getSrcArray(65536)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 2, 1048576, getSrcArray(1048576)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 3, 1048577, getSrcArray(1048577)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 2097150, getSrcArray(2097150)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 2, 2097153, getSrcArray(2097153)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1000, getRandomSrcArray(1000)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1024, getRandomSrcArray(1024)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 2048, getRandomSrcArray(2048)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 3072, getRandomSrcArray(3072)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 4096, getRandomSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 16384, getRandomSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 3, 65536, getRandomSrcArray(65536)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 1000, getRandomSrcArray(1000)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 1024, getRandomSrcArray(1024)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 2048, getRandomSrcArray(2048)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 3072, getRandomSrcArray(3072)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 1, 4096, getRandomSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 2, 16384, getRandomSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, 1, 3, 65536, getRandomSrcArray(65536)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                1048576,
                getRandomSrcArray(1048576)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                1048577,
                getRandomSrcArray(1048577)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                2097150,
                getRandomSrcArray(2097150)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                2097153,
                getRandomSrcArray(2097153)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                33554432,
                getRandomSrcArray(33554432)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                2,
                10485760,
                getRandomSrcArray(10485760)))
        : Stream.of(
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1000, getSrcArray(1000)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1024, getSrcArray(1024)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 2048, getSrcArray(2048)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 3072, getSrcArray(3072)),
            Arguments.of(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 4096, getSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 16384, getSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 3, 65536, getSrcArray(65536)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1000, getRandomSrcArray(1000)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 1024, getRandomSrcArray(1024)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 2048, getRandomSrcArray(2048)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 3072, getRandomSrcArray(3072)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 1, 4096, getRandomSrcArray(4096)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 2, 16384, getRandomSrcArray(16384)),
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 3, 65536, getRandomSrcArray(65536)));
  }

  public static Stream<Arguments> paramsWithDifferentChunk() {
    return QPLTestSuite.FORCE_HARDWARE
        ? Stream.of(
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
                1,
                11,
                2097153,
                2097152,
                getSrcArray(2097153)))
        : Stream.of(
            Arguments.of(
                QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE,
                1,
                11,
                2097153,
                2097152,
                getSrcArray(2097153)));
  }

  public static Stream<Arguments> paramsWithLargeLength() {
    return Stream.of(
        Arguments.of(1, 11, 2097153, getSrcArray(2097153)),
        Arguments.of(1, 11, 131071, getRandomSrcArray(131071)),
        Arguments.of(1, 11, 2621440, getSrcArray(2621440)));
  }

  public static boolean shouldSkip(QPLUtils.ExecutionPaths ePath) {
    return (ePath.equals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE)
            || ePath.equals(QPLUtils.ExecutionPaths.QPL_PATH_AUTO))
        && !QPLTestSuite.FORCE_HARDWARE;
  }

  private static byte[] getSrcArray(int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) bytes[i] = (byte) 10;
    return bytes;
  }

  private static byte[] getRandomSrcArray(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  private ByteBuffer getSourceDirectBB(byte[] srcData) {
    int len = srcData.length;
    final int inOffset = 0;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(len);
    srcBB.position(inOffset);
    srcBB.put(srcData, 0, len);
    srcBB.flip();
    return srcBB;
  }

  private ByteBuffer getSourceBB(byte[] srcData) {
    int len = srcData.length;
    final int inOffset = 0;
    ByteBuffer srcBB = ByteBuffer.allocate(len);
    srcBB.position(inOffset);
    srcBB.put(srcData, 0, len);
    srcBB.flip();
    return srcBB;
  }

  private byte[] readAllBytes(String fileName) throws IOException {
    return Files.readAllBytes(Path.of(fileName));
  }

  @Test
  public void testDefaultQPLJobCreation() {
    assumeFalse(shouldSkip(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE));
    QPLJob defaultJob = new QPLJob();
    defaultJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    defaultJob.setFlags(compressionFlags);
    assertEquals(defaultJob.getOperationType(), 1);
    assertEquals(defaultJob.getCompressionLevel(), 1);
    assertEquals(defaultJob.getRetryCount(), 0);
    assertEquals(defaultJob.getFlags(), 24579);
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testParameterizedQPLJobCreation(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    QPLJob job = new QPLJob(ePath);
    job.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    job.setFlags(compressionFlags);
    job.setCompressionLevel(cl);
    job.setRetryCount(rt);

    assertEquals(job.getOperationType(), 1);
    assertEquals(job.getFlags(), 24579);
    assertEquals(job.getCompressionLevel(), cl);
    assertEquals(job.getRetryCount(), rt);

    QPLJob decompressJob = new QPLJob(ePath);
    decompressJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    decompressJob.setFlags(decompressionFlags);
    decompressJob.setCompressionLevel(cl);
    decompressJob.setRetryCount(rt);

    assertEquals(decompressJob.getOperationType(), 0);
    assertEquals(decompressJob.getFlags(), 3);
    assertEquals(decompressJob.getRetryCount(), rt);
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testGetValidExecutionPath(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    QPLUtils.ExecutionPaths validExecutionPath = QPLJob.getValidExecutionPath(ePath);
    assertEquals(ePath.name(), validExecutionPath.name());
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

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testMaxCompressedLength(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          int srcLen = Integer.MAX_VALUE - 2;
          int estimatedDestLen = QPLJob.maxCompressedLength(srcLen);
          assert estimatedDestLen > 0;
        });
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressDBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    srcBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();
    srcBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressSrcRODstDBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    ByteBuffer srcBB1 = srcBB.asReadOnlyBuffer();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB1, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressRODBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    ByteBuffer srcBB1 = srcBB.asReadOnlyBuffer();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB1, compressedBB, cl, rt);
    compressedBB.flip();
    ByteBuffer compressedBB1 = compressedBB.asReadOnlyBuffer();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB1, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressDuplicateSrc(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    ByteBuffer srcBB1 = srcBB.duplicate();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB1, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressSrcDBBDstBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    srcBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressSrcBBDstDBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    srcBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressSliceBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocate(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    ByteBuffer srcSlice = src.slice();
    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocate(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    ByteBuffer compressedSlice = compressed.slice();
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcSlice, compressedSlice, cl, rt);
    assertEquals(n, srcSlice.position());
    assertEquals(n, srcSlice.limit());
    compressedSlice.flip();
    ByteBuffer result = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedSlice, result, rt);

    int decompressed = result.position();
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i)
      assert data[i] == result.get(i) : "Failed comparison on index: " + i;
  }

  @Test
  public void testCompressSrcRODstRO() {
    int n = 100;
    byte[] srcData = new byte[n];

    for (int i = 0; i < n; i++) srcData[i] = (byte) i;

    ByteBuffer srcBB = ByteBuffer.allocate(n);
    srcBB.put(srcData, 0, n);
    srcBB.flip();
    ByteBuffer srcBBRO = srcBB.asReadOnlyBuffer();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer compressedBBRO = compressedBB.asReadOnlyBuffer();
    assertThrows(
        ReadOnlyBufferException.class,
        () -> {
          QPLJob qplJob = new QPLJob(executionPath);
          executeCompress(qplJob, srcBBRO, compressedBBRO, 1, 1);
        });
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressSrcRODstBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    ByteBuffer srcBBRO = srcBB.asReadOnlyBuffer();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBBRO, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    srcBBRO.flip();
    assertEquals(resultBB.compareTo(srcBBRO), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBBVaryingOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocate(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocate(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, src, compressed, cl, rt);
    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocate(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);
    executeDecompress(qplJob, compressed, result, rt);
    assertEquals(outOffset + remaining, compressed.position());
    assertEquals(outOffset + remaining, compressed.limit());
    assertEquals(result.capacity() - inOffset, result.limit());

    int decompressed = result.position() - inOffset;
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i)
      assert data[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testDBBVaryingOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocateDirect(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, src, compressed, cl, rt);
    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);
    executeDecompress(qplJob, compressed, result, rt);
    assertEquals(outOffset + remaining, compressed.position());
    assertEquals(outOffset + remaining, compressed.limit());
    assertEquals(result.capacity() - inOffset, result.limit());

    int decompressed = result.position() - inOffset;
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i) {
      assert data[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
    }
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testSrcROVaryingOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    ByteBuffer srcBB1 = src.asReadOnlyBuffer();

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocateDirect(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB1, compressed, cl, rt);
    assertEquals(inOffset + n, srcBB1.position());
    assertEquals(inOffset + n, srcBB1.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);
    executeDecompress(qplJob, compressed, result, rt);
    assertEquals(outOffset + remaining, compressed.position());
    assertEquals(outOffset + remaining, compressed.limit());
    assertEquals(result.capacity() - inOffset, result.limit());

    int decompressed = result.position() - inOffset;
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i)
      assert data[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testDBDecompressionOverflow(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n / 2);
    ByteBuffer tmpBB = ByteBuffer.allocateDirect(n);
    ByteBuffer mergeBB;
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressedBB, resultBB);

    while (qplJob.isOutputInsufficient()) {
      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(compressedBB, tmpBB);
    }

    int totalCapacity = resultBB.limit() + tmpBB.limit();
    mergeBB = ByteBuffer.allocateDirect(totalCapacity);
    int position = resultBB.position();
    resultBB.flip();
    mergeBB.put(resultBB);
    mergeBB.position(position);
    tmpBB.flip();
    mergeBB.put(tmpBB);

    srcBB.flip();
    mergeBB.flip();
    for (int i = 0; i < n; ++i)
      assert srcBB.get(i) == mergeBB.get(i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBBDecompressionOverflow(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n / 2);
    ByteBuffer tmpBB = ByteBuffer.allocate(n);
    ByteBuffer mergeBB;

    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressedBB, resultBB);
    while (qplJob.isOutputInsufficient()) {
      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(compressedBB, tmpBB);
    }
    int totalCapacity = resultBB.limit() + tmpBB.limit();
    mergeBB = ByteBuffer.allocate(totalCapacity);
    int position = resultBB.position();
    resultBB.flip();
    mergeBB.put(resultBB);
    mergeBB.position(position);
    tmpBB.flip();
    mergeBB.put(tmpBB);
    srcBB.flip();
    mergeBB.flip();
    for (int i = 0; i < n; ++i)
      assert srcBB.get(i) == mergeBB.get(i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testDecompressionOverflowDBBVaryingOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocate(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocateDirect(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, src, compressed, cl, rt);
    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);

    ByteBuffer result = ByteBuffer.allocate((inOffset + n + inOffset) / 2);
    result.position(inOffset).limit(result.capacity() - inOffset);
    ByteBuffer tmpBB = ByteBuffer.allocate(n);
    ByteBuffer mergeBB;

    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressed, result);
    while (qplJob.isOutputInsufficient()) {

      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(compressed, tmpBB);
    }
    int totalCapacity = result.limit() + tmpBB.limit();
    mergeBB = ByteBuffer.allocate(totalCapacity);
    int position = result.position() - inOffset;
    result.flip().position(inOffset);
    mergeBB.put(result);
    mergeBB.position(position);
    tmpBB.flip();
    mergeBB.put(tmpBB);
    src.flip().position(inOffset);
    mergeBB.flip();

    for (int i = 0; i < n; ++i) {

      assert mergeBB.get(i) == src.get(inOffset + i) : "Failed comparison on index: " + i;
    }
  }

  @Test
  public void testCompressSameSrcAndDst() {
    assertThrows(
        QPLException.class,
        () -> {
          int n = 100;
          byte[] srcData = new byte[n];

          for (int i = 0; i < n; i++) srcData[i] = (byte) i;
          ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
          srcBB.put(srcData, 0, n);
          srcBB.flip();
          QPLJob qplJob = new QPLJob(executionPath);
          executeCompress(qplJob, srcBB, srcBB, 1, 1);
        });
  }

  @Test
  public void testCompressInsufficientDstLength() {
    assertThrows(
        QPLException.class,
        () -> {
          int n = 100;
          byte[] srcData = new byte[n];

          for (int i = 0; i < n; i++) srcData[i] = (byte) i;
          ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
          srcBB.put(srcData, 0, n);
          srcBB.flip();
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(n);
          QPLJob qplJob = new QPLJob(executionPath);
          executeCompress(qplJob, srcBB, compressedBB, 1, 1);
        });
  }

  @ParameterizedTest
  @MethodSource("provideUnsupportedCL")
  public void testCompressBBWrongCL(QPLUtils.ExecutionPaths ePath, int cl) {
    assertThrows(
        QPLException.class,
        () -> {
          int n = 100;
          ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
          srcBB.position(0);
          srcBB.put(getSrcArray(n), 0, n);
          srcBB.flip();
          int compressedSize = QPLJob.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
          QPLJob qplJob = new QPLJob(ePath);
          executeCompress(qplJob, srcBB, compressedBB, cl, 1);
          compressedBB.flip();
        });
  }

  @Test
  public void testCompressWrongCL() {
    assertThrows(
        QPLException.class,
        () -> {
          int n = 66560;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(executionPath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setCompressionLevel(-2147483647);
          qplJob.execute(src, 0, src.length, compressed, 0, compressedSize);
        });
  }

  @Test
  public void testCompressNegativeRetryCount() {
    int n = 66560;
    byte[] src = new byte[n];
    RANDOM.nextBytes(src);

    int compressedSize = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressedSize];
    QPLJob qplJob = new QPLJob(executionPath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(1);
    qplJob.setFlags(compressionFlags);
    qplJob.setRetryCount(-2147483647);
    qplJob.execute(src, 0, src.length, compressed, 0, compressedSize);

    byte[] result = new byte[n];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(-2147483647);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressed, 0, compressed.length, result, 0, result.length);
    assertArrayEquals(result, src);
  }

  @Test
  public void testCompressDecompressAtSpecifiedOffset() {
    int n = 20;
    byte[] src = new byte[n];

    for (int i = 0; i < n; i++) src[i] = (byte) i;

    int compressedSize = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressedSize];
    QPLJob qplJob = new QPLJob(executionPath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setFlags(compressionFlags);
    qplJob.execute(src, 0, src.length, compressed, 0, compressedSize);

    int resultOffset = 5;
    byte[] result = new byte[resultOffset + n];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setFlags(decompressionFlags);
    int resultLength =
        qplJob.execute(
            compressed, 0, compressed.length, result, resultOffset, result.length - resultOffset);
    assertEquals(resultLength, n);
    assertArrayEquals(
        Arrays.copyOfRange(src, 0, src.length),
        Arrays.copyOfRange(result, resultOffset, resultOffset + resultLength));
  }

  @Test
  public void testCompressLargeOffset() {
    int n = 1000000;
    int offset = 999999;
    byte[] src = new byte[n];
    RANDOM.nextBytes(src);

    int compressedSize = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressedSize];
    QPLJob qplJob = new QPLJob(executionPath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setFlags(compressionFlags);
    qplJob.execute(src, offset, src.length - offset, compressed, 0, compressedSize);

    byte[] result = new byte[n - offset];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressed, 0, compressed.length, result, 0, result.length);
    byte[] srcRang = Arrays.copyOfRange(src, offset, n);
    assertArrayEquals(srcRang, result);
  }

  @Test
  public void testCompressBigLength() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          int n = 10000000;
          int offset = 9999999;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(executionPath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(src, offset, src.length, compressed, 0, compressedSize);
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testCompressEmptyDBB(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          byte[] data = new byte[0];
          int n = data.length;
          int offset = 2;
          ByteBuffer src = ByteBuffer.allocateDirect(n + offset);
          src.position(offset);
          src.put(data, 0, n);
          src.flip().position(offset);

          int outOffset = 3;
          int compressedLength = QPLJob.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedLength);
          QPLJob qplJob = new QPLJob(ePath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          compressedBB.position(outOffset);
          qplJob.execute(src, compressedBB);
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testCompressReleasedQPLResource(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalStateException.class,
        () -> {
          int n = 66560;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(ePath);
          qplJob.doClear();
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(src, 0, src.length, compressed, 0, compressedSize);
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testDecompressClosedQPLJob(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalStateException.class,
        () -> {
          int n = 100;
          byte[] srcData = new byte[n];

          for (int i = 0; i < n; i++) srcData[i] = (byte) i;

          ByteBuffer srcBB = ByteBuffer.allocate(n);
          srcBB.put(srcData, 0, n);
          srcBB.flip();
          int compressedSize = QPLJob.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
          QPLJob qplJob = new QPLJob(ePath);
          qplJob.doClear();
          executeCompress(qplJob, srcBB, compressedBB, 1, 1);
          compressedBB.flip();
          srcBB.flip();

          ByteBuffer resultBB = ByteBuffer.allocate(n);
          executeDecompress(qplJob, compressedBB, resultBB, 1);
          resultBB.flip();
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testReleaseResourcesClosedQPLJob(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalStateException.class,
        () -> {
          int n = 100;
          byte[] srcData = new byte[n];

          for (int i = 0; i < n; i++) srcData[i] = (byte) i;

          ByteBuffer srcBB = ByteBuffer.allocate(n);
          srcBB.put(srcData, 0, n);
          srcBB.flip();
          int compressedSize = QPLJob.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
          QPLJob qplJob = new QPLJob(ePath);
          qplJob.doClear();
          qplJob.doClear();
          executeCompress(qplJob, srcBB, compressedBB, 1, 1);
        });
  }

  @Test
  public void testCompressEmptyBB() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          int n = 66560;
          byte[] emptySrcArray = new byte[0];
          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(executionPath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(emptySrcArray, compressed);
        });
  }

  @Test
  public void testCompressWithNegativeOffset() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          int n = 65536;
          int offset = -2147483648;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(executionPath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(src, offset, src.length, compressed, 0, compressedSize);
        });
  }

  @Test
  public void testCompressIntegerOverflow() {
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          int n = 65536;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLJob.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLJob qplJob = new QPLJob(executionPath);
          qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
          qplJob.setFlags(compressionFlags);
          qplJob.execute(src, 2147483647, src.length, compressed, 0, compressedSize);
        });
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testCompressionWithByteBuffer(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {
      byte[] src = readAllBytes(FILE_PATH);
      byte[] dec = new byte[src.length];

      ByteBuffer srcBuf = ByteBuffer.allocateDirect(src.length);
      ByteBuffer comBuf = ByteBuffer.allocateDirect(QPLJob.maxCompressedLength(src.length));
      ByteBuffer decBuf = ByteBuffer.allocateDirect(src.length);

      srcBuf.put(src);
      srcBuf.flip();

      QPLJob qplJob = new QPLJob(ePath);
      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
      qplJob.setFlags(compressionFlags);
      qplJob.setCompressionLevel(cl);
      qplJob.setRetryCount(rt);
      int compressedSize = qplJob.execute(srcBuf, comBuf);
      comBuf.flip();

      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
      qplJob.setFlags(decompressionFlags);
      qplJob.setRetryCount(rt);
      int decompressedSize = qplJob.execute(comBuf, decBuf);
      decBuf.flip();
      decBuf.get(dec, 0, decompressedSize);

      assertTrue(compressedSize > 0);
      assertEquals(decompressedSize, src.length);
      assertArrayEquals(src, dec);
    } catch (QPLException | IOException | IllegalArgumentException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("paramsWithLargeLength")
  public void testCompressSWDecompressHW(int cl, int rt, int n, byte[] src) {
    assumeFalse(shouldSkip(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE));
    ByteBuffer srcBB = getSourceBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJobSw = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
    executeCompress(qplJobSw, srcBB, compressedBB, cl, rt);

    compressedBB.flip();
    srcBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    QPLJob qplJobHw = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
    executeDecompress(qplJobHw, compressedBB, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("paramsWithLargeLength")
  public void testCompressHwDecompressSw(int cl, int rt, int n, byte[] src) {
    assumeFalse(shouldSkip(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE));
    ByteBuffer srcBB = getSourceBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJobSw = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
    executeCompress(qplJobSw, srcBB, compressedBB, cl, rt);
    compressedBB.flip();
    srcBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    QPLJob qplJobHw = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
    executeDecompress(qplJobHw, compressedBB, resultBB, rt);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("paramsWithDifferentChunk")
  public void testDifferetChunkCDBB(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, int chunk, byte[] src) {
    System.setProperty("idxd.wqMaxTransferBytes", String.valueOf(chunk));
    try {
      Class<?> c = Class.forName("com.intel.qpl.QPLJob");
      Method method = c.getDeclaredMethod("loadConfig");
      method.setAccessible(true);
      method.invoke(null);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, srcBB, compressedBB, cl, rt);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n);
    executeDecompress(qplJob, compressedBB, resultBB, rt);
    resultBB.flip();
    srcBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("paramsWithDifferentChunk")
  public void testDifferentChunkBBVaryingOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, int chunk, byte[] data) {
    System.setProperty("idxd.wqMaxTransferBytes", String.valueOf(chunk));
    try {
      Class<?> c = Class.forName("com.intel.qpl.QPLJob");
      Method method = c.getDeclaredMethod("loadConfig");
      method.setAccessible(true);
      method.invoke(null);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocate(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocate(outOffset + QPLJob.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLJob qplJob = new QPLJob(ePath);
    executeCompress(qplJob, src, compressed, cl, rt);

    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocate(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);
    executeDecompress(qplJob, compressed, result, rt);

    assertEquals(outOffset + remaining, compressed.position());
    assertEquals(outOffset + remaining, compressed.limit());

    int decompressed = result.position() - inOffset;
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i)
      assert data[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testCompressDecompressByteArray(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    int compressLength = QPLJob.maxCompressedLength(n);

    byte[] compressed = new byte[compressLength];
    QPLJob qplJob = new QPLJob(ePath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setFlags(compressionFlags);
    qplJob.setCompressionLevel(cl);
    qplJob.setRetryCount(rt);
    int compressedBytes = qplJob.execute(src, compressed);

    byte[] result = new byte[n];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setFlags(decompressionFlags);
    qplJob.setRetryCount(rt);
    qplJob.execute(compressed, 0, compressedBytes, result, 0, result.length);
    assertArrayEquals(src, result);
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testCompressionWithByteArray(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {

      byte[] src = readAllBytes(FILE_PATH);
      byte[] dst = new byte[QPLJob.maxCompressedLength(src.length)];
      byte[] dec = new byte[src.length];

      QPLJob qplJob = new QPLJob(ePath);
      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
      qplJob.setFlags(compressionFlags);
      qplJob.setCompressionLevel(cl);
      qplJob.setRetryCount(rt);
      int compressedSize = qplJob.execute(src, dst);

      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
      qplJob.setFlags(decompressionFlags);
      qplJob.setRetryCount(rt);
      int decompressedSize = qplJob.execute(dst, dec);

      assertTrue(compressedSize > 0);
      assertEquals(decompressedSize, src.length);
      assertArrayEquals(src, dec);
    } catch (QPLException | IOException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testWrongIdxdConfiguration(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    int chunk = 1048576;
    System.setProperty("idxd.wqMaxTransferBytes", String.valueOf(chunk));
    try {
      Class<?> c = Class.forName("com.intel.qpl.QPLJob");
      Method method = c.getDeclaredMethod("loadConfig");
      method.setAccessible(true);
      method.invoke(null);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | IllegalArgumentException e) {
      assertTrue(true);
    }
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testWrongIdxdConfiguration2(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    System.setProperty("idxd.wqMaxTransferBytes", "test");
    try {
      Class<?> c = Class.forName("com.intel.qpl.QPLJob");
      Method method = c.getDeclaredMethod("loadConfig");
      method.setAccessible(true);
      method.invoke(null);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException e) {
      assertTrue(true);
    }
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testCompressionWithByteArrayDiffOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {

      byte[] src = readAllBytes(FILE_PATH);
      byte[] dst = new byte[QPLJob.maxCompressedLength(src.length)];
      byte[] dec = new byte[src.length];

      QPLJob qplJob = new QPLJob(ePath);
      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
      qplJob.setFlags(compressionFlags);
      qplJob.setCompressionLevel(cl);
      qplJob.setRetryCount(rt);
      assertEquals(qplJob.getExecutionPathCode(), ePath.getExecutionPathCode());
      int compressedSize = qplJob.execute(src, 3, src.length - 3, dst, 0, dst.length);

      qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
      qplJob.setFlags(decompressionFlags);
      qplJob.setRetryCount(rt);
      int decompressedSize = qplJob.execute(dst, 0, compressedSize, dec, 3, dec.length - 3);

      assertTrue(compressedSize > 0);
      assertEquals(decompressedSize, src.length - 3);

      String str = new String(src, StandardCharsets.UTF_8);
      assertEquals(
          0, str.substring(3).compareTo(new String(dec, StandardCharsets.UTF_8).substring(3)));

    } catch (QPLException | IOException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBADecompressionOverflow(QPLUtils.ExecutionPaths ePath, int cl, int rt, int n) {
    byte[] src = getSrcArray(n);
    int compressLength = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressLength];

    QPLJob qplJob = new QPLJob(ePath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(cl);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(compressionFlags);
    int compressedBytes = qplJob.execute(src, compressed);

    byte[] result = new byte[n];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);

    qplJob.execute(compressed, 0, compressedBytes, result, 0, result.length / 2);
    if (qplJob.isOutputInsufficient()) {
      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(
          compressed,
          qplJob.getBytesRead(),
          compressedBytes - qplJob.getBytesRead(),
          result,
          qplJob.getBytesWritten(),
          (result.length - qplJob.getBytesWritten()));
    }
    assertArrayEquals(src, result);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBADecompressionOverflowLoop(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n) {
    byte[] src = getSrcArray(n);
    int compressLength = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressLength];

    QPLJob qplJob = new QPLJob(ePath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(cl);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(compressionFlags);
    int compressedBytes = qplJob.execute(src, compressed);

    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);
    int compressedOffset = 0;
    int decompressOffest = 0;
    byte[] mergedBytes = new byte[n];
    byte[] result = new byte[n / 2];
    qplJob.execute(
        compressed,
        compressedOffset,
        compressedBytes - compressedOffset,
        result,
        decompressOffest,
        result.length);
    while (qplJob.isOutputInsufficient()) {
      System.arraycopy(result, 0, mergedBytes, decompressOffest, qplJob.getBytesWritten());
      compressedOffset += qplJob.getBytesRead();
      decompressOffest += qplJob.getBytesWritten();
      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(
          compressed,
          compressedOffset,
          compressedBytes - compressedOffset,
          result,
          0,
          result.length);
    }
    System.arraycopy(result, 0, mergedBytes, decompressOffest, qplJob.getBytesWritten());
    assertArrayEquals(src, mergedBytes);
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testCleanerAPI(QPLUtils.ExecutionPaths ePath) throws IOException {
    assumeFalse(shouldSkip(ePath));
    byte[] src = readAllBytes(FILE_PATH);
    byte[] dst = new byte[QPLJob.maxCompressedLength(src.length)];
    byte[] dec = new byte[src.length];

    QPLJob qplJob = new QPLJob(ePath);
    WeakReference<QPLJob> weak = new WeakReference<>(qplJob);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setFlags(compressionFlags);
    qplJob.setCompressionLevel(1);
    qplJob.setRetryCount(1);
    int compressedSize = qplJob.execute(src, dst);

    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setFlags(decompressionFlags);
    qplJob.setRetryCount(1);
    int decompressedSize = qplJob.execute(dst, dec);

    assertTrue(compressedSize > 0);
    assertEquals(decompressedSize, src.length);
    assertArrayEquals(src, dec);
    qplJob = null;
    System.gc();
    assertNull(weak.get());
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testDecompressionOverflow(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        QPLOutputOverflowException.class,
        () -> {
          int n = 2097153;
          byte[] src = getSrcArray(n);
          ByteBuffer srcBB = getSourceDirectBB(src);
          int compressedSize = QPLJob.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);

          QPLJob qplJob = new QPLJob(ePath);
          executeCompress(qplJob, srcBB, compressedBB, 1, 1);
          compressedBB.flip();

          ByteBuffer resultBB = ByteBuffer.allocateDirect(n / 2);
          ByteBuffer tmpBB = ByteBuffer.allocateDirect(50);
          ByteBuffer mergeBB;

          executeDecompress(qplJob, compressedBB, resultBB, 1);
          while (qplJob.isOutputInsufficient()) {
            qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
            qplJob.execute(compressedBB, tmpBB);
          }
          int totalCapacity = resultBB.limit() + tmpBB.limit();
          mergeBB = ByteBuffer.allocateDirect(totalCapacity);
          int position = resultBB.position();
          resultBB.flip();
          mergeBB.put(resultBB);
          mergeBB.position(position);
          tmpBB.flip();
          mergeBB.put(tmpBB);

          srcBB.flip();
          mergeBB.flip();
          for (int i = 0; i < n; ++i)
            assert srcBB.get(i) == mergeBB.get(i) : "Failed comparison on index: " + i;
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testReset(QPLUtils.ExecutionPaths ePath) throws IOException {
    assumeFalse(shouldSkip(ePath));
    byte[] src = readAllBytes(FILE_PATH);
    byte[] dst = new byte[QPLJob.maxCompressedLength(src.length)];

    QPLJob qplJob = new QPLJob(ePath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setFlags(compressionFlags);
    qplJob.setCompressionLevel(1);
    qplJob.setRetryCount(1);
    int compressedSize = qplJob.execute(src, dst);
    assertTrue(compressedSize > 0);
    assertTrue(qplJob.getBytesRead() > 0);
    assertTrue(qplJob.getBytesWritten() > 0);
    assertFalse(qplJob.isOutputInsufficient());
    assertEquals(qplJob.getCompressionLevel(), 1);
    assertEquals(qplJob.getFlags(), compressionFlags);
    assertEquals(qplJob.getRetryCount(), 1);

    qplJob.reset();
    assertEquals(0, qplJob.getBytesRead());
    assertEquals(0, qplJob.getBytesWritten());
    assertFalse(qplJob.isOutputInsufficient());
    assertEquals(qplJob.getFlags(), 0);
  }

  private void executeCompress(
      QPLJob qplJob, ByteBuffer srcBB, ByteBuffer compressedBB, int cl, int rt) {
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(cl);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(compressionFlags);
    qplJob.execute(srcBB, compressedBB);
  }

  private void executeDecompress(
      QPLJob qplJob, ByteBuffer compressedBB, ByteBuffer resultBB, int rt) {
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(rt);
    qplJob.setFlags(decompressionFlags);
    qplJob.execute(compressedBB, resultBB);
  }
}
