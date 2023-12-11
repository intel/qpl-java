/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl.junit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.intel.qpl.QPLCompressor;
import com.intel.qpl.QPLException;
import com.intel.qpl.QPLOutputOverflowException;
import com.intel.qpl.QPLUtils;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class QPLCompressorTest {
  private static final Random RANDOM = new Random();

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

  public static boolean shouldSkip(QPLUtils.ExecutionPaths ePath) {
    return (ePath.equals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE)
            || ePath.equals(QPLUtils.ExecutionPaths.QPL_PATH_AUTO))
        && !QPLTestSuite.FORCE_HARDWARE;
  }

  @Test
  public void testConstructor1() {
    assumeFalse(shouldSkip(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE));
    QPLCompressor defaultJob = new QPLCompressor();
    assertEquals(defaultJob.getCompressionLevel(), 1);
    assertEquals(defaultJob.getRetryCount(), 0);
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testConstructor2(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    QPLCompressor job = new QPLCompressor(ePath, cl, rt);
    assertEquals(job.getCompressionLevel(), cl);
    assertEquals(job.getRetryCount(), rt);
  }

  @ParameterizedTest
  @MethodSource("provideUnsupportedCL")
  public void testUnsupportedCL(QPLUtils.ExecutionPaths ePath, int cl) {
    assertThrows(
        QPLException.class,
        () -> {
          int n = 100;
          ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
          srcBB.position(0);
          srcBB.put(getSrcArray(n), 0, n);
          srcBB.flip();
          int compressedSize = QPLCompressor.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
          QPLCompressor compressor = new QPLCompressor(ePath, cl, 0);
          int compressedBytes = compressor.compress(srcBB, compressedBB);
          assertTrue(compressedBytes > 0);
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testGetValidExecutionPath(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    QPLUtils.ExecutionPaths validExecutionPath = QPLCompressor.getValidExecutionPath(ePath);
    assertEquals(ePath.name(), validExecutionPath.name());
  }

  @Test
  public void testGetValidCompressionLevel() {
    int level = QPLUtils.DEFAULT_COMPRESSION_LEVEL;
    QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
    int validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(level, validLevel);

    level = -1;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

    level = 0;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

    level = 1;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(level, validLevel);

    level = 2;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

    level = 3;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(level, validLevel);

    level = 100;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

    level = 2147483647;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);

    level = -2147483647;
    validLevel = QPLCompressor.getValidCompressionLevel(executionPath, level);
    assertEquals(QPLUtils.DEFAULT_COMPRESSION_LEVEL, validLevel);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testDBBCompress(QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLCompressor.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(srcBB, compressedBB);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n);

    compressor.decompress(compressedBB, resultBB);
    resultBB.flip();
    srcBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBBCompress(QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    int compressedSize = QPLCompressor.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(srcBB, compressedBB);
    compressedBB.flip();
    srcBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    compressor.decompress(compressedBB, resultBB);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testSrcRODstDBBCompress(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceBB(src);
    ByteBuffer srcBB1 = srcBB.asReadOnlyBuffer();
    int compressedSize = QPLCompressor.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(srcBB1, compressedBB);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    compressor.decompress(compressedBB, resultBB);
    resultBB.flip();
    assertEquals(resultBB.compareTo(srcBB), 0);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBBDifferentOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocate(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocate(outOffset + QPLCompressor.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(src, compressed);

    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocate(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);
    compressor.decompress(compressed, result);
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
  public void testDBBDifferentOffset(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] data) {
    final int inOffset = 2;
    ByteBuffer src = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    src.position(inOffset);
    src.put(data, 0, n);
    src.flip().position(inOffset);

    int outOffset = 5;
    ByteBuffer compressed =
        ByteBuffer.allocateDirect(
            outOffset + QPLCompressor.maxCompressedLength(data.length) + outOffset);
    byte[] garbage = new byte[compressed.capacity()];
    RANDOM.nextBytes(garbage);
    compressed.put(garbage);
    compressed.position(outOffset).limit(compressed.capacity() - outOffset);

    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(src, compressed);

    assertEquals(inOffset + n, src.position());
    assertEquals(inOffset + n, src.limit());
    compressed.flip().position(outOffset);
    int remaining = compressed.remaining();

    ByteBuffer result = ByteBuffer.allocateDirect(inOffset + n + inOffset);
    result.position(inOffset).limit(result.capacity() - inOffset);

    compressor.decompress(compressed, result);
    assertEquals(outOffset + remaining, compressed.position());
    assertEquals(outOffset + remaining, compressed.limit());
    assertEquals(result.capacity() - inOffset, result.limit());

    int decompressed = result.position() - inOffset;
    assert decompressed == n : "Failed uncompressed size";
    for (int i = 0; i < n; ++i)
      assert data[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testBACompress(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {

      byte[] src = getSrcArray(4096);
      byte[] dst = new byte[QPLCompressor.maxCompressedLength(src.length)];

      QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
      int compressedSize = compressor.compress(src, dst);
      assertTrue(compressedSize > 0);
    } catch (QPLException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testBACompress2nd(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {

      byte[] src = getSrcArray(4096);
      byte[] dst = new byte[QPLCompressor.maxCompressedLength(src.length)];
      byte[] dec = new byte[src.length];

      QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
      int compressedSize = compressor.compress(src, 0, src.length, dst, 0, dst.length);
      int decompressedSize = compressor.decompress(dst, 0, compressedSize, dec, 0, dec.length);
      assertTrue(compressedSize > 0);
      assertEquals(decompressedSize, src.length);
      assertArrayEquals(src, dec);
    } catch (QPLException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("provideAllParams")
  public void testBACompressDiffOffset(QPLUtils.ExecutionPaths ePath, int cl, int rt) {
    try {

      byte[] src = getSrcArray(4096);
      byte[] dst = new byte[QPLCompressor.maxCompressedLength(src.length)];
      byte[] dec = new byte[src.length];

      QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
      int compressedSize = compressor.compress(src, 3, src.length - 3, dst, 0, dst.length);

      int decompressedSize = compressor.decompress(dst, 0, compressedSize, dec, 3, dec.length - 3);

      assertTrue(compressedSize > 0);
      assertEquals(decompressedSize, src.length - 3);

      String str = new String(src, StandardCharsets.UTF_8);
      assertEquals(
          0, str.substring(3).compareTo(new String(dec, StandardCharsets.UTF_8).substring(3)));

    } catch (QPLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testQPLJobCleanerAPI() {
    byte[] src = getSrcArray(4096);
    byte[] dst = new byte[QPLCompressor.maxCompressedLength(src.length)];
    byte[] dec = new byte[src.length];

    QPLCompressor compressor = new QPLCompressor(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE, 1, 0);
    WeakReference<QPLCompressor> weak = new WeakReference<>(compressor);
    int compressedSize = compressor.compress(src, dst);

    int decompressedSize = compressor.decompress(dst, dec);
    compressor = null;
    assertTrue(compressedSize > 0);
    assertEquals(decompressedSize, src.length);
    assertArrayEquals(src, dec);
    System.gc();
    assertNull(weak.get());
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testCompressReleasedResource(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalStateException.class,
        () -> {
          int n = 66560;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLCompressor.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLCompressor compressor = new QPLCompressor(ePath, 1, 0);
          compressor.doClear();
          compressor.compress(src, 0, src.length, compressed, 0, compressedSize);
        });
  }

  @ParameterizedTest
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testDeompressReleasedResource(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        IllegalStateException.class,
        () -> {
          int n = 66560;
          byte[] src = new byte[n];
          RANDOM.nextBytes(src);

          int compressedSize = QPLCompressor.maxCompressedLength(n);
          byte[] compressed = new byte[compressedSize];
          QPLCompressor compressor = new QPLCompressor(ePath, 1, 0);
          compressor.compress(src, 0, src.length, compressed, 0, compressedSize);
          byte[] result = new byte[n];
          compressor.doClear();
          compressor.decompress(compressed, result);
        });
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testBADecompressionOverflowLoop(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n) {
    byte[] src = getSrcArray(n);
    int compressLength = QPLCompressor.maxCompressedLength(n);
    byte[] compressed = new byte[compressLength];

    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    int compressedBytes = compressor.compress(src, compressed);

    int compressedOffset = 0;
    int decompressOffest = 0;
    byte[] mergedBytes = new byte[n];
    byte[] result = new byte[n / 2];
    compressor.decompress(
        compressed,
        compressedOffset,
        compressedBytes - compressedOffset,
        result,
        decompressOffest,
        result.length);
    while (compressor.isOutputInsufficient()) {
      System.arraycopy(result, 0, mergedBytes, decompressOffest, compressor.getBytesWritten());
      compressedOffset += compressor.getBytesRead();
      decompressOffest += compressor.getBytesWritten();
      compressor.decompress(
          compressed,
          compressedOffset,
          compressedBytes - compressedOffset,
          result,
          0,
          result.length);
    }
    System.arraycopy(result, 0, mergedBytes, decompressOffest, compressor.getBytesWritten());
    assertArrayEquals(src, mergedBytes);
  }

  @ParameterizedTest
  @MethodSource("provideParamsLength")
  public void testDBDecompressionOverflow(
      QPLUtils.ExecutionPaths ePath, int cl, int rt, int n, byte[] src) {
    ByteBuffer srcBB = getSourceDirectBB(src);
    int compressedSize = QPLCompressor.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);

    QPLCompressor compressor = new QPLCompressor(ePath, cl, rt);
    compressor.compress(srcBB, compressedBB);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n / 2);
    ByteBuffer tmpBB = ByteBuffer.allocateDirect(n);
    ByteBuffer mergeBB;
    compressor.decompress(compressedBB, resultBB);
    while (compressor.isOutputInsufficient()) {
      compressor.decompress(compressedBB, tmpBB);
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
  @EnumSource(QPLUtils.ExecutionPaths.class)
  public void testDecompressionOverflow(QPLUtils.ExecutionPaths ePath) {
    assumeFalse(shouldSkip(ePath));
    assertThrows(
        QPLOutputOverflowException.class,
        () -> {
          byte[] src = getSrcArray(2097153);
          int n = src.length;
          ByteBuffer srcBB = getSourceDirectBB(src);
          int compressedSize = QPLCompressor.maxCompressedLength(n);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);

          QPLCompressor compressor = new QPLCompressor(ePath, 1, 0);
          compressor.compress(srcBB, compressedBB);
          compressedBB.flip();

          ByteBuffer resultBB = ByteBuffer.allocateDirect(n / 2);
          ByteBuffer tmpBB = ByteBuffer.allocateDirect(50);
          ByteBuffer mergeBB;

          compressor.decompress(compressedBB, resultBB);
          while (compressor.isOutputInsufficient()) {
            compressor.decompress(compressedBB, tmpBB);
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
}
