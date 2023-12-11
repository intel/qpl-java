/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl.fuzzing;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.intel.qpl.QPLCompressor;
import com.intel.qpl.QPLException;
import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class FuzzerTest {
  private static final Random RANDOM = new Random();

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {

    try {
      QPLUtils.ExecutionPaths path =
          QPLJob.getValidExecutionPath(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
      boolean FORCE_HARDWARE =
          path != null && path.equals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);

      byte[] src = data.consumeBytes(Integer.MAX_VALUE);
      if (src.length <= 0) return;
      if (FORCE_HARDWARE) {
        CompressorConstants.executionPath = QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE;
        testByteArray(src);
        testCompressorBA(src);
        testCompressorBB(src);
        testByteArrayOverflow(src);
        testByteArrayLargerLength();
        testByteArrayOverflowLargerLength();
        testCompressDecompressBAWithParameters(src);
        testCompressDecompressBB(src);
        testCompressDecompressDBB(src);
        testCompressDecompressSrcBBDstDBB(src);
        testCompressDecompressSrcDBBDstBB(src);
        testCompressDecompressSrcRODstBB(src);
        testCompressDifferentCLAndRC(src);
        testDBBLargerLength();
        testDBBDecompressionOverflow();

        testBALargerLengthQPLCompressJob();
        testBAOverflowQPLCompressJob(src);
      }
      CompressorConstants.executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
      testByteArray(src);
      testCompressorBA(src);
      testCompressorBB(src);
      testBALargerLengthQPLCompressJob();
      testBAOverflowQPLCompressJob(src);
      testByteArrayOverflow(src);
      testByteArrayLargerLength();
      testByteArrayOverflowLargerLength();
      testCompressDecompressBAWithParameters(src);
      testCompressDecompressBB(src);
      testCompressDecompressDBB(src);
      testCompressDecompressSrcBBDstDBB(src);
      testCompressDecompressSrcDBBDstBB(src);
      testCompressDecompressSrcRODstBB(src);
      testCompressDifferentCLAndRC(src);
      testDBBLargerLength();
      testDBBDecompressionOverflow();
    } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ignore) {
    } catch (QPLException e) {
      final String clMessage = "Error occurred while executing job. Status code is - 87";
      if (!e.getMessage().equalsIgnoreCase(clMessage)) {
        throw e;
      }
    }
  }

  static void testByteArray(byte[] src) {
    int size = src.length;
    int compressLength = QPLJob.maxCompressedLength(size);
    byte[] dst = new byte[compressLength];

    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.setCompressionLevel(1);
    int compressedBytes = qplJob.execute(src, dst);
    byte[] result = new byte[size];
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(1);
    qplJob.execute(dst, 0, compressedBytes, result, 0, result.length);
    qplJob.doClear();
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testByteArrayOverflow(byte[] src) {
    int n = src.length;
    int compressLength = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressLength];

    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(1);
    qplJob.setRetryCount(1);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    int compressedBytes = qplJob.execute(src, compressed);

    byte[] result = new byte[n];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(1);
    qplJob.setFlags(CompressorConstants.decompressionFlags);

    qplJob.execute(compressed, 0, compressedBytes, result, 0, result.length / 2);
    if (qplJob.isOutputInsufficient()) {
      qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_LAST.getId());
      qplJob.execute(
          compressed,
          qplJob.getBytesRead(),
          compressedBytes - qplJob.getBytesRead(),
          result,
          qplJob.getBytesWritten(),
          result.length - qplJob.getBytesWritten());
    }
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testCompressDecompressBAWithParameters(byte[] src) {
    int srcOffset = RANDOM.nextInt(9) + 1;
    int srcLength = RANDOM.nextInt(200);

    int compressLength = QPLJob.maxCompressedLength(srcLength);

    byte[] dst = new byte[compressLength];
    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.setCompressionLevel(1);
    int compressedBytes = qplJob.execute(src, srcOffset, srcLength, dst, 0, dst.length);
    byte[] result = new byte[src.length];
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(0);
    qplJob.execute(dst, 0, compressedBytes, result, 0, result.length);
    qplJob.doClear();
    // Extract Sub src array based on srcOffset and srcLength
    byte[] srcRang = Arrays.copyOfRange(src, srcOffset, srcLength);
    byte[] resRang = Arrays.copyOfRange(result, 0, srcLength - srcOffset);
    assert Arrays.equals(srcRang, resRang) : "Source and decompressed bytes are not equal";
  }

  static void testByteArrayLargerLength() {
    int minLength = 1024 * 1024;
    int maxLength = Integer.MAX_VALUE - 2;
    int size = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    byte[] src = getSrcArray(size);

    int compressLength = QPLJob.maxCompressedLength(size);
    byte[] dst = new byte[compressLength];

    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.setCompressionLevel(1);
    int compressedBytes = qplJob.execute(src, dst);
    byte[] result = new byte[size];
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(1);
    qplJob.execute(dst, 0, compressedBytes, result, 0, result.length);
    qplJob.doClear();
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testByteArrayOverflowLargerLength() {
    int minLength = 1024 * 1024;
    int maxLength = Integer.MAX_VALUE - 2;
    int size = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    byte[] src = getSrcArray(size);
    int compressLength = QPLJob.maxCompressedLength(size);
    byte[] compressed = new byte[compressLength];

    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    qplJob.setCompressionLevel(1);
    qplJob.setRetryCount(1);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    int compressedBytes = qplJob.execute(src, compressed);

    byte[] result = new byte[size];
    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(1);
    qplJob.setFlags(CompressorConstants.decompressionFlags);

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
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testCompressDecompressBB(byte[] srcData) {
    int n = srcData.length;
    ByteBuffer srcBB = ByteBuffer.allocate(n);
    srcBB.put(srcData, 0, n);
    srcBB.flip();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.execute(srcBB, compressedBB);
    compressedBB.flip();
    srcBB.flip();
    ByteBuffer resultBB = ByteBuffer.allocate(n);
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(0);
    qplJob.execute(compressedBB, resultBB);
    resultBB.flip();
    qplJob.doClear();
    assert resultBB.compareTo(srcBB) == 0 : "Source and decompressed Bytebuffer is not equal";
  }

  static void testCompressDecompressDBB(byte[] srcData) {
    int n = srcData.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
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
  }

  static void testCompressDecompressSrcBBDstDBB(byte[] srcData) {
    int n = srcData.length;
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
  }

  static void testCompressDecompressSrcDBBDstBB(byte[] srcData) {
    int n = srcData.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
    srcBB.put(srcData, 0, n);
    srcBB.flip();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.execute(srcBB, compressedBB);
    compressedBB.flip();
    srcBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(0);
    qplJob.execute(compressedBB, resultBB);
    resultBB.flip();
    qplJob.doClear();
    assert resultBB.compareTo(srcBB) == 0 : "Source and decompressed Bytebuffer is not equal";
  }

  static void testCompressDecompressSrcRODstBB(byte[] srcData) {
    int n = srcData.length;
    ByteBuffer srcBB = ByteBuffer.allocate(n);
    srcBB.put(srcData, 0, n);
    srcBB.flip();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer srcBBRO = srcBB.asReadOnlyBuffer();
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.execute(srcBBRO, compressedBB);
    compressedBB.flip();
    srcBBRO.flip();

    ByteBuffer resultBB = ByteBuffer.allocate(n);
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(0);
    qplJob.execute(compressedBB, resultBB);
    resultBB.flip();
    qplJob.doClear();
    assert resultBB.compareTo(srcBB) == 0 : "Source and decompressed Bytebuffer is not equal";
  }

  static void testCompressDifferentCLAndRC(byte[] src) {
    int size = src.length;
    int cl = RANDOM.nextInt(9) + 1;
    int rt = RANDOM.nextInt(20);

    int compressLength = QPLJob.maxCompressedLength(size);
    byte[] dst = new byte[compressLength];

    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.setCompressionLevel(cl);
    qplJob.setRetryCount(rt);
    qplJob.execute(src, dst);
    byte[] result = new byte[size];
    qplJob.setOperationType(CompressorConstants.decompress);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
    qplJob.setRetryCount(rt);
    qplJob.execute(dst, result);
    qplJob.doClear();
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testDBBLargerLength() {
    int minLength = 1024 * 1024;
    int maxLength = Integer.MAX_VALUE - 2;
    int n = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    byte[] srcData = getSrcArray(n);
    ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
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
  }

  static void testDBBDecompressionOverflow() {
    int minLength = 1024 * 1024;
    int maxLength = Integer.MAX_VALUE - 2;
    int n = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    byte[] srcData = getSrcArray(n);
    ByteBuffer srcBB = ByteBuffer.allocateDirect(n);
    srcBB.put(srcData, 0, n);
    srcBB.flip();
    int compressedSize = QPLJob.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    QPLJob qplJob = new QPLJob(CompressorConstants.executionPath);
    qplJob.setOperationType(CompressorConstants.compress);
    qplJob.setFlags(CompressorConstants.compressionFlags);
    qplJob.execute(srcBB, compressedBB);
    compressedBB.flip();

    ByteBuffer resultBB = ByteBuffer.allocateDirect(n / 2);
    ByteBuffer tmpBB = ByteBuffer.allocateDirect(n);
    ByteBuffer mergeBB;

    qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    qplJob.setRetryCount(1);
    qplJob.setFlags(CompressorConstants.decompressionFlags);
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

  static void testBAOverflowQPLCompressJob(byte[] src) {
    int n = src.length;
    int compressLength = QPLJob.maxCompressedLength(n);
    byte[] compressed = new byte[compressLength];

    QPLCompressor compressor = new QPLCompressor(CompressorConstants.executionPath, 1, 0);
    int compressedBytes = compressor.compress(src, compressed);

    byte[] result = new byte[n];

    compressor.decompress(compressed, 0, compressedBytes, result, 0, result.length / 2);
    if (compressor.isOutputInsufficient()) {
      compressor.decompress(
          compressed,
          compressor.getBytesRead(),
          compressedBytes - compressor.getBytesRead(),
          result,
          compressor.getBytesWritten(),
          result.length - compressor.getBytesWritten());
    }
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testBALargerLengthQPLCompressJob() {
    int minLength = 1024 * 1024;
    int maxLength = Integer.MAX_VALUE - 2;
    int size = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    byte[] src = getSrcArray(size);

    int compressLength = QPLCompressor.maxCompressedLength(size);
    byte[] dst = new byte[compressLength];

    QPLCompressor compressor = new QPLCompressor(CompressorConstants.executionPath, 1, 0);
    int compressedBytes = compressor.compress(src, dst);
    byte[] result = new byte[size];

    compressor.decompress(dst, 0, compressedBytes, result, 0, result.length);
    compressor.doClear();
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  static void testCompressorBB(byte[] srcData) {
    int n = srcData.length;
    ByteBuffer srcBB = ByteBuffer.allocate(n);
    srcBB.put(srcData, 0, n);

    srcBB.flip();
    int compressedSize = QPLCompressor.maxCompressedLength(n);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    QPLCompressor compressor = new QPLCompressor(CompressorConstants.executionPath, 1, 0);
    compressor.compress(srcBB, compressedBB);
    compressedBB.flip();
    srcBB.flip();
    ByteBuffer resultBB = ByteBuffer.allocate(n);

    compressor.decompress(compressedBB, resultBB);
    resultBB.flip();
    compressor.doClear();
    assert resultBB.compareTo(srcBB) == 0 : "Source and decompressed Bytebuffer is not equal";
  }

  static void testCompressorBA(byte[] src) {
    int size = src.length;
    int compressLength = QPLCompressor.maxCompressedLength(size);
    byte[] dst = new byte[compressLength];

    QPLCompressor compressor = new QPLCompressor(CompressorConstants.executionPath, 1, 0);
    int compressedBytes = compressor.compress(src, dst);
    byte[] result = new byte[size];
    compressor.decompress(dst, 0, compressedBytes, result, 0, result.length);
    compressor.doClear();
    assert Arrays.equals(src, result) : "Source and decompressed bytes are not equal";
  }

  private static byte[] getSrcArray(int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) bytes[i] = (byte) 10;
    return bytes;
  }
}
