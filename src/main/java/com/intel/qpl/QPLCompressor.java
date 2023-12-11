/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

/**
 * Provides methods to compress and decompress data.
 *
 * <p>This class is not thread safe.
 */
public class QPLCompressor {
  private final QPLJob job;
  /**
   * Creates a new QPLCompressor that uses {@link QPLUtils.ExecutionPaths#QPL_PATH_HARDWARE}, {@link
   * QPLUtils#DEFAULT_COMPRESSION_LEVEL}, {@link QPLUtils#DEFAULT_RETRY_COUNT}.
   */
  public QPLCompressor() {
    this(
        QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE,
        QPLUtils.DEFAULT_COMPRESSION_LEVEL,
        QPLUtils.DEFAULT_RETRY_COUNT);
  }

  /**
   * Creates a new QPLCompressor with specified parameters.
   *
   * @param executionPath the execution path {@link QPLUtils.ExecutionPaths}
   * @param compressionLevel the compression level.
   * @param retryCount the number of attempts to acquire hardware resources.
   */
  public QPLCompressor(
      QPLUtils.ExecutionPaths executionPath, int compressionLevel, int retryCount) {
    job = new QPLJob(executionPath);
    job.setCompressionLevel(compressionLevel);
    job.setRetryCount(retryCount);
  }

  /**
   * Returns the maximum compression length for the specified source length. Use this method to
   * estimate the size of a buffer for compression given the size of a source buffer.
   *
   * @param srcLen the length of the source array or buffer.
   * @return the maximum compression length for the specified length
   * @throws IllegalArgumentException if the Source length is less than one or too large.
   */
  public static int maxCompressedLength(int srcLen) {
    return QPLJob.maxCompressedLength(srcLen);
  }

  /**
   * Validates and returns valid execution path.
   *
   * <p>while validating, if execution path is not available then it is set to QPL_PATH_SOFTWARE
   * path.
   *
   * @param executionPath execution path.
   * @return valid execution path.
   */
  public static QPLUtils.ExecutionPaths getValidExecutionPath(
      QPLUtils.ExecutionPaths executionPath) {
    return QPLJob.getValidExecutionPath(executionPath);
  }

  /**
   * Validates and returns valid compression level.
   *
   * <p>while validating, if compression level is not supported then it is set to default level 1.
   *
   * @param executionPath execution path.
   * @param compressionLevel compression level.
   * @return valid compression level.
   */
  public static int getValidCompressionLevel(
      QPLUtils.ExecutionPaths executionPath, int compressionLevel) {
    return QPLJob.getValidCompressionLevel(executionPath, compressionLevel);
  }

  /**
   * Compresses the source buffer and stores the result in the destination buffer. Returns actual
   * number of bytes of compressed data.
   *
   * <p>The positions of both the source and destinations buffers are advanced by the number of
   * bytes read from the source and the number of bytes of compressed data written to the
   * destination.
   *
   * @param src the source buffer holding the source data
   * @param dst the destination buffer that will store the compressed data
   * @return returns the size of the compressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     bytes.
   */
  public int compress(ByteBuffer src, ByteBuffer dst) {
    job.reset();
    job.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    job.setFlags(QPLUtils.CompressionFlags);
    return job.execute(src, dst);
  }

  /**
   * Compresses the source array and stores the result in the destination array. Returns the actual
   * number of bytes of the compressed data.
   *
   * @param src the source array holding the source data
   * @param dst the destination array for the compressed data
   * @return the size of the compressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     bytes.
   */
  public int compress(byte[] src, byte[] dst) {
    return compress(src, 0, src.length, dst, 0, dst.length);
  }

  /**
   * Compresses the source array, starting at the specified offset, and stores the result in the
   * destination array starting at the specified destination offset. Returns the actual number of
   * bytes of data compressed.
   *
   * @param src the source array holding the source data
   * @param srcOffset the start offset of the source data
   * @param srcLength the length of source data to compress
   * @param dst the destination array for the compressed data
   * @param dstOffset the destination offset where to start storing the compressed data
   * @param dstLength the maximum length that can be written to the destination array
   * @return the size of the compressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     bytes.
   */
  public int compress(
      byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
    job.reset();
    job.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
    job.setFlags(QPLUtils.CompressionFlags);
    return job.execute(src, srcOffset, srcLength, dst, dstOffset, dstLength);
  }

  /**
   * Decompresses the source buffer and stores the result in the destination buffer. Returns actual
   * number of bytes of decompressed data.
   *
   * <p>The positions of both the source and destinations buffers are advanced by the number of
   * bytes read from the source and the number of bytes of decompressed data written to the
   * destination.
   *
   * @param src the source buffer holding the compressed data
   * @param dst the destination buffer that will store the decompressed data
   * @return the size of the decompressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the
   *     decompressed bytes.
   */
  public int decompress(ByteBuffer src, ByteBuffer dst) {
    job.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    int decompressflags =
        job.isOutputInsufficient()
            ? QPLUtils.Flags.QPL_FLAG_LAST.getId()
            : QPLUtils.DecompressionFlags;
    job.setFlags(decompressflags);
    return job.execute(src, dst);
  }

  /**
   * Decompresses the source array and stores the result in the destination array. Returns the
   * actual number of bytes of the decompressed data.
   *
   * @param src the source array holding the compressed data
   * @param dst the destination array for the decompressed data
   * @return the size of the decompressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the
   *     decompressed bytes.
   */
  public int decompress(byte[] src, byte[] dst) {
    return decompress(src, 0, src.length, dst, 0, dst.length);
  }

  /**
   * Decompresses the source array, starting at the specified offset, and stores the result in the
   * destination array starting at the specified destination offset. Returns the actual number of
   * bytes of data decompressed.
   *
   * @param src the source array holding the compressed data
   * @param srcOffset the start offset of the source
   * @param srcLength the length of source data to decompress
   * @param dst the destination array for the decompressed data
   * @param dstOffset the destination offset where to start storing the decompressed data
   * @param dstLength the length that can be written to the destination array
   * @return the size of the decompressed data in bytes
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if the QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the
   *     decompressed bytes.
   */
  public int decompress(
      byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
    job.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
    int decompressflags =
        job.isOutputInsufficient()
            ? QPLUtils.Flags.QPL_FLAG_LAST.getId()
            : QPLUtils.DecompressionFlags;
    job.setFlags(decompressflags);
    return job.execute(src, srcOffset, srcLength, dst, dstOffset, dstLength);
  }

  /**
   * Returns configured retry count.
   *
   * @return retry count.
   */
  public int getRetryCount() {
    return job.getRetryCount();
  }

  /**
   * Returns configured compression level.
   *
   * @return compression level.
   */
  public int getCompressionLevel() {
    return job.getCompressionLevel();
  }

  /**
   * Checks if the destination buffer provided for the decompressed data was insufficient to
   * accommodate the entire decompressed data.
   *
   * <p>Call this method after invoking decompress method, especially if the exact destination size
   * is unknown during decompression.
   *
   * @return true if the destination size of the preceding decompress operation was insufficient ,
   *     otherwise false.
   */
  public boolean isOutputInsufficient() {
    return job.isOutputInsufficient();
  }

  /**
   * Returns bytes read from the source in the preceding operation.
   *
   * @return bytes read from the source in the preceding operation.
   */
  public int getBytesRead() {
    return job.getBytesRead();
  }

  /**
   * Returns bytes written to the destination in the preceding operation.
   *
   * @return bytes written to the destination in the preceding operation.
   */
  public int getBytesWritten() {
    return job.getBytesWritten();
  }

  /**
   * Releases resources held by this QPLCompressor. Resources held by this object are automatically
   * released on garbage collection. This method can be used to do this explicitly; consequently
   * this QPLCompressor will no longer be valid for use.
   *
   * @throws IllegalStateException If the user attempts to use this object after releasing the
   *     resource explicitly.
   */
  public void doClear() {
    job.doClear();
  }
}
