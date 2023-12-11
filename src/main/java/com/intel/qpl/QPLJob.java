/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

/**
 * Defines general qpl wrapper functions.
 *
 * <p>This class is not thread safe.
 */
public class QPLJob {
  private int executionPathCode;
  private int compressionLevel = 1;
  private int retryCount = 0;
  private final ByteBuffer jobBuffer;
  private int operationType = 0;
  private int flags = 0;
  private boolean isJobValid = true;
  static int IDXD_WQ_MAX_TRANSFER_BYTES;
  private int bytesRead = 0;
  private int bytesWritten = 0;
  private boolean outputInsufficient = false;
  /** Cleaner instance associated with this object. */
  private static Cleaner cleaner;

  /** Cleaner.Cleanable instance representing qpl cleanup action. */
  private final Cleaner.Cleanable cleanable;

  static {
    loadConfig();
  }

  /** Creates a new QPLJob using the default execution path 'QPL_PATH_HARDWARE'. */
  public QPLJob() {
    this(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
  }

  /**
   * Creates a new QPLJob using the specified execution path.
   *
   * @param executionPath execution path.
   */
  public QPLJob(QPLUtils.ExecutionPaths executionPath) {
    this.executionPathCode = executionPath.getExecutionPathCode();
    int size = QPLJNI.getQPLJobSize(this.executionPathCode);
    this.jobBuffer = ByteBuffer.allocateDirect(size);
    QPLJNI.initQPLJob(executionPath.getExecutionPathCode(), this.jobBuffer);
    cleanable = cleaner.register(this, new CleaningAction(jobBuffer));
  }

  /**
   * Returns the maximum compression length for the specified source length. Use this method to
   * estimate the size of a buffer for compression given the size of a source buffer.
   *
   * @param srcLen the length of the source array or buffer.
   * @return the maximum compression length for the specified length.
   * @throws IllegalArgumentException if the Source length is less than one or too large.
   */
  public static int maxCompressedLength(int srcLen) {
    // TODO: Modify this method once qpl provides API to return Maximum

    if (srcLen <= 0) {
      throw new IllegalArgumentException("Source length must be > 0, got " + srcLen);
    }
    int chunkLength = IDXD_WQ_MAX_TRANSFER_BYTES / 2;
    int chunks = (int) Math.ceil((double) srcLen / chunkLength);
    int dstLen;
    if (chunks <= 1) {
      dstLen = srcLen + (srcLen >> 12) + (srcLen >> 14) + (srcLen >> 25) + 13;
    } else {
      dstLen =
          (chunkLength + (chunkLength >> 12) + (chunkLength >> 14) + (chunkLength >> 25) + 13)
              * chunks;
    }

    if (dstLen <= 0) {
      throw new IllegalArgumentException("The source length is too large");
    }
    return dstLen;
  }

  /**
   * This method forms corresponding processing functions pipeline based on specified operation
   * type.
   *
   * <p>Intel® qpl-java library supports several operations @see {@link QPLUtils.Operations}. if
   * operation type is "compress" then it compresses bytes in 'src' and stores result into 'dst'.
   * Likewise based on different types, it executes different operations. upon return, the 'src'
   * buffer's position will be set to its limit; The 'dst' buffer's position will be advanced by n,
   * where n is the value returned by execute method;
   *
   * @param src the source buffer.
   * @param dst the destination buffer.
   * @return the number of bytes written into 'dst.
   * @throws ReadOnlyBufferException if the 'dst' is readonly.
   * @throws IllegalStateException if this QPLJob is invalid .
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     or decompressed bytes.
   */
  public int execute(ByteBuffer src, ByteBuffer dst) {
    if (!isJobValid) {
      throw new IllegalStateException(QPLUtils.QPL_JOB_INVALID);
    }
    QPLUtils.checkReadOnly(dst);
    int outputSize;
    try {
      outputSize = executeOperation(src, dst);
    } finally {
      advanceByteBuffer(src, dst);
    }
    return outputSize;
  }

  private int executeOperation(ByteBuffer src, ByteBuffer dst) {
    if ((src.hasArray() || src.isDirect()) && (dst.hasArray() || dst.isDirect())) {
      ByteBuffer srcBuf = null, dstBuf = null;
      byte[] srcArr = null, dstArr = null;
      int srcOffset = src.position();
      int dstOffset = dst.position();
      if (src.hasArray()) {
        srcArr = src.array();
        srcOffset += src.arrayOffset();
      } else {
        srcBuf = src;
      }
      if (dst.hasArray()) {
        dstArr = dst.array();
        dstOffset += dst.arrayOffset();
      } else {
        dstBuf = dst;
      }
      return QPLJNI.execute(
          this,
          srcArr,
          srcBuf,
          srcOffset,
          src.remaining(),
          dstArr,
          dstBuf,
          dstOffset,
          dst.remaining());
    } else {
      ByteBuffer srcBuf = null, dstBuf = null;
      byte[] dstArr = null;
      int srcOffset = src.position();
      int dstOffset = dst.position();
      byte[] srcArr = new byte[src.remaining()];
      src.get(srcArr);
      // since the src.get method advance the src position, flip the src to
      // its initial position.
      src.flip().position(srcOffset);

      if (dst.hasArray()) {
        dstArr = dst.array();
        dstOffset += dst.arrayOffset();
      } else {
        dstBuf = dst;
      }
      return QPLJNI.execute(
          this,
          srcArr,
          srcBuf,
          srcOffset,
          src.remaining(),
          dstArr,
          dstBuf,
          dstOffset,
          dst.remaining());
    }
  }

  private void advanceByteBuffer(ByteBuffer src, ByteBuffer dst) {
    src.position(src.position() + bytesRead);
    dst.position(dst.position() + bytesWritten);
  }

  /**
   * This method forms corresponding processing functions pipeline based on specified operation
   * type.
   *
   * <p>Intel® qpl-java library supports several operations @see {@link QPLUtils.Operations}. if
   * operation type is "compress" then it compresses bytes in 'src' and stores result into 'dst'.
   * Likewise based on different types, it executes different operations.
   *
   * @param src the source byte array.
   * @param dst the destination byte array.
   * @return the number of bytes written into 'dst'.
   * @throws IllegalArgumentException if 'src' and 'dst' length is less than 0.
   * @throws ArrayIndexOutOfBoundsException if 'src'/'dst' offset is less than 0 or, it's greater
   *     than 'src'/'dst' length.
   * @throws IllegalStateException if this QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     or decompressed bytes.
   */
  public int execute(byte[] src, byte[] dst) {
    return execute(src, 0, src.length, dst, 0, dst.length);
  }

  /**
   * This method forms corresponding processing functions pipeline based on specified operation
   * type.
   *
   * <p>Intel® qpl-java library supports several operations @see {@link QPLUtils.Operations}. if
   * operation type is "compress" then it Compresses 'srcLength' bytes from 'src' at 'srcOffset' and
   * stores the result into 'dst' at 'dstOffset'. Likewise based on different types, it executes
   * different operations.
   *
   * @param src the source byte array.
   * @param srcOffset source start offset.
   * @param srcLength source length.
   * @param dst the destination byte array.
   * @param dstOffset destination offset.
   * @param dstLength available space in the destination buffer after the offset.
   * @return the number of bytes written into 'dst'
   * @throws IllegalArgumentException if 'src' and 'dst' length is less than 0.
   * @throws ArrayIndexOutOfBoundsException if 'src'/'dst' offset is less than 0 or, it's greater
   *     than 'src'/'dst' length.
   * @throws IllegalStateException if this QPLJob is invalid.
   * @throws QPLOutputOverflowException if the dst is not large enough to accommodate the compressed
   *     or decompressed bytes.
   */
  public int execute(
      byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
    if (!isJobValid) {
      throw new IllegalStateException(QPLUtils.QPL_JOB_INVALID);
    }
    boolean isFirstBlock = (getFlags() & QPLUtils.Flags.QPL_FLAG_FIRST.getId()) != 0;
    if (isFirstBlock && srcLength <= 0) {
      throw new IllegalArgumentException("length must be > 0");
    }
    QPLUtils.validateByteArray(src, srcOffset, srcLength);
    QPLUtils.validateByteArray(dst, dstOffset, dstLength);
    return QPLJNI.execute(this, src, null, srcOffset, srcLength, dst, null, dstOffset, dstLength);
  }

  /**
   * Returns configured compression level.
   *
   * @return compression level.
   */
  public int getCompressionLevel() {
    return compressionLevel;
  }

  /**
   * Sets compression level. Default value is 1.
   *
   * @param compressionLevel compression level.
   */
  public void setCompressionLevel(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  /**
   * Returns configured retry count.
   *
   * @return retry count.
   */
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Sets retry count. Default value is 0.
   *
   * @param retryCount retry count.
   */
  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  /**
   * Returns configured operation type.
   *
   * <p>Default operation type is QPL_OP_DECOMPRESS.
   *
   * @return operation type.
   */
  public int getOperationType() {
    return operationType;
  }

  /**
   * Sets operation type. qpl-java library supports several operation types.
   *
   * @param operationType qpl operation type.
   * @see QPLUtils.Operations
   */
  public void setOperationType(QPLUtils.Operations operationType) {
    this.operationType = operationType.getOperationCode();
  }

  /**
   * Returns configured qpl operation flags.
   *
   * @return operation flags.
   */
  public int getFlags() {
    return flags;
  }

  /**
   * Sets auxiliary operation flags.
   *
   * @param flags operation flags
   * @see QPLUtils.Flags
   */
  public void setFlags(int flags) {
    this.flags = flags;
  }

  /**
   * Retuns configured execution path code.
   *
   * @return execution path code
   */
  public int getExecutionPathCode() {
    return executionPathCode;
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
    if (QPLUtils.ExecutionPaths.QPL_PATH_AUTO.equals(executionPath)
        || QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE.equals(executionPath)) {
      // Currently we don't have built in qpl api function to fetch the details
      // of IAA hardware availability, hence using the workaround to find the
      // hardware availability.
      // TODO: Remove the workaround once qpl api supports function to return
      // IAA availability .
      int status =
          QPLJNI.isExecutionPathAvailable(
              QPLUtils.ExecutionPaths.valueOf(executionPath.name()).getExecutionPathCode());
      return status == QPLUtils.QPL_SUCCESS_STATUS
          ? executionPath
          : QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
    }
    return QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
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
    // Its a workaround to check if the compression level is supported or not.
    int status =
        QPLJNI.isCompressionLevelSupported(
            QPLUtils.ExecutionPaths.valueOf(executionPath.name()).getExecutionPathCode(),
            compressionLevel);
    return status == QPLUtils.QPL_SUCCESS_STATUS
        ? compressionLevel
        : QPLUtils.DEFAULT_COMPRESSION_LEVEL;
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
    return outputInsufficient;
  }

  /** Resets the state of the object to its initial state. */
  public void reset() {
    this.outputInsufficient = false;
    this.bytesWritten = 0;
    this.bytesRead = 0;
    this.flags = 0;
    this.operationType = 0;
  }

  /**
   * Releases resources held by this QPLJob. Resources held by this object are automatically
   * released on garbage collection. This method can be used to do this explicitly; consequently
   * this QPLJob will no longer be valid for use.
   *
   * @throws IllegalStateException If the user attempts to use this object after releasing the
   *     resource explicitly.
   */
  public void doClear() {
    if (!isJobValid) {
      throw new IllegalStateException(QPLUtils.QPL_JOB_INVALID);
    }
    QPLJNI.finish(jobBuffer);
    isJobValid = false;
  }

  private static void doClear(ByteBuffer jobBuffer) {
    QPLJNI.finish(jobBuffer);
  }

  /**
   * Returns bytes read from the source in the preceding operation.
   *
   * @return bytes read from the source in the preceding operation.
   */
  public int getBytesRead() {
    return bytesRead;
  }

  /**
   * Returns bytes written to the destination in the preceding operation.
   *
   * @return bytes written to the destination in the preceding operation.
   */
  public int getBytesWritten() {
    return bytesWritten;
  }

  @SuppressWarnings({"deprecation", "removal"})
  private static void loadConfig() {

    java.security.AccessController.doPrivileged(
        new java.security.PrivilegedAction<Void>() {
          public Void run() {
            try {
              String chunkLengthValue = System.getProperty("idxd.wqMaxTransferBytes");
              if (chunkLengthValue != null && !chunkLengthValue.isEmpty()) {
                int value = Integer.parseInt(chunkLengthValue);
                if (value < QPLUtils.DEFAULT_IDXD_WQ_MAX_TRANSFER_BYTES) {
                  throw new IllegalArgumentException(QPLUtils.IDXD_WQ_MAX_TRANSFER_MESSAGE);
                }
                IDXD_WQ_MAX_TRANSFER_BYTES = value;

              } else {
                IDXD_WQ_MAX_TRANSFER_BYTES = QPLUtils.DEFAULT_IDXD_WQ_MAX_TRANSFER_BYTES;
              }
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException(QPLUtils.IDXD_WQ_MAX_TRANSFER_MESSAGE);
            }
            return null;
          }
        });
    QPLJNI.initValuesAndIDs(
        IDXD_WQ_MAX_TRANSFER_BYTES, QPLJob.maxCompressedLength(IDXD_WQ_MAX_TRANSFER_BYTES / 2));
    createCleaner();
  }

  private static void createCleaner() {
    SecurityManager sm = System.getSecurityManager();
    if (sm == null) {
      cleaner = Cleaner.create();
    } else {
      java.security.PrivilegedAction<Void> pa =
          () -> {
            cleaner = Cleaner.create();
            return null;
          };
      java.security.AccessController.doPrivileged(pa);
    }
  }

  private static class CleaningAction implements Runnable {
    private ByteBuffer jobBuffer;

    CleaningAction(ByteBuffer buff) {
      this.jobBuffer = buff;
    }

    @Override
    public void run() {
      if (jobBuffer != null) {
        doClear(jobBuffer);
        jobBuffer = null;
      }
    }
  }
}
