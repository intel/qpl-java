/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

/**
 * This class contains commonly used enums and constants for qpl operations.
 *
 * <p>DEFAULT_IDXD_WQ_MAX_TRANSFER_BYTES is the workqueue maximum transfer size (refer to <a
 * href="https://github.com/intel/idxd-config">idxd-config</a> documentation). Currently, the idxd
 * driver sets this limit at 2MB. This library expects workqueues to be configured with a 2MB
 * maximum transfer size. If the user provides source and destination buffer sizes surpassing this
 * limit, the library will process the buffers in chunks.
 *
 * <p>Note: In future, if the idxd driver permits an increase in the workqueue maximum transfer
 * size,users should also configure the following system property to ensure that the library
 * utilizes the set workqueue maximum transfer size.
 *
 * <p>Example:
 *
 * <pre>{@code
 * System.setProperty("idxd.wqMaxTransferBytes","4194304");
 * }</pre>
 *
 * This property may be removed or ignored in future releases
 */
public class QPLUtils {

  private QPLUtils() {}

  /**
   * This property determines whether operations are executed on the IAA hardware or emulated in
   * software. There are three [options]: hardware, software and auto.
   *
   * <p>Warning: The implementation of Auto Path is in progress.
   *
   * @see <a
   *     href="https://intel.github.io/qpl/documentation/introduction_docs/introduction.html#execution-paths">Intel
   *     QPL Execution paths</a>
   * @see <a
   *     href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_enums_and_structures.html?highlight=operation%20types#c.qpl_path_t">Execution
   *     path enum</a>
   */
  public enum ExecutionPaths {
    /**
     * Intel QPL automatically dispatches execution of the requested operations either to Intel IAA
     * or to the software library depending on internal heuristics.
     */
    QPL_PATH_AUTO(0x00000000),
    /** All hardware-supported functions are executed by Intel IAA. */
    QPL_PATH_HARDWARE(0x00000001),
    /** All supported functionalities are executed by the software library in the CPU. */
    QPL_PATH_SOFTWARE(0x00000002);
    private final int executionPathCode;

    ExecutionPaths(int code) {
      this.executionPathCode = code;
    }

    /**
     * Returns execution path code.
     *
     * @return execution path code.
     */
    public int getExecutionPathCode() {
      return executionPathCode;
    }
  }

  /**
   * This library support several operation types.
   *
   * @see <a
   *     href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_enums_and_structures.html?highlight=qpl_operation#c.qpl_operation">Intel
   *     QPL operation types</a>
   */
  public enum Operations {
    /** Performs Inflate operation. */
    QPL_OP_DECOMPRESS(0x00),
    /** Performs Deflate operation. */
    QPL_OP_COMPRESS(0x01);
    private final int operationCode;

    Operations(final int value) {
      operationCode = value;
    }

    /**
     * Returns Operation type code.
     *
     * @return Operation type code.
     */
    public int getOperationCode() {
      return operationCode;
    }
  }

  /**
   * This library supports several operation flags.
   *
   * @see <a
   *     href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_common_definitions.html#flags">Intel
   *     QPL flags</a>
   */
  public enum Flags {
    /** The start of an entire new task. */
    QPL_FLAG_FIRST(0x0001),
    /** The end of an entire task. */
    QPL_FLAG_LAST(0x0002),
    /** The data compressed as a single dynamic DEFLATE block. */
    QPL_FLAG_DYNAMIC_HUFFMAN(0x2000),
    /** Turn off verification. */
    QPL_FLAG_OMIT_VERIFY(0x4000);
    private final int id;

    Flags(int flag) {
      id = flag;
    }

    /**
     * Returns Operation flag id.
     *
     * @return Returns Operation flag id.
     */
    public int getId() {
      return id;
    }
  }

  /** The default compression level is set to 1. */
  public static final int DEFAULT_COMPRESSION_LEVEL = 1;

  static final int QPL_SUCCESS_STATUS = 0;

  static final String QPL_JOB_INVALID = "QPLJob is invalid.";

  /** The idxd driver default workqueue(wq) max transfer size. */
  static final int DEFAULT_IDXD_WQ_MAX_TRANSFER_BYTES = 2 * 1024 * 1024;

  static final String IDXD_WQ_MAX_TRANSFER_MESSAGE =
      "The IDXD_WQ_MAX_TRANSFER_BYTES must be >= 2MB";

  /**
   * Represents number of times QPLJob attempts to acquire hardware resources. A default value
   * <code>0</code> means no retries will be attempted after a failure.
   */
  public static final int DEFAULT_RETRY_COUNT = 0;

  static final int CompressionFlags =
      QPLUtils.Flags.QPL_FLAG_FIRST.getId()
          | QPLUtils.Flags.QPL_FLAG_LAST.getId()
          | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId()
          | QPLUtils.Flags.QPL_FLAG_OMIT_VERIFY.getId();
  static final int DecompressionFlags =
      QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId();

  static void validateByteArray(byte[] src, int offset, int length) {
    if (offset < 0 || offset >= src.length) throw new ArrayIndexOutOfBoundsException(offset);

    int range = offset + length - 1;
    if (range < 0 || range >= src.length) throw new ArrayIndexOutOfBoundsException(range);
  }

  static void checkReadOnly(ByteBuffer bb) {
    if (bb.isReadOnly()) {
      throw new ReadOnlyBufferException();
    }
  }
}
