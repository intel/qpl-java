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
 */
public class QPLUtils {

    private QPLUtils() {

    }

    /**
     * This property determines whether operations are executed on the IAA hardware or emulated in software. There are three [options]: hardware, software and auto.
     *
     * @see <a href="https://intel.github.io/qpl/documentation/introduction_docs/introduction.html#execution-paths">Intel QPL Execution paths</a>
     * @see <a href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_enums_and_structures.html?highlight=operation%20types#c.qpl_path_t">Execution path enum</a>
     */
    public enum ExecutionPaths {
        /**
         * Intel QPL automatically dispatches execution of the requested operations either to Intel IAA or to the software library depending on internal heuristics.
         */
        QPL_PATH_AUTO(0x00000000),
        /**
         * All hardware-supported functions are executed by Intel IAA.
         */
        QPL_PATH_HARDWARE(0x00000001),
        /**
         * All supported functionalities are executed by the software library in the CPU.
         */
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
     * @see <a href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_enums_and_structures.html?highlight=qpl_operation#c.qpl_operation">Intel QPL operation types</a>
     */
    public enum Operations {
        /**
         * Performs Inflate operation.
         */
        QPL_OP_DECOMPRESS(0x00),
        /**
         * Performs Deflate operation.
         */
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
     * @see <a href="https://intel.github.io/qpl/documentation/dev_ref_docs/c_ref/c_common_definitions.html#flags">Intel QPL flags</a>
     */
    public enum Flags {
        /**
         * The start of an entire new task.
         */
        QPL_FLAG_FIRST(0x0001),
        /**
         * The end of an entire task.
         */
        QPL_FLAG_LAST(0x0002),
        /**
         * The data compressed as a single dynamic DEFLATE block.
         */
        QPL_FLAG_DYNAMIC_HUFFMAN(0x2000),
        /**
         * Turn off verification.
         */
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

    /**
     * The default compression level is set to 1.
     */
    public static final int DEFAULT_COMPRESSION_LEVEL = 1;

    static final int QPL_SUCCESS_STATUS = 0;

    static final String QPL_JOB_INVALID = "QPLJob is invalid.";

    static void validateByteArray(byte[] src, int offset, int length) {
        if (length < 0)
            throw new IllegalArgumentException("length must be >= 0");

        if (offset < 0 || offset >= src.length)
            throw new ArrayIndexOutOfBoundsException(offset);

        int range = offset + length - 1;
        if (range < 0 || range >= src.length)
            throw new ArrayIndexOutOfBoundsException(range);
    }

    static void checkReadOnly(ByteBuffer bb) {
        if (bb.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
    }
}
