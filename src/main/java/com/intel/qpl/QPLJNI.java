/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl;

import java.nio.ByteBuffer;

/* JNI binding to the original c/c++ implementation of qpl library. */
class QPLJNI {

    static {
        // Try to load qpl-java (libqpl-java.so on Linux) from the java.library.path.
        Native.loadLibrary();
        initIDs();
    }

    private static native void initIDs();
    static native int getQPLJobSize(int exePathCode);
    static native void initQPLJob(int exePathCode, ByteBuffer jobBuffer);
    static native int execute(QPLJob job, byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] dstArray, ByteBuffer dstBuffer, int dstOff, int maxDestLen);
    static native void finish(ByteBuffer jobBuffer);
    static native int isExecutionPathAvailable(int exePathCode);
    static native int isCompressionLevelSupported(int exePathCode, int cl);
}
