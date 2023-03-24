/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_intel_qpl_QPLJNI */

#ifndef _Included_com_intel_qpl_QPLJNI
#define _Included_com_intel_qpl_QPLJNI
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initIDs
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initIDs
  (JNIEnv *, jclass);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    getQPLJobSize
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_getQPLJobSize
  (JNIEnv *, jclass, jint);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initQPLJob
 * Signature: (ILjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initQPLJob
  (JNIEnv *, jclass, jint, jobject);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    execute
 * Signature: (Lcom/intel/qpl/QPLJob;[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_execute
  (JNIEnv *, jclass, jobject, jbyteArray, jobject, jint, jint, jbyteArray, jobject, jint, jint);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    finish
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_finish
  (JNIEnv *, jclass, jobject);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    isExecutionPathAvailable
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isExecutionPathAvailable
  (JNIEnv *, jclass, jint);

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    isCompressionLevelSupported
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isCompressionLevelSupported
  (JNIEnv *, jclass, jint, jint);

#ifdef __cplusplus
}
#endif
#endif
