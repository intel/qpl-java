/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

#include "com_intel_qpl_QPLJNI.h"
#include "qpl/qpl.h"
#include "util.h"
#include <memory>

// Error messages
#define MEMORY_ALLOCATION_ERROR "memory allocation error"
#define QPL_INIT_JOB_ERROR "An error occurred during job initialization."
#define QPL_GET_JOB_SIZE_ERROR "An error occurred while getting job size."
#define QPL_EXECUTE_JOB_ERROR "Error occurred while executing job ."
#define QPL_FINI_JOB_ERROR "An error acquired during job finalization."
#define QPL_OPERATION_ERR "Non-supported value in the qpl_job operation field"
#define INPUT_INVALID "Input byteArray or buffer is invalid"
#define OUTPUT_INVALID "Output byteArray or buffer is invalid"

// Global caching
jclass qplJob_class;
jfieldID compressionLevel_id;
jfieldID retryCount_id;
jfieldID jobBuffer_id;
jfieldID operationType_id;
jfieldID flags_id;

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initIDs
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initIDs(JNIEnv *env, jclass clazz) {
  qplJob_class = env->FindClass("com/intel/qpl/QPLJob");
  compressionLevel_id = env->GetFieldID(qplJob_class, "compressionLevel", "I");
  retryCount_id = env->GetFieldID(qplJob_class, "retryCount", "I");
  jobBuffer_id = env->GetFieldID(qplJob_class, "jobBuffer", "Ljava/nio/ByteBuffer;");
  operationType_id = env->GetFieldID(qplJob_class, "operationType", "I");
  flags_id = env->GetFieldID(qplJob_class, "flags", "I");
  }

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    getQPLJobSize
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_getQPLJobSize(JNIEnv *env, jclass clazz, jint exePathCode) {
  qpl_path_t ePath = static_cast<qpl_path_t>(exePathCode);
  uint32_t size = 0;
  qpl_status status = qpl_get_job_size(ePath, &size);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_GET_JOB_SIZE_ERROR, status);
  }
  return (int)size;
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initQPLJob
 * Signature: (ILjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initQPLJob (JNIEnv *env, jclass clazz, jint exePathCode, jobject buffer) {
  qpl_path_t ePath = static_cast<qpl_path_t>(exePathCode);
  uint8_t *jobBuffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  qpl_job *job = reinterpret_cast<qpl_job *>(jobBuffer);

  qpl_status status = qpl_init_job(ePath, job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_INIT_JOB_ERROR, status);
  }
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    execute
 * Signature: (Lcom/intel/qpl/QPLJob;[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_execute(JNIEnv *env, jclass clazz, jobject javaJob, jbyteArray inputArr, jobject inputBuffer, jint inputStart, jint inputSize, jbyteArray outputArr, jobject outputBuffer, jint outputStart, jint outputMaxLength) {
  uint8_t *pInput;
  uint8_t *pOutput;
  jboolean isCopySrc, isCopyDest;

  if (inputArr != NULL) {
    pInput = reinterpret_cast<uint8_t *>(env->GetByteArrayElements(inputArr, &isCopySrc));
  } else {
    pInput = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(inputBuffer));
  }

  if (pInput == nullptr) {
    throw_exception(env, INPUT_INVALID);
    return 0;
  }

  if (outputArr != NULL) {
    pOutput = reinterpret_cast<uint8_t *>(env->GetByteArrayElements(outputArr, &isCopyDest));
  } else {
    pOutput = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(outputBuffer));
  }

  if (pOutput == nullptr) {
    throw_exception(env, OUTPUT_INVALID);
    return 0;
  }

  jobject buffValue = env->GetObjectField(javaJob, jobBuffer_id);
  jint operationValue = env->GetIntField(javaJob, operationType_id);
  jint rt = env->GetIntField(javaJob, retryCount_id);
  jint flags_value = env->GetIntField(javaJob, flags_id);

  qpl_operation operationType = static_cast<qpl_operation>(operationValue);
  uint8_t *jobBuffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffValue));
  qpl_job *job = reinterpret_cast<qpl_job *>(jobBuffer);
  qpl_status status;
  // Performing an operation
  job->next_in_ptr = pInput + inputStart;
  job->available_in = inputSize;
  job->next_out_ptr = pOutput + outputStart;
  job->available_out = outputMaxLength;

  switch (operationType) {
  case qpl_op_decompress: {
    job->op = qpl_op_decompress;
    job->flags = flags_value;
    break;
  }
  case qpl_op_compress: {
    // Returns the field ID for an compressionLevel instance variable of a QPLJob class.
    jint clValue = env->GetIntField(javaJob, compressionLevel_id);
    job->op = qpl_op_compress;
    job->level = static_cast<qpl_compression_levels>(clValue);
    job->flags = flags_value;
    break;
  }
  default: {
    throw_exception(env, QPL_OPERATION_ERR);
    return 0;
  }
  }
  // if queues are busy then retry the task execution until operation count reaches its retryCount.
  int tmpRetry = rt;
  do {
    status = qpl_execute_job(job);
    tmpRetry--;
  } while (status == QPL_STS_QUEUES_ARE_BUSY_ERR && tmpRetry > 0);

  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_EXECUTE_JOB_ERROR, status);
  }

  const uint32_t result = job->total_out;

  if (inputArr != NULL) {
    env->ReleaseByteArrayElements(inputArr, reinterpret_cast<jbyte *>(pInput), 0);
  }
  if (outputArr != NULL) {
    env->ReleaseByteArrayElements(outputArr, reinterpret_cast<jbyte *>(pOutput), 0);
  }
  return result;
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    finish
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_finish(JNIEnv *env, jclass clazz, jobject buffer) {
  uint8_t *jobBuffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  qpl_job *job = reinterpret_cast<qpl_job *>(jobBuffer);
  // Freeing resources
  qpl_status status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_FINI_JOB_ERROR, status);
  }
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    isExecutionPathAvailable
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isExecutionPathAvailable(JNIEnv *env, jclass clazz, jint exePathCode) {

  qpl_path_t execution_path = static_cast<qpl_path_t>(exePathCode);
  qpl_status status;
  uint32_t size = 0;
  std::unique_ptr<uint8_t[]> job_buffer;
  qpl_job *job;

  // Job initialization
  status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    return status;
  }
  try {
    job_buffer = std::make_unique<uint8_t[]>(size);
  }
  catch (std::bad_alloc &e) {
    throw_exception(env, e.what());
  }

  job = reinterpret_cast<qpl_job *>(job_buffer.get());
  status = qpl_init_job(execution_path, job);
  if (status != QPL_STS_OK) {
    return status;
  }
  // Freeing resources
  status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_FINI_JOB_ERROR, status);
  }
  return status;
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    isCompressionLevelSupported
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isCompressionLevelSupported(JNIEnv *env, jclass clazz, jint exePathCode, jint cl) {
  const uint8_t source_size = 40;
  uint8_t source[source_size];
  uint8_t destination[source_size + 5];

  for (int i = 0; i < source_size; i++)
    source[i] = i;

  qpl_path_t execution_path = static_cast<qpl_path_t>(exePathCode);
  qpl_compression_levels level = static_cast<qpl_compression_levels>(cl);

  qpl_status status;
  uint32_t size = 0;
  std::unique_ptr<uint8_t[]> job_buffer;
  qpl_job *job;

  // Job initialization
  status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_GET_JOB_SIZE_ERROR, status);
  }
  try {
    job_buffer = std::make_unique<uint8_t[]>(size);
  }
  catch (std::bad_alloc &e) {
    throw_exception(env, e.what());
  }

  job = reinterpret_cast<qpl_job *>(job_buffer.get());
  status = qpl_init_job(execution_path, job);

  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_INIT_JOB_ERROR, status);
  }

  job->op = qpl_op_compress;
  job->level = level;
  job->next_in_ptr =  source;
  job->available_in = source_size;
  job->next_out_ptr = destination;
  job->available_out = source_size * 5;
  job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_CHECKSUMS;

  // Compression
  status = qpl_execute_job(job);
  if (status != QPL_STS_OK) {
    return status;
  }
  // Freeing resources
  status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_FINI_JOB_ERROR, status);
  }
  return status;
}
