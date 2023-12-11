/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

#include "com_intel_qpl_QPLJNI.h"

#include <memory>

#include "qpl/qpl.h"
#include "util.h"

// Error messages
static constexpr const char *MEMORY_ALLOCATION_ERROR =
    "memory allocation error";
static constexpr const char *QPL_INIT_JOB_ERROR =
    "An error occurred during job initialization";
static constexpr const char *QPL_GET_JOB_SIZE_ERROR =
    "An error occurred while getting job size";
static constexpr const char *QPL_EXECUTE_JOB_ERROR =
    "Error occurred while executing job";
static constexpr const char *QPL_FINI_JOB_ERROR =
    "An error acquired during job finalization";
static constexpr const char *QPL_OPERATION_ERR =
    "Non-supported value in the qpl_job operation field";
static constexpr const char *INPUT_INVALID =
    "Input byteArray or buffer is invalid";
static constexpr const char *OUTPUT_INVALID =
    "Output byteArray or buffer is invalid";

static int src_chunk_len;
static int idxd_wq_max_transfer_bytes;
static int estimated_dst_chunk_len;

// Global caching
static jclass qplJob_class;
static jfieldID compression_level_id;
static jfieldID retry_count_id;
static jfieldID jobBuffer_id;
static jfieldID operation_type_id;
static jfieldID flags_id;
static jfieldID bytes_read_id;
static jfieldID bytes_written_id;
static jfieldID output_insufficient_id;

/*
 * This function returns the minimum of two numbers.
 * @param length its chunk_length or estimated output length .
 * @param remaining its input/output remaining length.
 * @return the minimum of two numbers.
 */
static int min(int length, int remaining) { return (length > remaining) ? remaining : length; }

/*
 * Compresses/decompresses a buffer pointed to by the given source pointer and
 * writes it to the destination buffer pointed to by the destination pointer.
 * The read and write of the source and destination buffers is bounded by the
 * source and destination lengths respectively.
 *
 * @param env  pointer to the JNI environment.
 * @param clazz  java class.
 * @param job pointer to the qpl_job struct.
 * @param p_input  pointer to the input buffer.
 * @param input_pos input buffer position.
 * @param input_length input buffer length.
 * @param p_output pointer to the output buffer.
 * @param output_pos output buffer position
 * @param output_length length of the output buffer.
 * @param retry_count the number of decompression retries before we give up.
 * @return qpl_status (0) on success, non-zero otherwise.
 */
static qpl_status compress_or_decompress(JNIEnv *env, jclass clazz,
                                         qpl_job *job, uint8_t *p_input,
                                         jint input_pos, jint input_length,
                                         uint8_t *p_output, jint output_pos,
                                         jint output_length, jint retry_count)
{

  if (job->data_ptr.path == qpl_path_software) {
    return qpl_execute_job(job);
  }
  // initially status will be initialized with qpl error code 57
  qpl_status status = QPL_STS_SIZE_ERR;
  jint src_chunk_size = job->op == qpl_op_decompress ? idxd_wq_max_transfer_bytes : src_chunk_len;
  jint dst_chunk_size = job->op == qpl_op_decompress ? idxd_wq_max_transfer_bytes : estimated_dst_chunk_len;

  jint input_to_consume = input_length;
  jint output_to_fill = output_length;
  jint input_offset = input_pos;
  jint output_offset = output_pos;

  if (input_to_consume < idxd_wq_max_transfer_bytes && output_to_fill < idxd_wq_max_transfer_bytes) {
    do {
      status = qpl_execute_job(job);
      retry_count--;
    } while (status == QPL_STS_QUEUES_ARE_BUSY_ERR && retry_count > 0);
    return status;
  }
  while ((input_to_consume > 0) || (job->op == qpl_op_decompress && status == QPL_STS_MORE_OUTPUT_NEEDED && output_to_fill > 0)) {
    jint in_chunk_length;
    if (input_to_consume <= src_chunk_size) {
      job->flags |= QPL_FLAG_LAST;
      in_chunk_length = input_to_consume;
    } else {
      job->flags &= ~QPL_FLAG_LAST;
      in_chunk_length = src_chunk_size;
    }
    jint out_chunk_length = min(dst_chunk_size, output_to_fill);
    uint32_t previous_total_out = job->total_out;
    job->next_in_ptr = p_input + input_offset;
    job->available_in = in_chunk_length;
    job->next_out_ptr = p_output + output_offset;
    job->available_out = out_chunk_length;

    // Execute compression operation
    do {
      status = qpl_execute_job(job);
      retry_count--;
    } while (status == QPL_STS_QUEUES_ARE_BUSY_ERR && retry_count > 0);

    input_to_consume = input_length - job->total_in;
    output_to_fill = output_length - job->total_out;
    input_offset = input_pos + job->total_in;
    output_offset = output_pos + job->total_out;
    job->flags &= ~QPL_FLAG_FIRST;

    if (job->total_out == previous_total_out) {
      output_to_fill = -1;
      input_to_consume = -1;
    }
    if (status != QPL_STS_OK && !(job->op == qpl_op_decompress && status == QPL_STS_MORE_OUTPUT_NEEDED)) {
      return status;
    }
  }
  return status;
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initValuesAndIDs
 * Signature: (II)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initValuesAndIDs(JNIEnv *env, jclass clazz, jint idxd_wq_size, jint estimated_len) {
  qplJob_class = env->FindClass("com/intel/qpl/QPLJob");
  compression_level_id = env->GetFieldID(qplJob_class, "compressionLevel", "I");
  retry_count_id = env->GetFieldID(qplJob_class, "retryCount", "I");
  jobBuffer_id = env->GetFieldID(qplJob_class, "jobBuffer", "Ljava/nio/ByteBuffer;");
  operation_type_id = env->GetFieldID(qplJob_class, "operationType", "I");
  flags_id = env->GetFieldID(qplJob_class, "flags", "I");
  bytes_read_id = env->GetFieldID(qplJob_class, "bytesRead", "I");
  bytes_written_id = env->GetFieldID(qplJob_class, "bytesWritten", "I");
  output_insufficient_id = env->GetFieldID(qplJob_class, "outputInsufficient", "Z");

  idxd_wq_max_transfer_bytes=idxd_wq_size;
  src_chunk_len = idxd_wq_size/2;
  estimated_dst_chunk_len = estimated_len;
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    getQPLJobSize
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_getQPLJobSize(
    JNIEnv *env, jclass clazz, jint exe_path_code) {
  qpl_path_t e_path = static_cast<qpl_path_t>(exe_path_code);
  uint32_t size = 0;
  qpl_status status = qpl_get_job_size(e_path, &size);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_GET_JOB_SIZE_ERROR, status);
  }
  return static_cast<int>(size);
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    initQPLJob
 * Signature: (ILjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_initQPLJob(JNIEnv *env,
                                                            jclass clazz,
                                                            jint exe_path_code,
                                                            jobject buffer) {
  qpl_path_t e_path = static_cast<qpl_path_t>(exe_path_code);
  uint8_t *job_buffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  qpl_job *job = reinterpret_cast<qpl_job *>(job_buffer);

  qpl_status status = qpl_init_job(e_path, job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_INIT_JOB_ERROR, status);
  }
}

/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    execute
 * Signature:
 * (Lcom/intel/qpl/QPLJob;[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_execute(
    JNIEnv *env, jclass clazz, jobject javaJob, jbyteArray input_arr,
    jobject input_buf, jint input_start, jint input_size, jbyteArray output_arr,
    jobject output_buffer, jint output_start, jint output_max_len) {
  uint8_t *p_input = nullptr;
  uint8_t *p_output = nullptr;
  jboolean is_copy_src = false;
  jboolean is_copy_dest = false;

  if (input_arr != nullptr) {
    p_input = reinterpret_cast<uint8_t *>(
        env->GetPrimitiveArrayCritical(input_arr, &is_copy_src));
  } else if (input_buf != nullptr) {
    p_input =
        reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(input_buf));
  }

  if (p_input == nullptr) {
    throw_exception(env, INPUT_INVALID);
    return 0;
  }

  if (output_arr != nullptr) {
    p_output = reinterpret_cast<uint8_t *>(
        env->GetPrimitiveArrayCritical(output_arr, &is_copy_dest));
  } else if (output_buffer != nullptr) {
    p_output =
        reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(output_buffer));
  }

  if (p_output == nullptr) {
    throw_exception(env, OUTPUT_INVALID);
    return 0;
  }

  jobject buf_val = env->GetObjectField(javaJob, jobBuffer_id);
  jint operation_val = env->GetIntField(javaJob, operation_type_id);
  jint rt = env->GetIntField(javaJob, retry_count_id);
  jint flags_val = env->GetIntField(javaJob, flags_id);

  qpl_operation operationType = static_cast<qpl_operation>(operation_val);
  uint8_t *job_buffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buf_val));
  qpl_job *job = reinterpret_cast<qpl_job *>(job_buffer);
  qpl_status status;

  // Performing an operation
  job->next_in_ptr = p_input + input_start;
  job->available_in = input_size;
  job->next_out_ptr = p_output + output_start;
  job->available_out = output_max_len;
  job->total_in=0;
  job->total_out=0;

  switch (operationType) {
  case qpl_op_decompress: {
    job->op = qpl_op_decompress;
    job->flags = flags_val;
    break;
  }
  case qpl_op_compress: {
    // Returns the field ID for an compressionLevel instance variable of a
    // QPLJob class.
    jint cl_val = env->GetIntField(javaJob, compression_level_id);
    job->op = qpl_op_compress;
    job->level = static_cast<qpl_compression_levels>(cl_val);
    job->flags = flags_val;
    break;
  }
  default: {
    throw_exception(env, QPL_OPERATION_ERR);
    return 0;
  }
  }
  // if queues are busy then retry the task execution until operation count
  // reaches its retryCount.
  int retry_count = rt;

  status =
      compress_or_decompress(env, clazz, job, p_input, input_start, input_size,
                             p_output, output_start, output_max_len, rt);

  env->SetBooleanField(javaJob, output_insufficient_id, JNI_FALSE);
  if (status != QPL_STS_OK) {
    if (job->op == qpl_op_compress && status == QPL_STS_MORE_OUTPUT_NEEDED) {
      throw_ouput_overflow_exception(env, QPL_EXECUTE_JOB_ERROR, status);
    } else if (job->op == qpl_op_decompress && status == QPL_STS_MORE_OUTPUT_NEEDED ){
        if(job->total_in == 0 && job->total_out == 0){
        throw_ouput_overflow_exception(env, QPL_EXECUTE_JOB_ERROR, status);
        }
        else{
        env->SetBooleanField(javaJob, output_insufficient_id, JNI_TRUE);
        }
    } else if (status != QPL_STS_MORE_OUTPUT_NEEDED) {
      throw_exception(env, QPL_EXECUTE_JOB_ERROR, status);
    }
  }

  if (input_arr != nullptr) {
    env->ReleasePrimitiveArrayCritical(input_arr, reinterpret_cast<jbyte *>(p_input), 0);
  }
  if (output_arr != nullptr) {
    env->ReleasePrimitiveArrayCritical(output_arr, reinterpret_cast<jbyte *>(p_output), 0);
  }

  env->SetIntField(javaJob, bytes_read_id, job->total_in);
  env->SetIntField(javaJob, bytes_written_id, job->total_out);


  return job->total_out;
}
/*
 * Class:     com_intel_qpl_QPLJNI
 * Method:    finish
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_intel_qpl_QPLJNI_finish(JNIEnv *env,
                                                        jclass clazz,
                                                        jobject buffer) {
  uint8_t *job_buffer = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  qpl_job *job = reinterpret_cast<qpl_job *>(job_buffer);
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
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isExecutionPathAvailable(
    JNIEnv *env, jclass clazz, jint exe_path_code) {
  qpl_path_t execution_path = static_cast<qpl_path_t>(exe_path_code);
  uint32_t size = 0;

  // Job initialization
  qpl_status status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    return status;
  }

  std::unique_ptr<qpl_job[]> job_buffer = nullptr;
  try {
    job_buffer = std::make_unique<qpl_job[]>(size);
  } catch (std::bad_alloc &e) {
    throw_exception(env, e.what());
  }

  qpl_job *job = job_buffer.get();
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
JNIEXPORT jint JNICALL Java_com_intel_qpl_QPLJNI_isCompressionLevelSupported(
    JNIEnv *env, jclass clazz, jint exe_path_code, jint cl) {
  constexpr uint8_t source_size = 40;

  qpl_path_t execution_path = static_cast<qpl_path_t>(exe_path_code);
  qpl_compression_levels level = static_cast<qpl_compression_levels>(cl);

  // Job initialization
  uint32_t size = 0;
  qpl_status status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_GET_JOB_SIZE_ERROR, status);
  }
  std::unique_ptr<qpl_job[]> job_buffer = nullptr;
  try {
    job_buffer = std::make_unique<qpl_job[]>(size);
  } catch (std::bad_alloc &e) {
    throw_exception(env, e.what());
  }

  qpl_job *job = job_buffer.get();
  status = qpl_init_job(execution_path, job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_INIT_JOB_ERROR, status);
  }

  uint8_t source[source_size];
  uint8_t destination[source_size + 5];
  for (int i = 0; i < source_size; ++i) source[i] = i;

  job->op = qpl_op_compress;
  job->level = level;
  job->next_in_ptr = source;
  job->available_in = source_size;
  job->next_out_ptr = destination;
  job->available_out = source_size * 5;
  job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN |
               QPL_FLAG_OMIT_CHECKSUMS;

  // Compression
  status = qpl_execute_job(job);
  if (status == QPL_STS_UNSUPPORTED_COMPRESSION_LEVEL) {
    return status;
  } else if (status != QPL_STS_OK) {
    throw_exception(env, QPL_FINI_JOB_ERROR, status);
  }
  // Freeing resources
  status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    throw_exception(env, QPL_FINI_JOB_ERROR, status);
  }
  return status;
}
