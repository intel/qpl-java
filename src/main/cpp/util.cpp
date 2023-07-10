/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

#include "util.h"

#include <string>

void throw_exception(JNIEnv *env, const char *arg, jlong status) {
  char buf[256];
  jclass clz = env->FindClass("com/intel/qpl/QPLException");
  std::snprintf(buf, sizeof(buf), "%s. Status code is - %ld", arg, status);
  env->ThrowNew(clz, buf);
}

void throw_exception(JNIEnv *env, const char *arg) {
  jclass clz = env->FindClass("com/intel/qpl/QPLException");
  env->ThrowNew(clz, arg);
}
