/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

#include <jni.h>

#include "qpl/qpl.h"

#ifndef _Included_com_intel_qpl_util
#define _Included_com_intel_qpl_util

void throw_exception(JNIEnv *env, const char *arg, jlong status);

void throw_exception(JNIEnv *env, const char *arg);

void throw_ouput_overflow_exception(JNIEnv *env, const char *arg, jlong status);


#endif