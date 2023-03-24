/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

#include <jni.h>
#include "qpl/qpl.h"

void throw_exception(JNIEnv *env, const char *arg, jlong status);

void throw_exception(JNIEnv *env, const char *arg);