/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl.fuzzing;

import com.intel.qpl.QPLUtils;

public class CompressorConstants {
  static final int compressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId() | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId();
  static final int decompressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId();
  static final QPLUtils.Operations compress = QPLUtils.Operations.QPL_OP_COMPRESS;
  static final QPLUtils.Operations decompress = QPLUtils.Operations.QPL_OP_DECOMPRESS;
  static final QPLUtils.ExecutionPaths executionPath = QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE;
}
