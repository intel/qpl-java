/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl.junit;

import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectPackages("com.intel.qpl.junit")
public class QPLTestSuite {
  private static final QPLUtils.ExecutionPaths path =
      QPLJob.getValidExecutionPath(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE);
  public static final boolean FORCE_HARDWARE =
      (path != null && path.equals(QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE));
}
