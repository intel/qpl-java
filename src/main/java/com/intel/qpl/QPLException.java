/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl;

/** Thrown to indicate that the qpl operation failed to execute successfully. */
public class QPLException extends RuntimeException {

  /**
   * Constructs a new QPLException with the specified message.
   *
   * @param message error message
   */
  public QPLException(String message) {
    super(message);
  }
}
