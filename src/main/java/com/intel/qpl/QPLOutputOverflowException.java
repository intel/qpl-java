/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package com.intel.qpl;

/**
 * Thrown to indicate that the qpl operation failed due to insufficient space in the provided output
 * buffer.
 */
public class QPLOutputOverflowException extends QPLException {
  /**
   * Constructs a new QPLOutputOverflowException with the specified message.
   *
   * @param message error message
   */
  public QPLOutputOverflowException(String message) {
    super(message);
  }
}
