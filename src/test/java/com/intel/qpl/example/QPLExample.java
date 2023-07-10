/*******************************************************************************
 * Copyright (C) 2023 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package com.intel.qpl.example;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;

public class QPLExample {

    private static final int compressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId() | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId();
    private static final int decompressionFlags = QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId();
    private static final QPLUtils.Operations compressOperation = QPLUtils.Operations.QPL_OP_COMPRESS;
    private static final QPLUtils.Operations decompressOperation = QPLUtils.Operations.QPL_OP_DECOMPRESS;

    public static void main(String[] args) {
        compressDecompress();
        compressDecompressWithOptions();
    }

    private static void compressDecompress() {

        byte[] uncompressedBytes = "12345345234572".getBytes(StandardCharsets.UTF_8);
        int compressedLength = QPLJob.maxCompressedLength(uncompressedBytes.length);
        byte[] compressedBytes = new byte[compressedLength];

        //Create  QPLJob for compression
        QPLJob qplJob = new QPLJob();
        qplJob.setOperationType(compressOperation);
        qplJob.setFlags(compressionFlags);
        qplJob.execute(uncompressedBytes, compressedBytes);

        //Reset qpljob for Decompression
        byte[] uncompressedResult = new byte[uncompressedBytes.length];
        qplJob.setOperationType(decompressOperation);
        qplJob.setFlags(decompressionFlags);
        qplJob.execute(compressedBytes, uncompressedResult);

        if (Arrays.equals(uncompressedBytes, uncompressedResult)) {
            System.out.println("**************************************************");
            System.out.println("Compress/Decompress succeeded");
            System.out.println("**************************************************");
        }
        else {
            System.out.println("**************************************************");
            System.out.println("Compress/Decompress failed");
            System.out.println("**************************************************");
        }
    }

    private static void compressDecompressWithOptions() {

        byte[] uncompressedBytes = "12345345234572".getBytes(StandardCharsets.UTF_8);
        int compressedLength = QPLJob.maxCompressedLength(uncompressedBytes.length);
        byte[] compressedBytes = new byte[compressedLength];

        //Create  QPLJob for compression
        QPLJob qplJob = new QPLJob(QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
        qplJob.setOperationType(compressOperation);
        qplJob.setFlags(compressionFlags);
        qplJob.setCompressionLevel(1);
        qplJob.setRetryCount(0);
        qplJob.execute(uncompressedBytes, compressedBytes);

        //Reset qpljob for Decompression
        byte[] uncompressedResult = new byte[uncompressedBytes.length];
        qplJob.setOperationType(decompressOperation);
        qplJob.setFlags(decompressionFlags);
        qplJob.setRetryCount(0);
        qplJob.execute(compressedBytes, uncompressedResult);

        if (Arrays.equals(uncompressedBytes, uncompressedResult)) {
            System.out.println("**************************************************");
            System.out.println("Compress/Decompress succeeded");
            System.out.println("**************************************************");
        }
        else {
            System.out.println("**************************************************");
            System.out.println("Compress/Decompress failed");
            System.out.println("**************************************************");
        }
    }
}
