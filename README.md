<!-------------------------------------
Copyright (C) 2023 Intel Corporation
SPDX-License-Identifier: MIT
--------------------------------------->

# Java* Native Interface binding for Intel® Query Processing Library #

## OVERVIEW ##
This library will allow Java* applications to communicate with the Intel® Query Processing Library (Intel® QPL) , for use with Intel® In-Memory Analytics Accelerator (Intel® IAA) , to improve performance by accelerating operations like compression and decompression.

## HOW TO BUILD & RUN ##

### PREREQUISITES TO BUILD ###
The following are the prerequisites for building this Java library:

1. Intel® QPL library - To build, Intel® QPL follow [Installation](https://intel.github.io/qpl/documentation/get_started_docs/installation.html).
   Make sure Intel® QPL library installed into either "/usr/local/lib64" or "/usr/local/lib".
2. Java 11 or Java 17.
3. Build tools - **g++**, **CMake** , **Maven** and **clang** (for fuzz testing).


### PREREQUISITES TO RUN ###
This library assumes the availability of Intel® IAA hardware.

For more information about the Intel&reg; In-Memory Analytics Accelerator, refer to the [IAA spec](https://cdrdv2.intel.com/v1/dl/getContent/721858) on the [Intel&reg; 64 and IA-32 Architectures Software Developer Manuals](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html) page.

### STEPS TO BUILD ###
Once all the prerequisites have been satisfied:
   ```
   $ git clone https://github.com/intel/qpl-java.git
   $ cd qpl-java
   $ mvn clean package
   ```

Available Maven commands include:

- `compile` - builds sources
- `test` - builds and runs tests
- `site` - generates Surefire report into ```target/site```
- `javadoc:javadoc` - builds javadocs into ```target/site/apidocs```
- `package` - builds jar file into ```target``` directory
- `spotless:check` - check if source code is formatted well.
- `spotless:apply` - fixes source code format issues.


### LIBRARY TESTING ###
This library supports both functional and Fuzz testing.

##### FUNCTIONAL TEST #####
To run all the functional tests, execute the following command:
```
mvn clean test
```
##### FUZZ TEST #####
Jazzer tool is used to enable fuzz testing on this project.

see [here](https://github.com/CodeIntelligenceTesting/jazzer/blob/main/CONTRIBUTING.md) for Jazzer dependencies.


To run the Fuzz tests, execute the following command:
```
mvn clean test -Dfuzzing=true
```
The above command executes each Jazzer Fuzz tests for 10 seconds.
To run for a longer duration, modify ```-max_total_time``` fuzzParameter in pom.xml
### USING THIS LIBRARY IN EXISTING JAVA APPLICATIONS ###
To use this library in your Java application, build the qpl-java jar and include
its location in your Java classpath.  For example:
   ```
   $ mvn package
   $ javac -cp .:<path>/qpl-java/target/qpl-java-<version>.jar <source>
   $ java -cp .:<path>/qpl-java/target/qpl-java-<version>.jar <class>
   ```

Alternatively, include qpl-java's `target/classes` directory in your Java classpath and the
`target/cppbuild` directory in your `java.library.path`.  For example:
   ```
   $ mvn compile
   $ javac -cp .:<path>/qpl-java/target/classes <source>
   $ java -cp .:<path>/qpl-java/target/classes -Djava.library.path=<path>/qpl-java/target/cppbuild <class>
   ```
## CONTRIBUTING ##
Thanks for your interest! Please see the CONTRIBUTING.md document for information on how to contribute.
## Contacts ##
For more information on this library, contact Kokoori, Shylaja (shylaja.kokoori@intel.com) or Suvarna Reddy, Sevanthi (sevanthi.suvarna.reddy@intel.com) .

&nbsp;

><b id="f1">*</b> Java is a registered trademark of Oracle and/or its affiliates.