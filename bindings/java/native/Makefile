# DecentDB JNI Native Library Build
#
# This Makefile builds the JNI bridge library that connects the Java JDBC driver
# to the DecentDB C API.
#
# Usage:
#   make                        # Build for current platform
#   make DECENTDB_LIB_DIR=...   # Point to directory containing libc_api.so
#
# Prerequisites:
#   - JDK with jni.h in $JAVA_HOME/include
#   - DecentDB shared library: run `nimble build_lib` first from the repo root
#
# Output: build/libdecentdb_jni.so (Linux), build/libdecentdb_jni.dylib (macOS)

JAVA_HOME ?= $(shell java -XshowSettings:all -version 2>&1 | grep "java.home" | awk '{print $$3}')
DECENTDB_LIB_DIR ?= $(shell realpath ../../../build 2>/dev/null || echo ../../../build)
BUILD_DIR = $(shell realpath ../../../build 2>/dev/null || echo ../../../build)

OS := $(shell uname -s)
ARCH := $(shell uname -m)

ifeq ($(OS),Darwin)
    LIB_EXT = dylib
    SHARED_FLAGS = -dynamiclib
    OS_INCLUDES = -I$(JAVA_HOME)/include/darwin
    PLATFORM_FLAGS = -undefined dynamic_lookup
else ifeq ($(OS),Linux)
    LIB_EXT = so
    SHARED_FLAGS = -shared -fPIC
    OS_INCLUDES = -I$(JAVA_HOME)/include/linux
    PLATFORM_FLAGS =
else
    $(error Unsupported OS: $(OS))
endif

CC = gcc
CFLAGS = -O2 -fPIC -Wall \
    -I$(JAVA_HOME)/include \
    $(OS_INCLUDES) \
    -I.

OUTPUT_LIB = $(BUILD_DIR)/libdecentdb_jni.$(LIB_EXT)

.PHONY: all clean

all: $(OUTPUT_LIB)

$(OUTPUT_LIB): decentdb_jni.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(SHARED_FLAGS) $(PLATFORM_FLAGS) \
	    -o $@ \
	    decentdb_jni.c \
	    -L$(DECENTDB_LIB_DIR) -lc_api \
	    -Wl,-rpath,'$$ORIGIN' \
	    -Wl,-rpath,$(DECENTDB_LIB_DIR)
	@echo "Built: $@"

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -f $(OUTPUT_LIB)
