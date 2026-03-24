# DecentDB JNI Native Library Build
#
# This Makefile builds the JNI bridge library that connects the Java JDBC driver
# to the DecentDB C API.
#
# Usage:
#   make                        # Build for current platform
#   make DECENTDB_LIB_DIR=...   # Point to directory containing libdecentdb.so
#
# Prerequisites:
#   - JDK with jni.h in $JAVA_HOME/include
#   - DecentDB shared library: run `cargo build -p decentdb` first from the repo root
#
# Output: target/debug/libdecentdb_jni.so (Linux), target/debug/libdecentdb_jni.dylib (macOS)

JAVA_HOME ?= $(shell java -XshowSettings:all -version 2>&1 | grep "java.home" | awk '{print $$3}')
DECENTDB_LIB_DIR ?= $(shell p=$$(realpath ../../../target/debug 2>/dev/null || echo ../../../target/debug); cygpath -m "$$p" 2>/dev/null || echo "$$p")
BUILD_DIR = $(DECENTDB_LIB_DIR)

OS := $(shell uname -s)
ARCH := $(shell uname -m)

IS_WINDOWS := 0
ifneq (,$(filter MINGW% MSYS% CYGWIN%,$(OS)))
    IS_WINDOWS := 1
endif

ifeq ($(OS),Darwin)
    LIB_EXT = dylib
    SHARED_FLAGS = -dynamiclib
    OS_INCLUDES = -I$(JAVA_HOME)/include/darwin
    PLATFORM_FLAGS = -undefined dynamic_lookup
    RPATH_FLAGS = -Wl,-rpath,@loader_path -Wl,-rpath,$(DECENTDB_LIB_DIR)
else ifeq ($(OS),Linux)
    LIB_EXT = so
    SHARED_FLAGS = -shared -fPIC
    OS_INCLUDES = -I$(JAVA_HOME)/include/linux
    PLATFORM_FLAGS =
    RPATH_FLAGS = -Wl,-rpath,'$$ORIGIN' -Wl,-rpath,$(DECENTDB_LIB_DIR)
else ifeq ($(IS_WINDOWS),1)
    LIB_EXT = dll
    SHARED_FLAGS = -shared
    OS_INCLUDES = -I$(JAVA_HOME)/include/win32
    PLATFORM_FLAGS = -Wl,--enable-auto-import
    RPATH_FLAGS =
else
    $(error Unsupported OS: $(OS))
endif

CC = gcc
CFLAGS = -O2 -Wall \
    -I$(JAVA_HOME)/include \
    $(OS_INCLUDES) \
    -I.

ifeq ($(IS_WINDOWS),1)
    OUTPUT_LIB = $(BUILD_DIR)/decentdb_jni.$(LIB_EXT)
else
    OUTPUT_LIB = $(BUILD_DIR)/libdecentdb_jni.$(LIB_EXT)
endif

ifeq ($(IS_WINDOWS),1)
    # Rust cdylib typically produces target/debug/decentdb.dll on Windows.
    ifneq (,$(wildcard $(DECENTDB_LIB_DIR)/libdecentdb.dll.a))
        DECENTDB_LINK_FLAGS = $(DECENTDB_LIB_DIR)/libdecentdb.dll.a
    else ifneq (,$(wildcard $(DECENTDB_LIB_DIR)/decentdb.lib))
        DECENTDB_LINK_FLAGS = $(DECENTDB_LIB_DIR)/decentdb.lib
    else ifneq (,$(wildcard $(DECENTDB_LIB_DIR)/decentdb.dll))
        DECENTDB_LINK_FLAGS = $(DECENTDB_LIB_DIR)/decentdb.dll
    else
        DECENTDB_LINK_FLAGS = -L$(DECENTDB_LIB_DIR) -ldecentdb
    endif
else
    DECENTDB_LINK_FLAGS = -L$(DECENTDB_LIB_DIR) -ldecentdb
endif

.PHONY: all clean

all: $(OUTPUT_LIB)

$(OUTPUT_LIB): decentdb_jni.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(SHARED_FLAGS) $(PLATFORM_FLAGS) \
	    -o $@ \
	    decentdb_jni.c \
        $(DECENTDB_LINK_FLAGS) \
        $(RPATH_FLAGS)
	@echo "Built: $@"

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -f $(OUTPUT_LIB)
