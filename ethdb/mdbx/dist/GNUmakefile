# This makefile is for GNU Make 3.80 or above, and nowadays provided
# just for compatibility and preservation of traditions.
#
# Please use CMake in case of any difficulties or
# problems with this old-school's magic.
#
################################################################################
#
# Preprocessor macros (for MDBX_OPTIONS) of interest.
#
# Note that the defaults should already be correct for most platforms;
# you should not need to change any of these. Read their descriptions
# in README and source code (see src/options.h) if you do.
# There may be other macros of interest.
#

SHELL   := env bash

# install sandbox
DESTDIR ?=

# install prefixes (inside sandbox)
prefix  ?= /usr/local
mandir  ?= $(prefix)/man

# lib/bin suffix for multiarch/biarch, e.g. '.x86_64'
suffix  ?=

CC      ?= gcc
CFLAGS_EXTRA ?=
LD      ?= ld
MDBX_OPTIONS ?= -DNDEBUG=1
CFLAGS  ?= -O2 -g -Wall -Werror -Wextra -Wpedantic -ffunction-sections -fPIC -fvisibility=hidden -std=gnu11 -pthread -Wno-error=attributes $(CFLAGS_EXTRA)
# -Wno-tautological-compare
CXX     ?= g++
# Choosing C++ standard with deferred simple variable expansion trick
CXXSTD  ?= $(eval CXXSTD := $$(shell PROBE=$$$$([ -f mdbx.c++ ] && echo mdbx.c++ || echo src/mdbx.c++); for std in gnu++20 c++20 gnu++2a c++2a gnu++17 c++17 gnu++14 c++14 gnu+11 c++11; do $(CXX) -std=$$$${std} -c $$$${PROBE} -o /dev/null 2>/dev/null >/dev/null && echo "-std=$$$${std}" && exit; done))$(CXXSTD)
CXXFLAGS = $(CXXSTD) $(filter-out -std=gnu11,$(CFLAGS))

# HINT: Try append '--no-as-needed,-lrt' for ability to built with modern glibc, but then run with the old.
LIBS    ?= $(shell uname | grep -qi SunOS && echo "-lkstat") $(shell uname | grep -qi -e Darwin -e OpenBSD || echo "-lrt") $(shell uname | grep -qi Windows && echo "-lntdll")

LDFLAGS ?= $(shell $(LD) --help 2>/dev/null | grep -q -- --gc-sections && echo '-Wl,--gc-sections,-z,relro,-O1')$(shell $(LD) --help 2>/dev/null | grep -q -- -dead_strip && echo '-Wl,-dead_strip')
EXE_LDFLAGS ?= -pthread

################################################################################

UNAME      := $(shell uname -s 2>/dev/null || echo Unknown)
define uname2sosuffix
  case "$(UNAME)" in
    Darwin*|Mach*) echo dylib;;
    CYGWIN*|MINGW*|MSYS*|Windows*) echo dll;;
    *) echo so;;
  esac
endef
SO_SUFFIX  := $(shell $(uname2sosuffix))

HEADERS    := mdbx.h mdbx.h++
LIBRARIES  := libmdbx.a libmdbx.$(SO_SUFFIX)
TOOLS      := mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
MANPAGES   := mdbx_stat.1 mdbx_copy.1 mdbx_dump.1 mdbx_load.1 mdbx_chk.1

.PHONY: mdbx all install clean

all: $(LIBRARIES) $(TOOLS)

mdbx: libmdbx.a libmdbx.$(SO_SUFFIX)

tools: $(TOOLS)

strip: all
	strip libmdbx.$(SO_SUFFIX) $(TOOLS)

clean:
	rm -rf $(TOOLS) mdbx_test @* *.[ao] *.[ls]o *~ tmp.db/* \
		*.gcov *.log *.err src/*.o test/*.o mdbx_example dist \
		config.h src/config.h src/version.c *.tar*

libmdbx.a: mdbx-static.o mdbx++-static.o
	$(AR) rs $@ $?

libmdbx.$(SO_SUFFIX): mdbx-dylib.o mdbx++-dylib.o
	$(CXX) $(CXXFLAGS) $^ -pthread -shared $(LDFLAGS) $(LIBS) -o $@


################################################################################
# Amalgamated source code, i.e. distributed after `make dist`
MAN_SRCDIR := man1/

config.h: mdbx.c $(lastword $(MAKEFILE_LIST))
	(echo '#define MDBX_BUILD_TIMESTAMP "$(shell date +%Y-%m-%dT%H:%M:%S%z)"' \
	&& echo '#define MDBX_BUILD_FLAGS "$(CXXSTD) $(CFLAGS) $(LDFLAGS) $(LIBS)"' \
	&& echo '#define MDBX_BUILD_COMPILER "$(shell (LC_ALL=C $(CC) --version || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_TARGET "$(shell set -o pipefail; (LC_ALL=C $(CC) -v 2>&1 | grep -i '^Target:' | cut -d ' ' -f 2- || (LC_ALL=C $(CC) --version | grep -qi e2k && echo E2K) || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	) > $@

mdbx-dylib.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST))
	$(CC) $(CFLAGS) $(MDBX_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c -o $@

mdbx-static.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST))
	$(CC) $(CFLAGS) $(MDBX_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c -o $@

mdbx++-dylib.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	$(CXX) $(CXXFLAGS) $(MDBX_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c++ -o $@

mdbx++-static.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	$(CXX) $(CXXFLAGS) $(MDBX_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c++ -o $@

mdbx_%:	mdbx_%.c libmdbx.a
	$(CC) $(CFLAGS) $(MDBX_OPTIONS) '-DMDBX_CONFIG_H="config.h"' $^ $(EXE_LDFLAGS) $(LIBS) -o $@

install: $(LIBRARIES) $(TOOLS) $(HEADERS)
	install -D -p -s -t $(DESTDIR)$(prefix)/bin$(suffix) $(TOOLS) && \
	install -D -p -s -t $(DESTDIR)$(prefix)/lib$(suffix) $(filter-out libmdbx.a,$(LIBRARIES)) && \
	install -D -p -t $(DESTDIR)$(prefix)/lib$(suffix) libmdbx.a && \
	install -D -p -m 444 -t $(DESTDIR)$(prefix)/include $(HEADERS) && \
	install -D -p -m 444 -t $(DESTDIR)$(mandir)/man1 $(addprefix $(MAN_SRCDIR), $(MANPAGES))

uninstall:
	rm -f $(addprefix $(DESTDIR)$(prefix)/bin$(suffix)/,$(TOOLS)) \
		$(addprefix $(DESTDIR)$(prefix)/lib$(suffix)/,$(LIBRARIES)) \
		$(addprefix $(DESTDIR)$(prefix)/include/,$(HEADERS)) \
		$(addprefix $(DESTDIR)$(mandir)/man1/,$(MANPAGES))

################################################################################
# Benchmarking by ioarena

IOARENA ?= $(shell \
  (test -x ../ioarena/@BUILD/src/ioarena && echo ../ioarena/@BUILD/src/ioarena) || \
  (test -x ../../@BUILD/src/ioarena && echo ../../@BUILD/src/ioarena) || \
  (test -x ../../src/ioarena && echo ../../src/ioarena) || which ioarena)
NN	?= 25000000
BENCH_CRUD_MODE ?= nosync

ifneq ($(wildcard $(IOARENA)),)

.PHONY: bench clean-bench re-bench

clean-bench:
	rm -rf bench-*.txt _ioarena/*

re-bench: clean-bench bench

define bench-rule
bench-$(1)_$(2).txt: $(3) $(IOARENA) $(lastword $(MAKEFILE_LIST))
	LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B crud -m $(BENCH_CRUD_MODE) -n $(2) \
		| tee $$@ | grep throughput && \
	LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B get,iterate -m $(BENCH_CRUD_MODE) -r 4 -n $(2) \
		| tee -a $$@ | grep throughput \
	|| mv -f $$@ $$@.error

endef

$(eval $(call bench-rule,mdbx,$(NN),libmdbx.$(SO_SUFFIX)))

$(eval $(call bench-rule,sophia,$(NN)))
$(eval $(call bench-rule,leveldb,$(NN)))
$(eval $(call bench-rule,rocksdb,$(NN)))
$(eval $(call bench-rule,wiredtiger,$(NN)))
$(eval $(call bench-rule,forestdb,$(NN)))
$(eval $(call bench-rule,lmdb,$(NN)))
$(eval $(call bench-rule,nessdb,$(NN)))
$(eval $(call bench-rule,sqlite3,$(NN)))
$(eval $(call bench-rule,ejdb,$(NN)))
$(eval $(call bench-rule,vedisdb,$(NN)))
$(eval $(call bench-rule,dummy,$(NN)))

$(eval $(call bench-rule,debug,10))

bench: bench-mdbx_$(NN).txt

.PHONY: bench-debug

bench-debug: bench-debug_10.txt

bench-quartet: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt bench-rocksdb_$(NN).txt bench-wiredtiger_$(NN).txt
bench-triplet: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt bench-sqlite3_$(NN).txt
bench-couple: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt

endif
