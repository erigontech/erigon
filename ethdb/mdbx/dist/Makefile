# This is thunk-Makefile for calling GNU Make 3.80 or above

all bench bench-quartet build-test check clean clean-bench cross-gcc cross-qemu dist doxygen gcc-analyzer install mdbx memcheck reformat release-assets strip test test-asan test-fault test-leak test-singleprocess test-ubsan test-valgrind tools:
	@CC=$(CC) \
	CXX=`if test -n "$(CXX)" && which "$(CXX)" > /dev/null; then echo "$(CXX)"; elif test -n "$(CCC)" && which "$(CCC)" > /dev/null; then echo "$(CCC)"; else echo "c++"; fi` \
	`which gmake || which gnumake || echo 'echo "GNU Make 3.80 or above is required"; exit 2;'` \
		$(MAKEFLAGS) -f GNUmakefile $@
