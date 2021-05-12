# This is thunk-Makefile for calling GNU Make 3.80 or above

all help options \
clean install install-no-strip install-strip strip tools uninstall \
bench bench-clean bench-couple bench-quartet bench-triplet re-bench \
lib libmdbx mdbx mdbx_chk mdbx_copy mdbx_drop mdbx_dump mdbx_load mdbx_stat \
check dist memcheck cross-gcc cross-qemu doxygen gcc-analyzer reformat \
release-assets tags test build-test mdbx_test \
test-asan test-fault test-leak test-singleprocess test-ubsan test-valgrind:
	@CC=$(CC) \
	CXX=`if test -n "$(CXX)" && which "$(CXX)" > /dev/null; then echo "$(CXX)"; elif test -n "$(CCC)" && which "$(CCC)" > /dev/null; then echo "$(CCC)"; else echo "c++"; fi` \
	`which gmake || which gnumake || echo 'echo "GNU Make 3.80 or above is required"; exit 2;'` \
		$(MAKEFLAGS) -f GNUmakefile $@
