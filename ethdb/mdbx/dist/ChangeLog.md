ChangeLog
---------

## v0.9.4 (in development) scheduled at 2021-02-23

TODO:
 - Engage new terminology (https://github.com/erthink/libmdbx/issues/137).
 - Resolve few TODOs (https://github.com/erthink/libmdbx/issues/124, https://github.com/erthink/libmdbx/issues/127,
   https://github.com/erthink/libmdbx/issues/115).
 - Finalize C++ API (few typos and trivia bugs are still likely for now).
 - Packages for [ROSA Linux](https://www.rosalinux.ru/), [ALT Linux](https://www.altlinux.org/), Fedora/RHEL, Debian/Ubuntu.

## v0.9.3 at 2021-02-02

Acknowledgements:

 - [Mahlon E. Smith](http://www.martini.nu/) for [FreeBSD port of libmdbx](https://svnweb.freebsd.org/ports/head/databases/mdbx/).
 - [장세연](http://www.castis.com) for bug fixing and PR.
 - [Clément Renault](https://github.com/Kerollmops/heed) for [Heed](https://github.com/Kerollmops/heed) fully typed Rust wrapper.
 - [Alex Sharov](https://github.com/AskAlexSharov) for bug reporting.
 - [Noel Kuntze](https://github.com/Thermi) for bug reporting.

Removed options and features:

 - Drop `MDBX_HUGE_TRANSACTIONS` build-option (now no longer required).

New features:

 - Package for FreeBSD is available now by Mahlon E. Smith.
 - New API functions to get/set various options (https://github.com/erthink/libmdbx/issues/128):
    - the maximum number of named databases for the environment;
    - the maximum number of threads/reader slots;
    - threshold (since the last unsteady commit) to force flush the data buffers to disk;
    - relative period (since the last unsteady commit) to force flush the data buffers to disk;
    - limit to grow a list of reclaimed/recycled page's numbers for finding a sequence of contiguous pages for large data items;
    - limit to grow a cache of dirty pages for reuse in the current transaction;
    - limit of a pre-allocated memory items for dirty pages;
    - limit of dirty pages for a write transaction;
    - initial allocation size for dirty pages list of a write transaction;
    - maximal part of the dirty pages may be spilled when necessary;
    - minimal part of the dirty pages should be spilled when necessary;
    - how much of the parent transaction dirty pages will be spilled while start each child transaction;
 - Unlimited/Dynamic size of retired and dirty page lists (https://github.com/erthink/libmdbx/issues/123).
 - Added `-p` option (purge subDB before loading) to `mdbx_load` tool.
 - Reworked spilling of large transaction and committing of nested transactions:
    - page spilling code reworked to avoid the flaws and bugs inherited from LMDB;
    - limit for number of dirty pages now is controllable at runtime;
    - a spilled pages, including overflow/large pages, now can be reused and refunded/compactified in nested transactions;
    - more effective refunding/compactification especially for the loosed page cache.
 - Added `MDBX_ENABLE_REFUND` and `MDBX_PNL_ASCENDING` internal/advanced build options.
 - Added `mdbx_default_pagesize()` function.
 - Better support architectures with a weak/relaxed memory consistency model (ARM, AARCH64, PPC, MIPS, RISC-V, etc) by means [C11 atomics](https://en.cppreference.com/w/c/atomic).
 - Speed up page number lists and dirty page lists (https://github.com/erthink/libmdbx/issues/132).
 - Added `LIBMDBX_NO_EXPORTS_LEGACY_API` build option.

Fixes:

 - Fixed missing cleanup (null assigned) in the C++ commit/abort (https://github.com/erthink/libmdbx/pull/143).
 - Fixed `mdbx_realloc()` for case of nullptr and `MDBX_AVOID_CRT=ON` for Windows.
 - Fixed the possibility to use invalid and renewed (closed & re-opened, dropped & re-created) DBI-handles (https://github.com/erthink/libmdbx/issues/146).
 - Fixed 4-byte aligned access to 64-bit integers, including access to the `bootid` meta-page's field (https://github.com/erthink/libmdbx/issues/153).
 - Fixed minor/potential memory leak during page flushing and unspilling.
 - Fixed handling states of cursors's and subDBs's for nested transactions.
 - Fixed page leak in extra rare case the list of retired pages changed during update GC on transaction commit.
 - Fixed assertions to avoid false-positive UB detection by CLANG/LLVM (https://github.com/erthink/libmdbx/issues/153).
 - Fixed `MDBX_TXN_FULL` and regressive `MDBX_KEYEXIST` during large transaction commit with `MDBX_LIFORECLAIM` (https://github.com/erthink/libmdbx/issues/123).
 - Fixed auto-recovery (`weak->steady` with the same boot-id) when Database size at last weak checkpoint is large than at last steady checkpoint.
 - Fixed operation on systems with unusual small/large page size, including PowerPC (https://github.com/erthink/libmdbx/issues/157).


## v0.9.2 at 2020-11-27

Acknowledgements:

 - Jens Alfke (Mobile Architect at [Couchbase](https://www.couchbase.com/)) for [NimDBX](https://github.com/snej/nimdbx).
 - Clément Renault (CTO at [MeiliSearch](https://www.meilisearch.com/)) for [mdbx-rs](https://github.com/Kerollmops/mdbx-rs).
 - Alex Sharov (Go-Lang Teach Lead at [TurboGeth/Ethereum](https://ethereum.org/)) for an extreme test cases and bug reporting.
 - George Hazan (CTO at [Miranda NG](https://www.miranda-ng.org/)) for bug reporting.
 - [Positive Technologies](https://www.ptsecurity.com/) for funding and [The Standoff](https://standoff365.com/).

Added features:

 - Provided package for [buildroot](https://buildroot.org/).
 - Binding for Nim is [available](https://github.com/snej/nimdbx) now by Jens Alfke.
 - Added `mdbx_env_delete()` for deletion an environment files in a proper and multiprocess-safe way.
 - Added `mdbx_txn_commit_ex()` with collecting latency information.
 - Fast completion pure nested transactions.
 - Added `LIBMDBX_INLINE_API` macro and inline versions of some API functions.
 - Added `mdbx_cursor_copy()` function.
 - Extended tests for checking cursor tracking.
 - Added `MDBX_SET_LOWERBOUND` operation for `mdbx_cursor_get()`.

Fixes:

 - Fixed missing installation of `mdbx.h++`.
 - Fixed use of obsolete `__noreturn`.
 - Fixed use of `yield` instruction on ARM if unsupported.
 - Added pthread workaround for buggy toolchain/cmake/buildroot.
 - Fixed use of `pthread_yield()` for non-GLIBC.
 - Fixed use of `RegGetValueA()` on Windows 2000/XP.
 - Fixed use of `GetTickCount64()` on Windows 2000/XP.
 - Fixed opening DB on a network shares (in the exclusive mode).
 - Fixed copy&paste typos.
 - Fixed minor false-positive GCC warning.
 - Added workaround for broken `DEFINE_ENUM_FLAG_OPERATORS` from Windows SDK.
 - Fixed cursor state after multimap/dupsort repeated deletes (https://github.com/erthink/libmdbx/issues/121).
 - Added `SIGPIPE` suppression for internal thread during `mdbx_env_copy()`.
 - Fixed extra-rare `MDBX_KEY_EXIST` error during `mdbx_commit()` (https://github.com/erthink/libmdbx/issues/131).
 - Fixed spilled pages checking (https://github.com/erthink/libmdbx/issues/126).
 - Fixed `mdbx_load` for 'plain text' and without `-s name` cases (https://github.com/erthink/libmdbx/issues/136).
 - Fixed save/restore/commit of cursors for nested transactions.
 - Fixed cursors state in rare/special cases (move next beyond end-of-data, after deletion and so on).
 - Added workaround for MSVC 19.28 (Visual Studio 16.8) (but may still hang during compilation).
 - Fixed paranoidal Clang C++ UB for bitwise operations with flags defined by enums.
 - Fixed large pages checking (for compatibility and to avoid false-positive errors from `mdbx_chk`).
 - Added workaround for Wine (https://github.com/miranda-ng/miranda-ng/issues/1209).
 - Fixed `ERROR_NOT_SUPPORTED` while opening DB by UNC pathnames (https://github.com/miranda-ng/miranda-ng/issues/2627).
 - Added handling `EXCEPTION_POSSIBLE_DEADLOCK` condition for Windows.


## v0.9.1 2020-09-30

Added features:

 - Preliminary C++ API with support for C++17 polymorphic allocators.
 - [Online C++ API reference](https://erthink.github.io/libmdbx/) by Doxygen.
 - Quick reference for Insert/Update/Delete operations.
 - Explicit `MDBX_SYNC_DURABLE` to sync modes for API clarity.
 - Explicit `MDBX_ALLDUPS` and `MDBX_UPSERT` for API clarity.
 - Support for read transactions preparation (`MDBX_TXN_RDONLY_PREPARE` flag).
 - Support for cursor preparation/(pre)allocation and reusing (`mdbx_cursor_create()` and `mdbx_cursor_bind()` functions).
 - Support for checking database using specified meta-page (see `mdbx_chk -h`).
 - Support for turn to the specific meta-page after checking (see `mdbx_chk -h`).
 - Support for explicit reader threads (de)registration.
 - The `mdbx_txn_break()` function to explicitly mark a transaction as broken.
 - Improved handling of corrupted databases by `mdbx_chk` utility and `mdbx_walk_tree()` function.
 - Improved DB corruption detection by checking parent-page-txnid.
 - Improved opening large DB (> 4Gb) from 32-bit code.
 - Provided `pure-function` and `const-function` attributes to C API.
 - Support for user-settable context for transactions & cursors.
 - Revised API and documentation related to Handle-Slow-Readers callback feature.

Deprecated functions and flags:

 - For clarity and API simplification the `MDBX_MAPASYNC` flag is deprecated.
   Just use `MDBX_SAFE_NOSYNC` or `MDBX_UTTERLY_NOSYNC` instead of it.
 - `MDBX_oom_func`, `mdbx_env_set_oomfunc()` and `mdbx_env_get_oomfunc()`
   replaced with `MDBX_hsr_func`, `mdbx_env_get_hsr` and `mdbx_env_get_hsr()`.

Fixes:

 - Fix `mdbx_strerror()` for `MDBX_BUSY` error (no error description is returned).
 - Fix update internal meta-geo information in read-only mode (`EACCESS` or `EBADFD` error).
 - Fix `mdbx_page_get()` null-defer when DB corrupted (crash by `SIGSEGV`).
 - Fix `mdbx_env_open()` for re-opening after non-fatal errors (`mdbx_chk` unexpected failures).
 - Workaround for MSVC 19.27 `static_assert()` bug.
 - Doxygen descriptions and refinement.
 - Update Valgrind's suppressions.
 - Workaround to avoid infinite loop of 'nested' testcase on MIPS under QEMU.
 - Fix a lot of typos & spelling (Thanks to Josh Soref for PR).
 - Fix `getopt()` messages for Windows (Thanks to Andrey Sporaw for reporting).
 - Fix MSVC compiler version requirements (Thanks to Andrey Sporaw for reporting).
 - Workarounds for QEMU's bugs to run tests for cross-builded library under QEMU.
 - Now C++ compiler optional for building by CMake.


## v0.9.0 2020-07-31 (not a release, but API changes)

Added features:

 - [Online C API reference](https://erthink.github.io/libmdbx/) by Doxygen.
 - Separated enums for environment, sub-databases, transactions, copying and data-update flags.

Deprecated functions and flags:

 - Usage of custom comparators and the `mdbx_dbi_open_ex()` are deprecated, since such databases couldn't be checked by the `mdbx_chk` utility.
   Please use the value-to-key functions to provide keys that are compatible with the built-in libmdbx comparators.


## v0.8.2 2020-07-06
- Added support multi-opening the same DB in a process with SysV locking (BSD).
- Fixed warnings & minors for LCC compiler (E2K).
- Enabled to simultaneously open the same database from processes with and without the `MDBX_WRITEMAP` option.
- Added key-to-value, `mdbx_get_keycmp()` and `mdbx_get_datacmp()` functions (helpful to avoid using custom comparators).
- Added `ENABLE_UBSAN` CMake option to enabling the UndefinedBehaviorSanitizer from GCC/CLANG.
- Workaround for [CLANG bug](https://bugs.llvm.org/show_bug.cgi?id=43275).
- Returning `MDBX_CORRUPTED` in case all meta-pages are weak and no other error.
- Refined mode bits while auto-creating LCK-file.
- Avoids unnecessary database file re-mapping in case geometry changed by another process(es).
  From the user's point of view, the `MDBX_UNABLE_EXTEND_MAPSIZE` error will now be returned less frequently and only when using the DB in the current process really requires it to be reopened.
- Remapping on-the-fly and of the database file was implemented.
  Now remapping with a change of address is performed automatically if there are no dependent readers in the current process.


## v0.8.1 2020-06-12
- Minor change versioning. The last number in the version now means the number of commits since last release/tag.
- Provide ChangeLog file.
- Fix for using libmdbx as a C-only sub-project with CMake.
- Fix `mdbx_env_set_geometry()` for case it is called from an opened environment outside of a write transaction.
- Add support for huge transactions and `MDBX_HUGE_TRANSACTIONS` build-option (default `OFF`).
- Refine LTO (link time optimization) for clang.
- Force enabling exceptions handling for MSVC (`/EHsc` option).


## v0.8.0 2020-06-05
- Support for Android/Bionic.
- Support for iOS.
- Auto-handling `MDBX_NOSUBDIR` while opening for any existing database.
- Engage github-actions to make release-assets.
- Clarify API description.
- Extended keygen-cases in stochastic test.
- Fix fetching of first/lower key from LEAF2-page during page merge.
- Fix missing comma in array of error messages.
- Fix div-by-zero while copy-with-compaction for non-resizable environments.
- Fixes & enhancements for custom-comparators.
- Fix `MDBX_AVOID_CRT` option and missing `ntdll.def`.
- Fix `mdbx_env_close()` to work correctly called concurrently from several threads.
- Fix null-deref in an ASAN-enabled builds while opening the environment with error and/or read-only.
- Fix AddressSanitizer errors after closing the environment.
- Fix/workaround to avoid GCC 10.x pedantic warnings.
- Fix using `ENODATA` for FreeBSD.
- Avoid invalidation of DBI-handle(s) when it just closes.
- Avoid using `pwritev()` for single-writes (up to 10% speedup for some kernels & scenarios).
- Avoiding `MDBX_UTTERLY_NOSYNC` as result of flags merge.
- Add `mdbx_dbi_dupsort_depthmask()` function.
- Add `MDBX_CP_FORCE_RESIZEABLE` option.
- Add deprecated `MDBX_MAP_RESIZED` for compatibility.
- Add `MDBX_BUILD_TOOLS` option (default `ON`).
- Refine `mdbx_dbi_open_ex()` to safe concurrently opening the same handle from different threads.
- Truncate clk-file during environment closing. So a zero-length lck-file indicates that the environment was closed properly.
- Refine `mdbx_update_gc()` for huge transactions with small sizes of database page.
- Extends dump/load to support all MDBX attributes.
- Avoid upsertion the same key-value data, fix related assertions.
- Rework min/max length checking for keys & values.
- Checking the order of keys on all pages during checking.
- Support `CFLAGS_EXTRA` make-option for convenience.
- Preserve the last txnid while copying with compactification.
- Auto-reset running transaction in mdbx_txn_renew().
- Automatically abort errored transaction in mdbx_txn_commit().
- Auto-choose page size for large databases.
- Rearrange source files, rework build, options-support by CMake.
- Crutch for WSL1 (Windows subsystem for Linux).
- Refine install/uninstall targets.
- Support for Valgrind 3.14 and later.
- Add check-analyzer check-ubsan check-asan check-leak targets to Makefile.
- Minor fix/workaround to avoid UBSAN traps for `memcpy(ptr, NULL, 0)`.
- Avoid some GCC-analyzer false-positive warnings.


## v0.7.0 2020-03-18
- Workarounds for Wine (Windows compatibility layer for Linux).
- `MDBX_MAP_RESIZED` renamed to `MDBX_UNABLE_EXTEND_MAPSIZE`.
- Clarify API description, fix typos.
- Speedup runtime checks in debug/checked builds.
- Added checking for read/write transactions overlapping for the same thread, added `MDBX_TXN_OVERLAPPING` error and `MDBX_DBG_LEGACY_OVERLAP` option.
- Added `mdbx_key_from_jsonInteger()`, `mdbx_key_from_double()`, `mdbx_key_from_float()`, `mdbx_key_from_int64()` and `mdbx_key_from_int32()` functions. See `mdbx.h` for description.
- Fix compatibility (use zero for invalid DBI).
- Refine/clarify error messages.
- Avoids extra error messages "bad txn" from mdbx_chk when DB is corrupted.


## v0.6.0 2020-01-21
- Fix `mdbx_load` utility for custom comparators.
- Fix checks related to `MDBX_APPEND` flag inside `mdbx_cursor_put()`.
- Refine/fix dbi_bind() internals.
- Refine/fix handling `STATUS_CONFLICTING_ADDRESSES`.
- Rework `MDBX_DBG_DUMP` option to avoid disk I/O performance degradation.
- Add built-in help to test tool.
- Fix `mdbx_env_set_geometry()` for large page size.
- Fix env_set_geometry() for large pagesize.
- Clarify API description & comments, fix typos.


## v0.5.0 2019-12-31
- Fix returning MDBX_RESULT_TRUE from page_alloc().
- Fix false-positive ASAN issue.
- Fix assertion for `MDBX_NOTLS` option.
- Rework `MADV_DONTNEED` threshold.
- Fix `mdbx_chk` utility for don't checking some numbers if walking on the B-tree was disabled.
- Use page's mp_txnid for basic integrity checking.
- Add `MDBX_FORCE_ASSERTIONS` built-time option.
- Rework `MDBX_DBG_DUMP` to avoid performance degradation.
- Rename `MDBX_NOSYNC` to `MDBX_SAFE_NOSYNC` for clarity.
- Interpret `ERROR_ACCESS_DENIED` from `OpenProcess()` as 'process exists'.
- Avoid using `FILE_FLAG_NO_BUFFERING` for compatibility with small database pages.
- Added install section for CMake.


## v0.4.0 2019-12-02
- Support for Mac OSX, FreeBSD, NetBSD, OpenBSD, DragonFly BSD, OpenSolaris, OpenIndiana (AIX and HP-UX pending).
- Use bootid for decisions of rollback.
- Counting retired pages and extended transaction info.
- Add `MDBX_ACCEDE` flag for database opening.
- Using OFD-locks and tracking for in-process multi-opening.
- Hot backup into pipe.
- Support for cmake & amalgamated sources.
- Fastest internal sort implementation.
- New internal dirty-list implementation with lazy sorting.
- Support for lazy-sync-to-disk with polling.
- Extended key length.
- Last update transaction number for each sub-database.
- Automatic read ahead enabling/disabling.
- More auto-compactification.
- Using -fsanitize=undefined and -Wpedantic options.
- Rework page merging.
- Nested transactions.
- API description.
- Checking for non-local filesystems to avoid DB corruption.
