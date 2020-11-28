/* lmdbgo.h
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb.  These functions have
 * no compatibility guarantees and may be modified or deleted without warning.
 * */
#ifndef _MDBXGO_H_
#define _MDBXGO_H_

#include "dist/mdbx.h"

/* Proxy functions for lmdb get/put operations. The functions are defined to
 * take char* values instead of void* to keep cgo from cheking their data for
 * nested pointers and causing a couple of allocations per argument.
 *
 * For more information see github issues for more information about the
 * problem and the decision.
 *      https://github.com/golang/go/issues/14387
 *      https://github.com/golang/go/issues/15048
 *      https://github.com/bmatsuo/lmdb-go/issues/63
 * */
int mdbxgo_del(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn);
int mdbxgo_get(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, MDBX_val *val);
int mdbxgo_put1(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, MDBX_val *val, MDBX_put_flags_t flags);
int mdbxgo_put2(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_put_flags_t flags);
int mdbxgo_cursor_put1(MDBX_cursor *cur, char *kdata, size_t kn, MDBX_val *val, MDBX_put_flags_t flags);
int mdbxgo_cursor_put2(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_put_flags_t flags);
int mdbxgo_cursor_putmulti(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, size_t vstride, MDBX_put_flags_t flags);
int mdbxgo_cursor_get1(MDBX_cursor *cur, char *kdata, size_t kn, MDBX_val *key, MDBX_val *val, MDBX_cursor_op op);
int mdbxgo_cursor_get2(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_val *key, MDBX_val *val, MDBX_cursor_op op);

/* ConstCString wraps a null-terminated (const char *) because Go's type system
 * does not represent the 'cosnt' qualifier directly on a function argument and
 * causes warnings to be emitted during linking.
 * */
typedef struct{ const char *p; } mdbxgo_ConstCString;

/* mdbxgo_reader_list is a proxy for mdb_reader_list that uses a special
 * mdb_msg_func proxy function to relay messages over the
 * mdbxgo_reader_list_bridge external Go func.
 * */
int mdbxgo_reader_list(MDBX_env *env, size_t ctx);


int mdbxgo_set_dupsort_cmp_exclude_suffix32(MDBX_txn *txn, MDBX_dbi dbi);
int mdbxgo_cmp(MDBX_txn *txn, MDBX_dbi dbi, char *adata, size_t an, char *bdata, size_t bn);
int mdbxgo_dcmp(MDBX_txn *txn, MDBX_dbi dbi, char *adata, size_t an, char *bdata, size_t bn);

MDBX_cmp_func *mdbxgo_get_cmp_exclude_suffix32();

MDBX_debug_func *mdbxgo_stderr_logger();

#endif
