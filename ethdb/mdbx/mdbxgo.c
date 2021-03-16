/* lmdbgo.c
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb
 * */
#include <string.h>
#include <stdio.h>
#include "_cgo_export.h"
#include "mdbxgo.h"
#include "dist/mdbx.h"

#define MDBXGO_SET_VAL(val, size, data) \
    *(val) = (MDBX_val){.iov_len = (size), .iov_base = (data)}

int mdbxgo_msg_func_proxy(const char *msg, void *ctx) {
    //  wrap msg and call the bridge function exported from lmdb.go.
    mdbxgo_ConstCString s;
    s.p = msg;
    return mdbxgoMDBMsgFuncBridge(s, (size_t)ctx);
}

//int mdbxgo_reader_list(MDBX_env *env, size_t ctx) {
//    // list readers using a static proxy function that does dynamic dispatch on
//    // ctx.
//	if (ctx)
//		return mdbx_reader_list(env, &mdbxgo_msg_func_proxy, (void *)ctx);
//	return mdbx_reader_list(env, 0, (void *)ctx);
//}

int mdbxgo_del(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn) {
    MDBX_val key, val;
    MDBXGO_SET_VAL(&key, kn, kdata);
    if (vdata) {
        MDBXGO_SET_VAL(&val, vn, vdata);
        return mdbx_del(txn, dbi, &key, &val);
    }
    return mdbx_del(txn, dbi, &key, NULL);
}

int mdbxgo_get(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, MDBX_val *val) {
    MDBX_val key;
    MDBXGO_SET_VAL(&key, kn, kdata);
    return mdbx_get(txn, dbi, &key, val);
}

int mdbxgo_put2(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_put_flags_t flags) {
    MDBX_val key, val;
    MDBXGO_SET_VAL(&key, kn, kdata);
    MDBXGO_SET_VAL(&val, vn, vdata);
    return mdbx_put(txn, dbi, &key, &val, flags);
}

int mdbxgo_put1(MDBX_txn *txn, MDBX_dbi dbi, char *kdata, size_t kn, MDBX_val *val, MDBX_put_flags_t flags) {
    MDBX_val key;
    MDBXGO_SET_VAL(&key, kn, kdata);
    return mdbx_put(txn, dbi, &key, val, flags);
}

int mdbxgo_cursor_put2(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_put_flags_t flags) {
    MDBX_val key, val;
    MDBXGO_SET_VAL(&key, kn, kdata);
    MDBXGO_SET_VAL(&val, vn, vdata);
    return mdbx_cursor_put(cur, &key, &val, flags);
}

int mdbxgo_cursor_put1(MDBX_cursor *cur, char *kdata, size_t kn, MDBX_val *val, MDBX_put_flags_t flags) {
    MDBX_val key;
    MDBXGO_SET_VAL(&key, kn, kdata);
    return mdbx_cursor_put(cur, &key, val, flags);
}

int mdbxgo_cursor_putmulti(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, size_t vstride, MDBX_put_flags_t flags) {
    MDBX_val key, val[2];
    MDBXGO_SET_VAL(&key, kn, kdata);
    MDBXGO_SET_VAL(&(val[0]), vstride, vdata);
    MDBXGO_SET_VAL(&(val[1]), vn, 0);
    return mdbx_cursor_put(cur, &key, &val[0], flags);
}

int mdbxgo_cursor_get1(MDBX_cursor *cur, char *kdata, size_t kn, MDBX_val *key, MDBX_val *val, MDBX_cursor_op op) {
    MDBXGO_SET_VAL(key, kn, kdata);
    return mdbx_cursor_get(cur, key, val, op);
}

int mdbxgo_cursor_get2(MDBX_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDBX_val *key, MDBX_val *val, MDBX_cursor_op op) {
    MDBXGO_SET_VAL(key, kn, kdata);
    MDBXGO_SET_VAL(val, vn, vdata);
    return mdbx_cursor_get(cur, key, val, op);
}

/* Compare two items lexically */
//static int __hot cmp_lexical(const MDBX_val *a, const MDBX_val *b) {
//  if (a->iov_len == b->iov_len)
//    return memcmp(a->iov_base, b->iov_base, a->iov_len);
//
//  const int diff_len = (a->iov_len < b->iov_len) ? -1 : 1;
//  const size_t shortest = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
//  int diff_data = memcmp(a->iov_base, b->iov_base, shortest);
//  return likely(diff_data) ? diff_data : diff_len;
//}

static int mdbxgo_dup_cmp_exclude_suffix32(const MDBX_val *a, const MDBX_val *b) {
	int diff;
	ssize_t len_diff;
	unsigned int len;
	unsigned int lenA;
	unsigned int lenB;

    lenA = a->iov_len >= 32 ? a->iov_len - 32 : a->iov_len;
	lenB = b->iov_len >= 32 ? b->iov_len - 32 : b->iov_len;
    len = lenA;
	len_diff = (ssize_t) lenA - (ssize_t) lenB;
	if (len_diff > 0) {
		len = lenB;
		len_diff = 1;
	}

	diff = memcmp(a->iov_base, b->iov_base, len);
	return diff ? diff : len_diff<0 ? -1 : len_diff;
}

MDBX_cmp_func *mdbxgo_get_cmp_exclude_suffix32() {
  return mdbxgo_dup_cmp_exclude_suffix32;
}

int mdbxgo_cmp(MDBX_txn *txn, MDBX_dbi dbi, char *adata, size_t an, char *bdata, size_t bn) {
    MDBX_val a;
    MDBXGO_SET_VAL(&a, an, adata);
    MDBX_val b;
    MDBXGO_SET_VAL(&b, bn, bdata);
    return mdbx_cmp(txn, dbi, &a, &b);
}

int mdbxgo_dcmp(MDBX_txn *txn, MDBX_dbi dbi, char *adata, size_t an, char *bdata, size_t bn) {
    MDBX_val a;
    MDBXGO_SET_VAL(&a, an, adata);
    MDBX_val b;
    MDBXGO_SET_VAL(&b, bn, bdata);
    return mdbx_dcmp(txn, dbi, &a, &b);
}




