package iter

// Dual - Iterator-like interface designed for grpc server-side streaming: 1 client request -> much responses from server
//   - K, V are valid only until next .Next() call (TODO: extend it to whole Tx lifetime?)
//   - No `Close` method: all streams produced by TemporalTx will be closed inside `tx.Rollback()` (by casting to `kv.Closer`)
//   - automatically checks cancelation of `ctx` passed to `db.Begin(ctx)`, can skip this
//     check in loops on stream. Dual has very limited API - user has no way to
//     terminate it - but user can specify more strict conditions when creating stream (then server knows better when to stop)
type Dual[K, V any] interface {
	Next() (K, V, error)
	HasNext() bool
}
type Unary[V any] interface {
	Next() (V, error)
	//NextBatch() ([]V, error)
	HasNext() bool
}
type U64 interface {
	Unary[uint64]
}

// KV - stream which return 2 items - usually called Key and Value (or `k` and `v`)
// Example:
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type KV Dual[[]byte, []byte]
