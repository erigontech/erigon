//+build !mdbx

package ethdb

func NewMDBX() LmdbOpts {
	panic("to use MDBX, compile with -tags 'mdbx'")
}
