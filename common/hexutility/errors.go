package hexutility

var (
	ErrMissingPrefix = &decError{"hex string without 0x prefix"}
	ErrOddLength     = &decError{"hex string of odd length"}
	ErrSyntax        = &decError{"invalid hex string"}
)

type decError struct{ msg string }

func (err decError) Error() string { return err.msg }
