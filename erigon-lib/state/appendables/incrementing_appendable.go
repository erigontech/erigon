package appendables

type IncrementingAppendable struct {
	*ProtoAppendable
	valsTbl string
}