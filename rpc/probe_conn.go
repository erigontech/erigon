package rpc

// ProbeConnKey returns a key per connection, not for merge.
func ProbeConnKey(n Notifier) any {
	if rn, ok := n.(*RemoteNotifier); ok {
		return rn.h.conn
	}
	return n
}
