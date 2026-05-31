package state

// RecordAccesses is a vio-restructure shim used only by execution/tests/blockgen
// chain_makers.go to record test-only access tracking. No-op for typed-vio.
func (v *VersionedIO) RecordAccesses(_ Version, _ AccessSet) {}
