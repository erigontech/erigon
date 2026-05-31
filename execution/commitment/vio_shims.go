package commitment

// Shims used by the typed-vio IBS / stateObject. No-op so the version-io
// shape compiles independently of the (separate) commitment-metrics landing.

func RecordSstoreInsert()   {}
func RecordSstoreUpdate()   {}
func RecordSstoreDelete()   {}
func RecordSstoreNoop()     {}
func RecordHasStorageMiss() {}
