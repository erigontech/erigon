package snapshotsync

// generate the messages
//go:generate protoc --go_out=. "./sndownloader.proto" -I=. -I=./../../build/include/google

// generate the services
//go:generate protoc --go-grpc_out=. "./sndownloader.proto" -I=. -I=./../../build/include/google


const (
	HeadersSnapshotName        = "headers"
	BodiesSnapshotName         = "bodies"
	StateSnapshotName          = "state"
	ReceiptsSnapshotName       = "receipts"
)