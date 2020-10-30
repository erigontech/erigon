package snapshotsync

// generate the messages
//go:generate protoc --go_out=. "./external_downloader.proto" -I=. -I=./../../build/include/google

// generate the services
//go:generate protoc --go-grpc_out=. "./external_downloader.proto" -I=. -I=./../../build/include/google


const (
	HeadersSnapshotName        = "headers"
	BodiesSnapshotName         = "bodies"
	StateSnapshotName          = "state"
	ReceiptsSnapshotName       = "receipts"
)

var (
	SnapshotNames = map[SnapshotType]string {
		SnapshotType_Headers: HeadersSnapshotName,
		SnapshotType_Bodies: BodiesSnapshotName,
		SnapshotType_State: StateSnapshotName,
		SnapshotType_Receipts: ReceiptsSnapshotName,
	}
)