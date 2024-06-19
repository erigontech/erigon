package remoteproto

//go:generate mockgen -typed=true -destination=./kv_client_mock.go -package=remoteproto . KVClient
//go:generate mockgen -typed=true -destination=./kv_state_changes_client_mock.go -package=remoteproto . KV_StateChangesClient
