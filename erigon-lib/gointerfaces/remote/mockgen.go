package remote

//go:generate mockgen -typed=true -destination=./kv_client_mock.go -package=remote . KVClient
//go:generate mockgen -typed=true -destination=./kv_state_changes_client_mock.go -package=remote . KV_StateChangesClient
