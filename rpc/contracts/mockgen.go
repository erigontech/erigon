package contracts

import "github.com/erigontech/erigon/execution/abi/bind"

//go:generate mockgen -typed=true -destination=./backend_mock.go -package=contracts . Backend
type Backend interface {
	bind.ContractBackend
}
