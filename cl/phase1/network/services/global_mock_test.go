package services

import (
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"go.uber.org/mock/gomock"
)

type mockFuncs struct {
	ctrl *gomock.Controller
}

func (m *mockFuncs) ComputeSigningRoot(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComputeSigningRoot", obj, domain)
	ret0, _ := ret[0].([32]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *mockFuncs) BlsVerify(pubkey, message, signature []byte) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlsVerify", pubkey, message, signature)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}
