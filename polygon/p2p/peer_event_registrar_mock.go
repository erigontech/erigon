// Code generated by MockGen. DO NOT EDIT.
// Source: ./peer_event_registrar.go
//
// Generated by this command:
//
//	mockgen -typed=true -source=./peer_event_registrar.go -destination=./peer_event_registrar_mock.go -package=p2p
//

// Package p2p is a generated GoMock package.
package p2p

import (
	reflect "reflect"

	event "github.com/erigontech/erigon-lib/event"
	sentryproto "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	eth "github.com/erigontech/erigon-p2p/protocols/eth"
	gomock "go.uber.org/mock/gomock"
)

// MockpeerEventRegistrar is a mock of peerEventRegistrar interface.
type MockpeerEventRegistrar struct {
	ctrl     *gomock.Controller
	recorder *MockpeerEventRegistrarMockRecorder
	isgomock struct{}
}

// MockpeerEventRegistrarMockRecorder is the mock recorder for MockpeerEventRegistrar.
type MockpeerEventRegistrarMockRecorder struct {
	mock *MockpeerEventRegistrar
}

// NewMockpeerEventRegistrar creates a new mock instance.
func NewMockpeerEventRegistrar(ctrl *gomock.Controller) *MockpeerEventRegistrar {
	mock := &MockpeerEventRegistrar{ctrl: ctrl}
	mock.recorder = &MockpeerEventRegistrarMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockpeerEventRegistrar) EXPECT() *MockpeerEventRegistrarMockRecorder {
	return m.recorder
}

// RegisterNewBlockHashesObserver mocks base method.
func (m *MockpeerEventRegistrar) RegisterNewBlockHashesObserver(observer event.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterNewBlockHashesObserver", observer)
	ret0, _ := ret[0].(UnregisterFunc)
	return ret0
}

// RegisterNewBlockHashesObserver indicates an expected call of RegisterNewBlockHashesObserver.
func (mr *MockpeerEventRegistrarMockRecorder) RegisterNewBlockHashesObserver(observer any) *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterNewBlockHashesObserver", reflect.TypeOf((*MockpeerEventRegistrar)(nil).RegisterNewBlockHashesObserver), observer)
	return &MockpeerEventRegistrarRegisterNewBlockHashesObserverCall{Call: call}
}

// MockpeerEventRegistrarRegisterNewBlockHashesObserverCall wrap *gomock.Call
type MockpeerEventRegistrarRegisterNewBlockHashesObserverCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall) Return(arg0 UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall) Do(f func(event.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall) DoAndReturn(f func(event.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockHashesObserverCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// RegisterNewBlockObserver mocks base method.
func (m *MockpeerEventRegistrar) RegisterNewBlockObserver(observer event.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterNewBlockObserver", observer)
	ret0, _ := ret[0].(UnregisterFunc)
	return ret0
}

// RegisterNewBlockObserver indicates an expected call of RegisterNewBlockObserver.
func (mr *MockpeerEventRegistrarMockRecorder) RegisterNewBlockObserver(observer any) *MockpeerEventRegistrarRegisterNewBlockObserverCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterNewBlockObserver", reflect.TypeOf((*MockpeerEventRegistrar)(nil).RegisterNewBlockObserver), observer)
	return &MockpeerEventRegistrarRegisterNewBlockObserverCall{Call: call}
}

// MockpeerEventRegistrarRegisterNewBlockObserverCall wrap *gomock.Call
type MockpeerEventRegistrarRegisterNewBlockObserverCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockpeerEventRegistrarRegisterNewBlockObserverCall) Return(arg0 UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockObserverCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockpeerEventRegistrarRegisterNewBlockObserverCall) Do(f func(event.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockObserverCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockpeerEventRegistrarRegisterNewBlockObserverCall) DoAndReturn(f func(event.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) UnregisterFunc) *MockpeerEventRegistrarRegisterNewBlockObserverCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// RegisterPeerEventObserver mocks base method.
func (m *MockpeerEventRegistrar) RegisterPeerEventObserver(observer event.Observer[*sentryproto.PeerEvent]) UnregisterFunc {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterPeerEventObserver", observer)
	ret0, _ := ret[0].(UnregisterFunc)
	return ret0
}

// RegisterPeerEventObserver indicates an expected call of RegisterPeerEventObserver.
func (mr *MockpeerEventRegistrarMockRecorder) RegisterPeerEventObserver(observer any) *MockpeerEventRegistrarRegisterPeerEventObserverCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterPeerEventObserver", reflect.TypeOf((*MockpeerEventRegistrar)(nil).RegisterPeerEventObserver), observer)
	return &MockpeerEventRegistrarRegisterPeerEventObserverCall{Call: call}
}

// MockpeerEventRegistrarRegisterPeerEventObserverCall wrap *gomock.Call
type MockpeerEventRegistrarRegisterPeerEventObserverCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockpeerEventRegistrarRegisterPeerEventObserverCall) Return(arg0 UnregisterFunc) *MockpeerEventRegistrarRegisterPeerEventObserverCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockpeerEventRegistrarRegisterPeerEventObserverCall) Do(f func(event.Observer[*sentryproto.PeerEvent]) UnregisterFunc) *MockpeerEventRegistrarRegisterPeerEventObserverCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockpeerEventRegistrarRegisterPeerEventObserverCall) DoAndReturn(f func(event.Observer[*sentryproto.PeerEvent]) UnregisterFunc) *MockpeerEventRegistrarRegisterPeerEventObserverCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
