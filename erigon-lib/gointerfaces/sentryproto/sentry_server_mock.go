// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/erigontech/erigon-lib/gointerfaces/sentryproto (interfaces: SentryServer)
//
// Generated by this command:
//
//	mockgen -typed=true -destination=./sentry_server_mock.go -package=sentryproto . SentryServer
//

// Package sentryproto is a generated GoMock package.
package sentryproto

import (
	context "context"
	reflect "reflect"

	typesproto "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	gomock "go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// MockSentryServer is a mock of SentryServer interface.
type MockSentryServer struct {
	ctrl     *gomock.Controller
	recorder *MockSentryServerMockRecorder
	isgomock struct{}
}

// MockSentryServerMockRecorder is the mock recorder for MockSentryServer.
type MockSentryServerMockRecorder struct {
	mock *MockSentryServer
}

// NewMockSentryServer creates a new mock instance.
func NewMockSentryServer(ctrl *gomock.Controller) *MockSentryServer {
	mock := &MockSentryServer{ctrl: ctrl}
	mock.recorder = &MockSentryServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSentryServer) EXPECT() *MockSentryServerMockRecorder {
	return m.recorder
}

// AddPeer mocks base method.
func (m *MockSentryServer) AddPeer(arg0 context.Context, arg1 *AddPeerRequest) (*AddPeerReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddPeer", arg0, arg1)
	ret0, _ := ret[0].(*AddPeerReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddPeer indicates an expected call of AddPeer.
func (mr *MockSentryServerMockRecorder) AddPeer(arg0, arg1 any) *MockSentryServerAddPeerCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddPeer", reflect.TypeOf((*MockSentryServer)(nil).AddPeer), arg0, arg1)
	return &MockSentryServerAddPeerCall{Call: call}
}

// MockSentryServerAddPeerCall wrap *gomock.Call
type MockSentryServerAddPeerCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerAddPeerCall) Return(arg0 *AddPeerReply, arg1 error) *MockSentryServerAddPeerCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerAddPeerCall) Do(f func(context.Context, *AddPeerRequest) (*AddPeerReply, error)) *MockSentryServerAddPeerCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerAddPeerCall) DoAndReturn(f func(context.Context, *AddPeerRequest) (*AddPeerReply, error)) *MockSentryServerAddPeerCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// HandShake mocks base method.
func (m *MockSentryServer) HandShake(arg0 context.Context, arg1 *emptypb.Empty) (*HandShakeReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandShake", arg0, arg1)
	ret0, _ := ret[0].(*HandShakeReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandShake indicates an expected call of HandShake.
func (mr *MockSentryServerMockRecorder) HandShake(arg0, arg1 any) *MockSentryServerHandShakeCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandShake", reflect.TypeOf((*MockSentryServer)(nil).HandShake), arg0, arg1)
	return &MockSentryServerHandShakeCall{Call: call}
}

// MockSentryServerHandShakeCall wrap *gomock.Call
type MockSentryServerHandShakeCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerHandShakeCall) Return(arg0 *HandShakeReply, arg1 error) *MockSentryServerHandShakeCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerHandShakeCall) Do(f func(context.Context, *emptypb.Empty) (*HandShakeReply, error)) *MockSentryServerHandShakeCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerHandShakeCall) DoAndReturn(f func(context.Context, *emptypb.Empty) (*HandShakeReply, error)) *MockSentryServerHandShakeCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Messages mocks base method.
func (m *MockSentryServer) Messages(arg0 *MessagesRequest, arg1 grpc.ServerStreamingServer[InboundMessage]) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Messages", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Messages indicates an expected call of Messages.
func (mr *MockSentryServerMockRecorder) Messages(arg0, arg1 any) *MockSentryServerMessagesCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Messages", reflect.TypeOf((*MockSentryServer)(nil).Messages), arg0, arg1)
	return &MockSentryServerMessagesCall{Call: call}
}

// MockSentryServerMessagesCall wrap *gomock.Call
type MockSentryServerMessagesCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerMessagesCall) Return(arg0 error) *MockSentryServerMessagesCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerMessagesCall) Do(f func(*MessagesRequest, grpc.ServerStreamingServer[InboundMessage]) error) *MockSentryServerMessagesCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerMessagesCall) DoAndReturn(f func(*MessagesRequest, grpc.ServerStreamingServer[InboundMessage]) error) *MockSentryServerMessagesCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// NodeInfo mocks base method.
func (m *MockSentryServer) NodeInfo(arg0 context.Context, arg1 *emptypb.Empty) (*typesproto.NodeInfoReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NodeInfo", arg0, arg1)
	ret0, _ := ret[0].(*typesproto.NodeInfoReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NodeInfo indicates an expected call of NodeInfo.
func (mr *MockSentryServerMockRecorder) NodeInfo(arg0, arg1 any) *MockSentryServerNodeInfoCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NodeInfo", reflect.TypeOf((*MockSentryServer)(nil).NodeInfo), arg0, arg1)
	return &MockSentryServerNodeInfoCall{Call: call}
}

// MockSentryServerNodeInfoCall wrap *gomock.Call
type MockSentryServerNodeInfoCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerNodeInfoCall) Return(arg0 *typesproto.NodeInfoReply, arg1 error) *MockSentryServerNodeInfoCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerNodeInfoCall) Do(f func(context.Context, *emptypb.Empty) (*typesproto.NodeInfoReply, error)) *MockSentryServerNodeInfoCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerNodeInfoCall) DoAndReturn(f func(context.Context, *emptypb.Empty) (*typesproto.NodeInfoReply, error)) *MockSentryServerNodeInfoCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// PeerById mocks base method.
func (m *MockSentryServer) PeerById(arg0 context.Context, arg1 *PeerByIdRequest) (*PeerByIdReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PeerById", arg0, arg1)
	ret0, _ := ret[0].(*PeerByIdReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PeerById indicates an expected call of PeerById.
func (mr *MockSentryServerMockRecorder) PeerById(arg0, arg1 any) *MockSentryServerPeerByIdCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PeerById", reflect.TypeOf((*MockSentryServer)(nil).PeerById), arg0, arg1)
	return &MockSentryServerPeerByIdCall{Call: call}
}

// MockSentryServerPeerByIdCall wrap *gomock.Call
type MockSentryServerPeerByIdCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPeerByIdCall) Return(arg0 *PeerByIdReply, arg1 error) *MockSentryServerPeerByIdCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPeerByIdCall) Do(f func(context.Context, *PeerByIdRequest) (*PeerByIdReply, error)) *MockSentryServerPeerByIdCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPeerByIdCall) DoAndReturn(f func(context.Context, *PeerByIdRequest) (*PeerByIdReply, error)) *MockSentryServerPeerByIdCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// PeerCount mocks base method.
func (m *MockSentryServer) PeerCount(arg0 context.Context, arg1 *PeerCountRequest) (*PeerCountReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PeerCount", arg0, arg1)
	ret0, _ := ret[0].(*PeerCountReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PeerCount indicates an expected call of PeerCount.
func (mr *MockSentryServerMockRecorder) PeerCount(arg0, arg1 any) *MockSentryServerPeerCountCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PeerCount", reflect.TypeOf((*MockSentryServer)(nil).PeerCount), arg0, arg1)
	return &MockSentryServerPeerCountCall{Call: call}
}

// MockSentryServerPeerCountCall wrap *gomock.Call
type MockSentryServerPeerCountCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPeerCountCall) Return(arg0 *PeerCountReply, arg1 error) *MockSentryServerPeerCountCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPeerCountCall) Do(f func(context.Context, *PeerCountRequest) (*PeerCountReply, error)) *MockSentryServerPeerCountCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPeerCountCall) DoAndReturn(f func(context.Context, *PeerCountRequest) (*PeerCountReply, error)) *MockSentryServerPeerCountCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// PeerEvents mocks base method.
func (m *MockSentryServer) PeerEvents(arg0 *PeerEventsRequest, arg1 grpc.ServerStreamingServer[PeerEvent]) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PeerEvents", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PeerEvents indicates an expected call of PeerEvents.
func (mr *MockSentryServerMockRecorder) PeerEvents(arg0, arg1 any) *MockSentryServerPeerEventsCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PeerEvents", reflect.TypeOf((*MockSentryServer)(nil).PeerEvents), arg0, arg1)
	return &MockSentryServerPeerEventsCall{Call: call}
}

// MockSentryServerPeerEventsCall wrap *gomock.Call
type MockSentryServerPeerEventsCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPeerEventsCall) Return(arg0 error) *MockSentryServerPeerEventsCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPeerEventsCall) Do(f func(*PeerEventsRequest, grpc.ServerStreamingServer[PeerEvent]) error) *MockSentryServerPeerEventsCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPeerEventsCall) DoAndReturn(f func(*PeerEventsRequest, grpc.ServerStreamingServer[PeerEvent]) error) *MockSentryServerPeerEventsCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// PeerMinBlock mocks base method.
func (m *MockSentryServer) PeerMinBlock(arg0 context.Context, arg1 *PeerMinBlockRequest) (*emptypb.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PeerMinBlock", arg0, arg1)
	ret0, _ := ret[0].(*emptypb.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PeerMinBlock indicates an expected call of PeerMinBlock.
func (mr *MockSentryServerMockRecorder) PeerMinBlock(arg0, arg1 any) *MockSentryServerPeerMinBlockCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PeerMinBlock", reflect.TypeOf((*MockSentryServer)(nil).PeerMinBlock), arg0, arg1)
	return &MockSentryServerPeerMinBlockCall{Call: call}
}

// MockSentryServerPeerMinBlockCall wrap *gomock.Call
type MockSentryServerPeerMinBlockCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPeerMinBlockCall) Return(arg0 *emptypb.Empty, arg1 error) *MockSentryServerPeerMinBlockCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPeerMinBlockCall) Do(f func(context.Context, *PeerMinBlockRequest) (*emptypb.Empty, error)) *MockSentryServerPeerMinBlockCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPeerMinBlockCall) DoAndReturn(f func(context.Context, *PeerMinBlockRequest) (*emptypb.Empty, error)) *MockSentryServerPeerMinBlockCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Peers mocks base method.
func (m *MockSentryServer) Peers(arg0 context.Context, arg1 *emptypb.Empty) (*PeersReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Peers", arg0, arg1)
	ret0, _ := ret[0].(*PeersReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Peers indicates an expected call of Peers.
func (mr *MockSentryServerMockRecorder) Peers(arg0, arg1 any) *MockSentryServerPeersCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Peers", reflect.TypeOf((*MockSentryServer)(nil).Peers), arg0, arg1)
	return &MockSentryServerPeersCall{Call: call}
}

// MockSentryServerPeersCall wrap *gomock.Call
type MockSentryServerPeersCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPeersCall) Return(arg0 *PeersReply, arg1 error) *MockSentryServerPeersCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPeersCall) Do(f func(context.Context, *emptypb.Empty) (*PeersReply, error)) *MockSentryServerPeersCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPeersCall) DoAndReturn(f func(context.Context, *emptypb.Empty) (*PeersReply, error)) *MockSentryServerPeersCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// PenalizePeer mocks base method.
func (m *MockSentryServer) PenalizePeer(arg0 context.Context, arg1 *PenalizePeerRequest) (*emptypb.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PenalizePeer", arg0, arg1)
	ret0, _ := ret[0].(*emptypb.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PenalizePeer indicates an expected call of PenalizePeer.
func (mr *MockSentryServerMockRecorder) PenalizePeer(arg0, arg1 any) *MockSentryServerPenalizePeerCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PenalizePeer", reflect.TypeOf((*MockSentryServer)(nil).PenalizePeer), arg0, arg1)
	return &MockSentryServerPenalizePeerCall{Call: call}
}

// MockSentryServerPenalizePeerCall wrap *gomock.Call
type MockSentryServerPenalizePeerCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerPenalizePeerCall) Return(arg0 *emptypb.Empty, arg1 error) *MockSentryServerPenalizePeerCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerPenalizePeerCall) Do(f func(context.Context, *PenalizePeerRequest) (*emptypb.Empty, error)) *MockSentryServerPenalizePeerCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerPenalizePeerCall) DoAndReturn(f func(context.Context, *PenalizePeerRequest) (*emptypb.Empty, error)) *MockSentryServerPenalizePeerCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// RemovePeer mocks base method.
func (m *MockSentryServer) RemovePeer(arg0 context.Context, arg1 *RemovePeerRequest) (*RemovePeerReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemovePeer", arg0, arg1)
	ret0, _ := ret[0].(*RemovePeerReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemovePeer indicates an expected call of RemovePeer.
func (mr *MockSentryServerMockRecorder) RemovePeer(arg0, arg1 any) *MockSentryServerRemovePeerCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemovePeer", reflect.TypeOf((*MockSentryServer)(nil).RemovePeer), arg0, arg1)
	return &MockSentryServerRemovePeerCall{Call: call}
}

// MockSentryServerRemovePeerCall wrap *gomock.Call
type MockSentryServerRemovePeerCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerRemovePeerCall) Return(arg0 *RemovePeerReply, arg1 error) *MockSentryServerRemovePeerCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerRemovePeerCall) Do(f func(context.Context, *RemovePeerRequest) (*RemovePeerReply, error)) *MockSentryServerRemovePeerCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerRemovePeerCall) DoAndReturn(f func(context.Context, *RemovePeerRequest) (*RemovePeerReply, error)) *MockSentryServerRemovePeerCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// SendMessageById mocks base method.
func (m *MockSentryServer) SendMessageById(arg0 context.Context, arg1 *SendMessageByIdRequest) (*SentPeers, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessageById", arg0, arg1)
	ret0, _ := ret[0].(*SentPeers)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessageById indicates an expected call of SendMessageById.
func (mr *MockSentryServerMockRecorder) SendMessageById(arg0, arg1 any) *MockSentryServerSendMessageByIdCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessageById", reflect.TypeOf((*MockSentryServer)(nil).SendMessageById), arg0, arg1)
	return &MockSentryServerSendMessageByIdCall{Call: call}
}

// MockSentryServerSendMessageByIdCall wrap *gomock.Call
type MockSentryServerSendMessageByIdCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerSendMessageByIdCall) Return(arg0 *SentPeers, arg1 error) *MockSentryServerSendMessageByIdCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerSendMessageByIdCall) Do(f func(context.Context, *SendMessageByIdRequest) (*SentPeers, error)) *MockSentryServerSendMessageByIdCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerSendMessageByIdCall) DoAndReturn(f func(context.Context, *SendMessageByIdRequest) (*SentPeers, error)) *MockSentryServerSendMessageByIdCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// SendMessageByMinBlock mocks base method.
func (m *MockSentryServer) SendMessageByMinBlock(arg0 context.Context, arg1 *SendMessageByMinBlockRequest) (*SentPeers, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessageByMinBlock", arg0, arg1)
	ret0, _ := ret[0].(*SentPeers)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessageByMinBlock indicates an expected call of SendMessageByMinBlock.
func (mr *MockSentryServerMockRecorder) SendMessageByMinBlock(arg0, arg1 any) *MockSentryServerSendMessageByMinBlockCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessageByMinBlock", reflect.TypeOf((*MockSentryServer)(nil).SendMessageByMinBlock), arg0, arg1)
	return &MockSentryServerSendMessageByMinBlockCall{Call: call}
}

// MockSentryServerSendMessageByMinBlockCall wrap *gomock.Call
type MockSentryServerSendMessageByMinBlockCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerSendMessageByMinBlockCall) Return(arg0 *SentPeers, arg1 error) *MockSentryServerSendMessageByMinBlockCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerSendMessageByMinBlockCall) Do(f func(context.Context, *SendMessageByMinBlockRequest) (*SentPeers, error)) *MockSentryServerSendMessageByMinBlockCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerSendMessageByMinBlockCall) DoAndReturn(f func(context.Context, *SendMessageByMinBlockRequest) (*SentPeers, error)) *MockSentryServerSendMessageByMinBlockCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// SendMessageToAll mocks base method.
func (m *MockSentryServer) SendMessageToAll(arg0 context.Context, arg1 *OutboundMessageData) (*SentPeers, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessageToAll", arg0, arg1)
	ret0, _ := ret[0].(*SentPeers)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessageToAll indicates an expected call of SendMessageToAll.
func (mr *MockSentryServerMockRecorder) SendMessageToAll(arg0, arg1 any) *MockSentryServerSendMessageToAllCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessageToAll", reflect.TypeOf((*MockSentryServer)(nil).SendMessageToAll), arg0, arg1)
	return &MockSentryServerSendMessageToAllCall{Call: call}
}

// MockSentryServerSendMessageToAllCall wrap *gomock.Call
type MockSentryServerSendMessageToAllCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerSendMessageToAllCall) Return(arg0 *SentPeers, arg1 error) *MockSentryServerSendMessageToAllCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerSendMessageToAllCall) Do(f func(context.Context, *OutboundMessageData) (*SentPeers, error)) *MockSentryServerSendMessageToAllCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerSendMessageToAllCall) DoAndReturn(f func(context.Context, *OutboundMessageData) (*SentPeers, error)) *MockSentryServerSendMessageToAllCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// SendMessageToRandomPeers mocks base method.
func (m *MockSentryServer) SendMessageToRandomPeers(arg0 context.Context, arg1 *SendMessageToRandomPeersRequest) (*SentPeers, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessageToRandomPeers", arg0, arg1)
	ret0, _ := ret[0].(*SentPeers)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessageToRandomPeers indicates an expected call of SendMessageToRandomPeers.
func (mr *MockSentryServerMockRecorder) SendMessageToRandomPeers(arg0, arg1 any) *MockSentryServerSendMessageToRandomPeersCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessageToRandomPeers", reflect.TypeOf((*MockSentryServer)(nil).SendMessageToRandomPeers), arg0, arg1)
	return &MockSentryServerSendMessageToRandomPeersCall{Call: call}
}

// MockSentryServerSendMessageToRandomPeersCall wrap *gomock.Call
type MockSentryServerSendMessageToRandomPeersCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerSendMessageToRandomPeersCall) Return(arg0 *SentPeers, arg1 error) *MockSentryServerSendMessageToRandomPeersCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerSendMessageToRandomPeersCall) Do(f func(context.Context, *SendMessageToRandomPeersRequest) (*SentPeers, error)) *MockSentryServerSendMessageToRandomPeersCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerSendMessageToRandomPeersCall) DoAndReturn(f func(context.Context, *SendMessageToRandomPeersRequest) (*SentPeers, error)) *MockSentryServerSendMessageToRandomPeersCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// SetStatus mocks base method.
func (m *MockSentryServer) SetStatus(arg0 context.Context, arg1 *StatusData) (*SetStatusReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetStatus", arg0, arg1)
	ret0, _ := ret[0].(*SetStatusReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetStatus indicates an expected call of SetStatus.
func (mr *MockSentryServerMockRecorder) SetStatus(arg0, arg1 any) *MockSentryServerSetStatusCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStatus", reflect.TypeOf((*MockSentryServer)(nil).SetStatus), arg0, arg1)
	return &MockSentryServerSetStatusCall{Call: call}
}

// MockSentryServerSetStatusCall wrap *gomock.Call
type MockSentryServerSetStatusCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServerSetStatusCall) Return(arg0 *SetStatusReply, arg1 error) *MockSentryServerSetStatusCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServerSetStatusCall) Do(f func(context.Context, *StatusData) (*SetStatusReply, error)) *MockSentryServerSetStatusCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServerSetStatusCall) DoAndReturn(f func(context.Context, *StatusData) (*SetStatusReply, error)) *MockSentryServerSetStatusCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// mustEmbedUnimplementedSentryServer mocks base method.
func (m *MockSentryServer) mustEmbedUnimplementedSentryServer() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedSentryServer")
}

// mustEmbedUnimplementedSentryServer indicates an expected call of mustEmbedUnimplementedSentryServer.
func (mr *MockSentryServerMockRecorder) mustEmbedUnimplementedSentryServer() *MockSentryServermustEmbedUnimplementedSentryServerCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedSentryServer", reflect.TypeOf((*MockSentryServer)(nil).mustEmbedUnimplementedSentryServer))
	return &MockSentryServermustEmbedUnimplementedSentryServerCall{Call: call}
}

// MockSentryServermustEmbedUnimplementedSentryServerCall wrap *gomock.Call
type MockSentryServermustEmbedUnimplementedSentryServerCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSentryServermustEmbedUnimplementedSentryServerCall) Return() *MockSentryServermustEmbedUnimplementedSentryServerCall {
	c.Call = c.Call.Return()
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSentryServermustEmbedUnimplementedSentryServerCall) Do(f func()) *MockSentryServermustEmbedUnimplementedSentryServerCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSentryServermustEmbedUnimplementedSentryServerCall) DoAndReturn(f func()) *MockSentryServermustEmbedUnimplementedSentryServerCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
