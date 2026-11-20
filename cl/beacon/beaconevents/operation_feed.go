package beaconevents

import ethevent "github.com/erigontech/erigon/common/event"

type operationFeed struct {
	feed ethevent.Feed
	// Like head on stateFeed, these two are also sent on feeds of their own.
	payload, payloadAvailable ethevent.Feed
}

func newOpFeed() *operationFeed {
	return &operationFeed{}
}

func (f *operationFeed) Subscribe(channel chan *EventStream) ethevent.Subscription {
	return f.feed.Subscribe(channel)
}

// SubscribeExecutionPayload subscribes channel to execution_payload events only.
func (f *operationFeed) SubscribeExecutionPayload(channel chan *EventStream) ethevent.Subscription {
	return f.payload.Subscribe(channel)
}

// SubscribeExecutionPayloadAvailable subscribes channel to execution_payload_available events only.
func (f *operationFeed) SubscribeExecutionPayloadAvailable(channel chan *EventStream) ethevent.Subscription {
	return f.payloadAvailable.Subscribe(channel)
}

func (f *operationFeed) SendAttestation(value *AttestationData) int {
	return f.feed.Send(&EventStream{
		Event: OpAttestation,
		Data:  value,
	})
}

func (f *operationFeed) SendSingleAttestation(value *SingleAttestationData) int {
	return f.feed.Send(&EventStream{
		Event: OpSingleAttestation,
		Data:  value,
	})
}

func (f *operationFeed) SendVoluntaryExit(value *VoluntaryExitData) int {
	return f.feed.Send(&EventStream{
		Event: OpVoluntaryExit,
		Data:  value,
	})
}

func (f *operationFeed) SendProposerSlashing(value *ProposerSlashingData) int {
	return f.feed.Send(&EventStream{
		Event: OpProposerSlashing,
		Data:  value,
	})
}

func (f *operationFeed) SendAttesterSlashing(value *AttesterSlashingData) int {
	return f.feed.Send(&EventStream{
		Event: OpAttesterSlashing,
		Data:  value,
	})
}

func (f *operationFeed) SendBlsToExecution(value *BlsToExecutionChangesData) int {
	return f.feed.Send(&EventStream{
		Event: OpBlsToExecution,
		Data:  value,
	})
}

func (f *operationFeed) SendContributionProof(value *ContributionAndProofData) int {
	return f.feed.Send(&EventStream{
		Event: OpContributionProof,
		Data:  value,
	})
}

func (f *operationFeed) SendBlobSidecar(value *BlobSidecarData) int {
	return f.feed.Send(&EventStream{
		Event: OpBlobSidecar,
		Data:  value,
	})
}

func (f *operationFeed) SendDataColumnSidecar(value *DataColumnSidecarData) int {
	return f.feed.Send(&EventStream{
		Event: OpDataColumnSidecar,
		Data:  value,
	})
}

// SendPayloadAttestationMessage emits a payload_attestation_message event. [New in Gloas:EIP7732]
func (f *operationFeed) SendPayloadAttestationMessage(value *PayloadAttestationMessageData) int {
	return f.feed.TrySend(&EventStream{
		Event: OpPayloadAttestationMessage,
		Data:  &VersionedPayloadAttestationMessage{Version: "gloas", Data: value},
	})
}

// SendExecutionPayloadBid emits an execution_payload_bid event. [New in Gloas:EIP7732]
func (f *operationFeed) SendExecutionPayloadBid(value *SignedExecutionPayloadBidData) int {
	return f.feed.TrySend(&EventStream{
		Event: OpExecutionPayloadBid,
		Data:  &VersionedSignedExecutionPayloadBid{Version: "gloas", Data: value},
	})
}

// SendExecutionPayloadAvailable emits an execution_payload_available event. [New in Gloas:EIP7732]
func (f *operationFeed) SendExecutionPayloadAvailable(value *ExecutionPayloadAvailableData) int {
	e := &EventStream{Event: OpExecutionPayloadAvailable, Data: value}
	return f.feed.TrySend(e) + f.payloadAvailable.TrySendOrFail(e)
}

func (f *operationFeed) SendExecutionPayload(value *ExecutionPayloadData) int {
	e := &EventStream{Event: OpExecutionPayload, Data: value}
	return f.feed.TrySend(e) + f.payload.TrySendOrFail(e)
}

func (f *operationFeed) SendExecutionPayloadGossip(value *ExecutionPayloadGossipData) int {
	return f.feed.TrySend(&EventStream{Event: OpExecutionPayloadGossip, Data: value})
}

func (f *operationFeed) SendProposerPreferences(value *VersionedSignedProposerPreferences) int {
	return f.feed.TrySend(&EventStream{Event: OpProposerPreferences, Data: value})
}
