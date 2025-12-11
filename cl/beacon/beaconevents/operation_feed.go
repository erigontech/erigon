package beaconevents

import ethevent "github.com/erigontech/erigon/p2p/event"

type operationFeed struct {
	feed *ethevent.Feed
}

func newOpFeed() *operationFeed {
	return &operationFeed{
		feed: &ethevent.Feed{},
	}
}

func (f *operationFeed) Subscribe(channel chan *EventStream) ethevent.Subscription {
	return f.feed.Subscribe(channel)
}

func (f *operationFeed) SendAttestation(value *AttestationData) int {
	return f.feed.Send(&EventStream{
		Event: OpAttestation,
		Data:  value,
	})
}

func (f *operationFeed) SendSingleAttestation(value *SingleAttestationData) int {
	return f.feed.Send(&EventStream{
		Event: OpAttestation,
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
