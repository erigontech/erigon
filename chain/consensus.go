package chain

type ConsensusName string

const (
	AuRaConsensus   ConsensusName = "aura"
	EtHashConsensus ConsensusName = "ethash"
	CliqueConsensus ConsensusName = "clique"
	BorConsensus    ConsensusName = "bor"
)
