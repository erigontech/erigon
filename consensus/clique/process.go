package clique

import "github.com/ledgerwatch/turbo-geth/consensus"

type CliqueProcess struct {
	Clique
	*consensus.Process
}

func NewCliqueProcess(c Clique, chain consensus.ChainHeaderReader) *CliqueProcess {
	return &CliqueProcess{
		Clique:  c,
		Process: consensus.NewProcess(chain),
	}
}

func (c *CliqueProcess) VerifyHeader() chan<- consensus.VerifyHeaderRequest {
	panic("implement me")
}

func (c *CliqueProcess) GetVerifyHeader() <-chan consensus.VerifyHeaderResponse {
	panic("implement me")
}

func (c *CliqueProcess) HeaderRequest() <-chan consensus.HeadersRequest {
	panic("implement me")
}

func (c *CliqueProcess) HeaderResponse() chan<- consensus.HeaderResponse {
	panic("implement me")
}
