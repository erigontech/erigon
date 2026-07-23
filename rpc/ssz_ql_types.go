package rpc

type Path string

type Filter string

type Anchor string

type Aliases struct {
	Path    Path   `json:"path"`
	Filters Filter `json:"filter"`
	Alias   string `json:"alias"`
}

type SSZQuery struct {
	Anchor    Anchor `json:"anchor"`
	Path      Path   `json:"path"`
	Filters   Filter `json:"filter"`
	Summaries bool   `json:"summaries"`
}

type SSZQLRequest struct {
	Aliases       []Aliases  `json:"aliases"`
	Queries       []SSZQuery `json:"query"`
	IncludeProofs bool       `json:"include_proof"`
	Multiproof    bool       `json:"multiproof"`
}
