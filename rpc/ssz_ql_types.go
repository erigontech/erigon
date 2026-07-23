package rpc

type Path string

type Filter string

type Anchor string

type Aliases struct {
	Path    Path   `json:"path"`
	Filters Filter `json:"filter,omitempty"`
	Alias   string `json:"alias"`
}

type SSZQuery struct {
	Anchor    Anchor `json:"anchor"`
	Path      Path   `json:"path"`
	Filters   Filter `json:"filter,omitempty"`
	Summaries bool   `json:"summaries,omitempty"`
}

type SSZQLRequest struct {
	Aliases       []Aliases  `json:"aliases,omitempty"`
	Queries       []SSZQuery `json:"queries"`
	IncludeProofs bool       `json:"include_proof,omitempty"`
	Multiproof    bool       `json:"multiproof,omitempty"`
}
