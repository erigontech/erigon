package sszql

// note: derived types of Proof and Leaf can change later

type Path string

type Filter string

type Anchor string

type Gindex int64

type Proof string

type Leaf string

type Result struct {
	// TODO: decide and implement actual fields later. probably have to create more result types depending on what's been queried for.
	Dummy string `json:"dummy"`
}

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

type AliasResponse struct {
	Alias string `json:"alias"`
	Value string `json:"value"`
}

type SSZQLResponse struct {
	Paths    []Path          `json:"paths"`
	Gindices []Gindex        `json:"gindices"`
	Proofs   []Proof         `json:"proofs"`
	Leaves   []Leaf          `json:"leaves"`
	Results  []Result        `json:"results"`
	Aliases  []AliasResponse `json:"aliases"`
}
