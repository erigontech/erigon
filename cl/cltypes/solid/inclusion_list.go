package solid

type SignedInclusionList struct {
	Message   *InclusionList `json:"message"`
	Signature string         `json:"signature"`
}

type InclusionList struct {
	Slot                       string   `json:"slot"`
	ValidatorIndex             string   `json:"validator_index"`
	InclusionListCommitteeRoot string   `json:"inclusion_list_committee_root"`
	Transactions               []string `json:"transactions"`
}
