package handler

import (
	"bytes"
	"net/http"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (a *ApiHandler) getSpec(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.beaconChainCfg), nil
}

func (a *ApiHandler) getDepositContract(r *http.Request) (*beaconResponse, error) {

	return newBeaconResponse(struct {
		ChainId         uint64 `json:"chain_id"`
		DepositContract string `json:"address"`
	}{ChainId: a.beaconChainCfg.DepositChainID, DepositContract: a.beaconChainCfg.DepositContractAddress}), nil

}

func (a *ApiHandler) getForkSchedule(r *http.Request) (*beaconResponse, error) {
	response := []cltypes.Fork{}
	// create first response (unordered and incomplete)
	for currentVersion, epoch := range a.beaconChainCfg.ForkVersionSchedule {
		response = append(response, cltypes.Fork{
			CurrentVersion: currentVersion,
			Epoch:          epoch,
		})
	}
	// Sort the respnses by epoch
	sort.Slice(response, func(i, j int) bool {
		if response[i].Epoch == response[j].Epoch {
			return bytes.Compare(response[i].CurrentVersion[:], response[j].CurrentVersion[:]) < 0
		}
		return response[i].Epoch < response[j].Epoch
	})
	var previousVersion libcommon.Bytes4
	for i := range response {
		response[i].PreviousVersion = previousVersion
		previousVersion = response[i].CurrentVersion
	}
	return newBeaconResponse(response), nil
}
