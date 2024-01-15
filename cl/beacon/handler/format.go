package handler

import (
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

func newBeaconResponse(data any) *beaconhttp.BeaconResponse {
	return beaconhttp.NewBeaconResponse(data)
}
